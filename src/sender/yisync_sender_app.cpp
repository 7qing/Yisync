#include "node/yisync_node_apps.hpp"

#include "network/yisync_async.hpp"
#include "network/yisync_network.hpp"
#include "network/yisync_scheduler.hpp"
#include "sender/yisync_send_buffer.hpp"
#include "sender/yisync_sender_plan.hpp"
#include "sender/yisync_source.hpp"
#include "core/yisync_protocol.hpp"
#include "core/yisync_sync.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>

namespace yisync::node {

class SenderApp {
 public:
  explicit SenderApp(NodeOptions options)
      : options_(std::move(options)),
        network_(loop_,
                 make_line_endpoints(options_),
                 make_line_configs(options_),
                 kMaxFrameBytes,
                 yisync::network::ReconnectPolicy{
                     .base_delay = std::chrono::milliseconds(options_.reconnect_base_delay_ms),
                     .max_delay = std::chrono::milliseconds(options_.reconnect_max_delay_ms),
                 },
                 yisync::network::make_hello(yisync::Role::Sender,
                                             "sender",
                                             static_cast<std::uint32_t>(options_.chunk_size),
                                             options_.recv_window_bytes)) {
    build_source_streams();
  }

  int run() {
    network_.on_message([this](yisync::LineId id, yisync::Message message) {
      on_message(id, std::move(message));
    });
    network_.on_connected([this](yisync::LineId id) {
      on_connected(id);
    });
    network_.on_lost_sends([this](const std::vector<yisync::LostSend>& lost_sends) {
      on_lost_sends(lost_sends);
    });
    network_.start();

    if (options_.watch && !options_.source_root.empty()) {
      start_watchers();
    }
    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
    loop_.call_later(std::chrono::seconds(30), [this] {
      if (!options_.watch && !done_) {
        std::cerr << "SENDER timeout\n";
        failed_ = true;
        loop_.stop();
      }
    });
    loop_.run();
    return failed_ || !done_ ? 1 : 0;
  }

 private:
  struct SourceWatcherState {
    std::uint64_t stream_id = 0;
    std::filesystem::path root;
    std::unique_ptr<yisync::ISourceWatcher> watcher;
  };

  class RtoEstimator {
   public:
    std::uint64_t rto_ticks(std::uint64_t fallback_ticks) const noexcept {
      if (!initialized_) {
        return std::max<std::uint64_t>(1, fallback_ticks);
      }
      const auto candidate = srtt_ticks_ + std::max<std::uint64_t>(1, rttvar_ticks_ * 4);
      return std::clamp<std::uint64_t>(candidate,
                                       std::max<std::uint64_t>(1, fallback_ticks / 2),
                                       std::max<std::uint64_t>(1, fallback_ticks * 8));
    }

    void observe(std::uint64_t sample_ticks) noexcept {
      if (sample_ticks == 0) {
        sample_ticks = 1;
      }
      if (!initialized_) {
        srtt_ticks_ = sample_ticks;
        rttvar_ticks_ = std::max<std::uint64_t>(1, sample_ticks / 2);
        initialized_ = true;
        return;
      }
      const auto abs_delta = srtt_ticks_ > sample_ticks
                                 ? srtt_ticks_ - sample_ticks
                                 : sample_ticks - srtt_ticks_;
      rttvar_ticks_ = std::max<std::uint64_t>(1, (3 * rttvar_ticks_ + abs_delta) / 4);
      srtt_ticks_ = std::max<std::uint64_t>(1, (7 * srtt_ticks_ + sample_ticks) / 8);
    }

    bool initialized() const noexcept {
      return initialized_;
    }

   private:
    bool initialized_ = false;
    std::uint64_t srtt_ticks_ = 0;
    std::uint64_t rttvar_ticks_ = 0;
  };

  void build_source_streams() {
    if (options_.source_root.empty()) {
      auto data = make_sender_bytes(options_.size);
      auto reader = std::make_shared<yisync::SimulatedSourceReader>(data);
      auto task = yisync::make_simulated_file_task(kStreamId,
                                                   kSeq,
                                                   kFileId,
                                                   std::move(data),
                                                   reader,
                                                   options_.chunk_size,
                                                   {});
      StreamSendState stream;
      stream.stream_id = task.stream_id;
      stream.source_manifest.stream_id = task.stream_id;
      stream.source_manifest.root = "<simulated>";
      stream.source_manifest.entries.push_back(yisync::ManifestEntry{
          .file_id = task.file_id,
          .seq = task.seq,
          .kind = task.kind,
          .name = task.name,
          .link_target = task.link_target,
          .size = task.source_size,
          .checksum = task.range_checksum,
      });
      stream.tasks.push_back(std::move(task));
      stream.complete = false;
      stream.has_pending_changes = true;
      streams_.push_back(std::move(stream));
      return;
    }

    struct ConfiguredRoot {
      std::uint64_t stream_id = 0;
      std::filesystem::path root;
      std::string entry_name_regex;
    };

    std::vector<ConfiguredRoot> roots;
    for (const auto& stream : options_.source_streams) {
      roots.push_back(ConfiguredRoot{
          .stream_id = stream.stream_id,
          .root = stream.root,
          .entry_name_regex = stream.entry_name_regex,
      });
    }
    if (!roots.empty()) {
      std::sort(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.stream_id < rhs.stream_id;
      });
      roots.erase(std::unique(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.stream_id == rhs.stream_id;
      }), roots.end());
    }
    std::error_code ec;
    if (!std::filesystem::exists(options_.source_root, ec)) {
      if (roots.empty()) {
        throw std::runtime_error("source root does not exist: " + options_.source_root.string());
      }
    }
    if (roots.empty()) {
      for (const auto& entry : std::filesystem::directory_iterator(options_.source_root)) {
        if (!entry.is_directory()) {
          continue;
        }
        const auto stream_id = parse_stream_dir_name(entry.path());
        if (stream_id.has_value()) {
          roots.push_back(ConfiguredRoot{
              .stream_id = *stream_id,
              .root = entry.path(),
          });
        }
      }

      std::sort(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.stream_id < rhs.stream_id;
      });

      if (roots.empty()) {
        roots.push_back(ConfiguredRoot{
            .stream_id = kStreamId,
            .root = options_.source_root,
        });
      }
    }

    for (const auto& configured : roots) {
      const auto stream_id = configured.stream_id;
      const auto& root = configured.root;
      if (!std::filesystem::exists(root, ec)) {
        throw std::runtime_error("source stream root does not exist: " + root.string());
      }
      auto directory = std::make_shared<yisync::SourceDirectory>(stream_id,
                                                                 root,
                                                                 4 * 1024 * 1024,
                                                                 configured.entry_name_regex);
      auto manifest = directory->scan_manifest();
      if (manifest.entries.empty() && !options_.watch) {
        continue;
      }

      StreamSendState stream;
      stream.stream_id = stream_id;
      stream.root = root;
      stream.entry_name_regex = configured.entry_name_regex;
      stream.source_manifest = manifest;
      stream.source_directory = std::move(directory);
      for (const auto& file : stream.source_directory->files()) {
        stream.tasks.push_back(yisync::make_real_file_task(stream.stream_id,
                                                           file,
                                                           *stream.source_directory,
                                                           stream.source_directory,
                                                           options_.chunk_size,
                                                           {}));
      }
      stream.complete = stream.tasks.empty();
      stream.has_pending_changes = !stream.tasks.empty();
      streams_.push_back(std::move(stream));
    }

    if (streams_.empty() && !options_.watch) {
      throw std::runtime_error("source root has no syncable entries");
    }
  }

  void rebuild_source_streams_for_rescan() {
    send_buffer_.clear();
    streams_.clear();
    done_ = false;
    build_source_streams();
  }

  void start_watchers() {
    watchers_.clear();
    for (const auto& stream : streams_) {
      if (stream.root.empty()) {
        continue;
      }
      try {
        watchers_.push_back(SourceWatcherState{
            .stream_id = stream.stream_id,
            .root = stream.root,
            .watcher = yisync::make_source_watcher(stream.root, options_.watch_backend),
        });
        std::cout << "SENDER watch stream=" << stream.stream_id
                  << " root=" << stream.root
                  << " backend=" << yisync::watch_backend_name(options_.watch_backend) << "\n";
      } catch (const std::exception& ex) {
        std::cerr << "SENDER watcher failed stream=" << stream.stream_id
                  << " root=" << stream.root
                  << " error=" << ex.what() << "\n";
        failed_ = true;
        loop_.stop();
        return;
      }
    }
    if (!watchers_.empty()) {
      schedule_watch_poll();
    }
  }

  void schedule_watch_poll() {
    loop_.call_later(options_.watch_poll_interval, [this] { watch_poll_tick(); });
  }

  void watch_poll_tick() {
    if (failed_ || !options_.watch) {
      return;
    }
    bool changed = false;
    for (auto& watcher : watchers_) {
      auto events = watcher.watcher->poll();
      if (events.empty()) {
        continue;
      }
      for (const auto& event : events) {
        std::cout << "SENDER watch event stream=" << watcher.stream_id
                  << " kind=" << static_cast<int>(event.kind)
                  << " path=" << event.path << "\n";
      }
      changed = true;
    }
    if (changed) {
      request_rescan("watch event");
    }
    schedule_watch_poll();
  }

  void request_rescan(std::string reason) {
    if (!options_.watch || rescan_scheduled_) {
      return;
    }
    rescan_scheduled_ = true;
    loop_.call_later(options_.watch_rescan_debounce, [this, reason = std::move(reason)] {
      rescan_scheduled_ = false;
      rescan_and_send_manifest(reason);
    });
  }

  void rescan_and_send_manifest(const std::string& reason) {
    if (failed_) {
      return;
    }
    if (!done_ && !streams_.empty()) {
      rescan_pending_ = true;
      std::cout << "SENDER rescan deferred reason=" << reason << "\n";
      return;
    }
    const auto old_count = streams_.size();
    try {
      rebuild_source_streams_for_rescan();
    } catch (const std::exception& ex) {
      std::cerr << "SENDER rescan failed reason=" << reason
                << " error=" << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    std::cout << "SENDER rescan reason=" << reason
              << " streams=" << streams_.size()
              << " previous_streams=" << old_count << "\n";
    if (!streams_.empty()) {
      send_manifest1();
    }
  }

  StreamSendState* stream_for_id(std::uint64_t stream_id) {
    auto it = std::find_if(streams_.begin(), streams_.end(), [stream_id](const auto& stream) {
      return stream.stream_id == stream_id;
    });
    return it == streams_.end() ? nullptr : &*it;
  }

  const StreamSendState* stream_for_id(std::uint64_t stream_id) const {
    auto it = std::find_if(streams_.begin(), streams_.end(), [stream_id](const auto& stream) {
      return stream.stream_id == stream_id;
    });
    return it == streams_.end() ? nullptr : &*it;
  }

  yisync::network::NetworkSendResult send_buffered(yisync::Message message,
                                                   yisync::SendRequest request) {
    const auto result = network_.send(message, request);
    if (!result.sent) {
      return result;
    }
    request.bytes = result.bytes;
    send_buffer_.remember(std::move(message), std::move(request), current_tick_);
    return result;
  }

  void erase_completed_buffered(const yisync::Heartbeat& heartbeat) {
    observe_ack_samples(send_buffer_.erase_completed(heartbeat, current_tick_));
  }

  void erase_file_begin_buffered(std::uint64_t stream_id,
                                 std::uint64_t file_id,
                                 std::uint64_t seq) {
    if (auto sample = send_buffer_.erase_file_begin(stream_id, file_id, seq, current_tick_)) {
      observe_ack_sample(*sample);
    }
  }

  void erase_chunk_buffered(const yisync::ReceivedChunk& chunk,
                            std::uint64_t stream_id) {
    if (auto sample = send_buffer_.erase_chunk(chunk, stream_id, current_tick_)) {
      observe_ack_sample(*sample);
    }
  }

  bool enqueue_nack_retransmit(const yisync::Nack& nack) {
    return send_buffer_.enqueue_nack_retransmit(nack, options_.chunk_size);
  }

  void reset_task_for_manifest_recovery(FileSendTask& task) {
    task.chunk_mode = task.preferred_chunk_mode;
    task.begin_sent = false;
    task.begin_ready = false;
    task.commit_sent = false;
    task.begin_line_id = 0;
    task.commit_line_id = 0;
    yisync::reset_append_state(task.append);
    yisync::reset_chunk_resend_state(task.chunk_resend);
  }

  bool recover_with_manifest1(const std::string& reason) {
    if (failed_) {
      return false;
    }
    if (manifest_recovery_attempts_ >= options_.max_manifest_recovery_attempts) {
      final_fail("SENDER final failure reason=" + reason +
                 " manifest_recovery_attempts=" + std::to_string(manifest_recovery_attempts_) +
                 " max=" + std::to_string(options_.max_manifest_recovery_attempts));
      return false;
    }

    manifest_recovery_attempts_ += 1;
    send_buffer_.clear();
    for (auto& stream : streams_) {
      stream.manifest_applied = false;
      stream.complete = false;
      stream.has_pending_changes = !stream.tasks.empty();
      if (stream.current_task > stream.tasks.size()) {
        stream.current_task = stream.tasks.size();
      }
      for (auto& task : stream.tasks) {
        reset_task_for_manifest_recovery(task);
      }
    }
    done_ = false;
    std::cerr << "SENDER recovery manifest1 reason=" << reason
              << " attempt=" << manifest_recovery_attempts_
              << " max=" << options_.max_manifest_recovery_attempts << "\n";
    send_manifest1();
    return true;
  }

  void observe_ack_samples(const std::vector<yisync::AckedSendSample>& samples) {
    for (const auto& sample : samples) {
      observe_ack_sample(sample);
    }
  }

  void observe_ack_sample(const yisync::AckedSendSample& sample) {
    if (sample.rtt_ticks == 0) {
      return;
    }
    rto_.observe(sample.rtt_ticks);
  }

  std::uint64_t current_rto_ticks() const noexcept {
    return rto_.rto_ticks(options_.chunk_retransmit_ticks);
  }

  void final_fail(const std::string& message) {
    if (!message.empty()) {
      std::cerr << message << "\n";
    }
    failed_ = true;
    network_.stop();
    loop_.stop();
  }

  bool is_recoverable_nack(yisync::NackReason reason) const noexcept {
    switch (reason) {
      case yisync::NackReason::BadChecksum:
      case yisync::NackReason::BadCommit:
      case yisync::NackReason::SizeConflict:
      case yisync::NackReason::ChecksumMismatch:
      case yisync::NackReason::BadSeq:
      case yisync::NackReason::BadOffset:
      case yisync::NackReason::BadChunk:
        return true;
      case yisync::NackReason::BadSession:
      case yisync::NackReason::BadFileOrder:
      case yisync::NackReason::BadCreate:
      case yisync::NackReason::FileExists:
      case yisync::NackReason::PrevFileIncomplete:
      case yisync::NackReason::UnsupportedCompression:
      case yisync::NackReason::DecodeError:
      case yisync::NackReason::IoError:
        return false;
    }
    return false;
  }

  const char* recovery_reason_name(yisync::NackReason reason) const noexcept {
    switch (reason) {
      case yisync::NackReason::BadChecksum:
        return "bad_checksum";
      case yisync::NackReason::BadCommit:
        return "bad_commit";
      case yisync::NackReason::SizeConflict:
        return "size_conflict";
      case yisync::NackReason::ChecksumMismatch:
        return "checksum_mismatch";
      case yisync::NackReason::BadSeq:
        return "bad_seq";
      case yisync::NackReason::BadOffset:
        return "bad_offset";
      case yisync::NackReason::BadChunk:
        return "bad_chunk";
      default:
        return "unrecoverable";
    }
  }

  bool flush_retransmits() {
    if (failed_ || done_) {
      return true;
    }
    for (std::size_t index = 0; index < send_buffer_.retransmit_queue().size();) {
      const auto key = send_buffer_.retransmit_queue()[index];
      auto* buffered = send_buffer_.find(key);
      if (buffered == nullptr) {
        send_buffer_.erase_retransmit(index);
        continue;
      }
      if (buffered->nack_retries >= options_.max_retransmit_retries) {
        const auto reason = "retransmit retries exhausted stream=" +
                            std::to_string(key.stream_id) +
                            " file_id=" + std::to_string(key.file_id) +
                            " kind=" + std::to_string(static_cast<int>(key.kind)) +
                            " retries=" + std::to_string(buffered->nack_retries);
        send_buffer_.erase_retransmit(index);
        recover_with_manifest1(reason);
        return false;
      }

      auto message = buffered->message;
      auto request = buffered->request;
      const auto result = network_.send(message, request);
      if (!result.sent) {
        return false;
      }
      buffered->message = std::move(message);
      send_buffer_.mark_retransmitted(key, result.bytes, current_tick_);
      mark_task_retransmitted(request, result.line_id);
      std::cout << "SENDER retransmit buffered line=" << result.line_id
                << " stream=" << request.stream_id
                << " file_id=" << request.file_id
                << " kind=" << static_cast<int>(request.kind)
                << " seq=" << request.seq
                << " offset=" << request.offset
                << " chunk_index=" << request.chunk_index
                << " retries=" << buffered->nack_retries << "\n";
      send_buffer_.erase_retransmit(index);
    }
    return true;
  }

  void mark_task_retransmitted(const yisync::SendRequest& request, yisync::LineId line_id) {
    auto* stream = stream_for_id(request.stream_id);
    if (stream == nullptr || stream->complete) {
      return;
    }
    auto* task = active_task(*stream);
    if (task == nullptr || task->file_id != request.file_id || task->seq != request.seq) {
      return;
    }
    switch (request.kind) {
      case yisync::SendKind::Create:
        if (!task->chunk_mode) {
          yisync::mark_append_create_retransmitted(task->append, line_id);
        }
        break;
      case yisync::SendKind::Data:
        if (!task->chunk_mode) {
          yisync::mark_append_data_retransmitted(task->append,
                                                 line_id,
                                                 request.offset,
                                                 request.end_offset);
        }
        break;
      case yisync::SendKind::FileBegin:
        if (task->chunk_mode) {
          task->begin_sent = true;
          task->begin_line_id = line_id;
        }
        break;
      case yisync::SendKind::Chunk:
        if (task->chunk_mode) {
          (void)yisync::mark_chunk_sent(task->chunk_resend,
                                        request.chunk_index,
                                        line_id,
                                        current_tick_);
        }
        break;
      case yisync::SendKind::FileCommit:
        if (task->chunk_mode) {
          task->commit_sent = true;
          task->commit_line_id = line_id;
        }
        break;
    }
  }

  bool enqueue_rto_retransmits() {
    if (failed_ || done_) {
      return true;
    }
    const auto rto_ticks = current_rto_ticks();
    for (const auto& key : send_buffer_.expired_keys(current_tick_, rto_ticks, 32)) {
      auto* buffered = send_buffer_.find(key);
      if (buffered == nullptr) {
        continue;
      }
      if (buffered->nack_retries >= options_.max_retransmit_retries) {
        return recover_with_manifest1("rto retries exhausted stream=" +
                                      std::to_string(key.stream_id) +
                                      " file_id=" + std::to_string(key.file_id) +
                                      " kind=" + std::to_string(static_cast<int>(key.kind)));
      }
      if (send_buffer_.enqueue_retransmit(key)) {
        std::cerr << "SENDER rto_timeout retransmit queued"
                  << " stream=" << key.stream_id
                  << " file_id=" << key.file_id
                  << " kind=" << static_cast<int>(key.kind)
                  << " seq=" << key.seq
                  << " offset=" << key.offset
                  << " chunk_index=" << key.chunk_index
                  << " rto_ticks=" << rto_ticks
                  << " retries=" << buffered->nack_retries << "\n";
      }
    }
    return true;
  }

  void on_connected(yisync::LineId id) {
    (void)id;
    if (done_ && options_.watch) {
      return;
    }
    send_manifest1();
    schedule_work();
  }

  void send_file_begin(StreamSendState& stream, FileSendTask& task) {
    if (!task.chunk_mode || task.begin_sent || !stream.manifest_applied) {
      return;
    }
    const yisync::FileBegin begin{
        .stream_id = task.stream_id,
        .seq = task.seq,
        .file_id = task.file_id,
        .name = task.name,
        .final_size = task.source_size,
        .chunk_size = options_.chunk_size,
        .chunk_count = task.chunk_count,
        .file_checksum = task.source_checksum,
        .prev_file_id = 0,
        .prev_final_size = 0,
    };
    auto message = yisync::Message{begin};
    const yisync::SendRequest request{
        .stream_id = begin.stream_id,
        .file_id = begin.file_id,
        .seq = begin.seq,
        .bytes = encoded_message_size(message),
        .split_allowed = false,
        .kind = yisync::SendKind::FileBegin,
    };
    const auto result = send_buffered(std::move(message), request);
    if (!result.sent) {
      return;
    }
    task.begin_sent = true;
    task.begin_line_id = result.line_id;
    std::cout << "SENDER FILE_BEGIN line=" << result.line_id
              << " stream=" << task.stream_id
              << " file_id=" << task.file_id
              << " chunks=" << task.chunk_count
              << " size=" << task.source_size << "\n";
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
    if (const auto* manifest = std::get_if<yisync::Manifest1>(&message)) {
      std::cerr << "SENDER unexpected Manifest1 from receiver line=" << line_id
                << " manifest_id=" << manifest->manifest_id << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    if (const auto* manifest2 = std::get_if<yisync::Manifest2>(&message)) {
      on_manifest2(line_id, *manifest2);
      return;
    }
    if (const auto* heartbeat = std::get_if<yisync::Heartbeat>(&message)) {
      on_heartbeat(line_id, *heartbeat);
      return;
    }
    if (const auto* nack = std::get_if<yisync::Nack>(&message)) {
      std::cerr << "SENDER NACK line=" << line_id
                << " reason=" << static_cast<int>(nack->reason)
                << " detail=" << nack->detail << "\n";
      if (!enqueue_nack_retransmit(*nack)) {
        const auto reason = std::string("nack buffer miss type=") +
                            recovery_reason_name(nack->reason) +
                            " stream=" + std::to_string(nack->stream_id) +
                            " file_id=" + std::to_string(nack->file_id) +
                            " got_seq=" + std::to_string(nack->got_seq);
        std::cerr << "SENDER NACK cannot find buffered packet " << reason << "\n";
        if (is_recoverable_nack(nack->reason)) {
          recover_with_manifest1(reason);
        } else {
          final_fail("SENDER final failure " + reason + " detail=" + nack->detail);
        }
        return;
      }
      schedule_work();
      return;
    }
    std::cerr << "SENDER unexpected message line=" << line_id << "\n";
    failed_ = true;
    loop_.stop();
  }

  void send_manifest1() {
    if (streams_.empty()) {
      return;
    }
    auto manifest = yisync::manifest1_from_streams(next_manifest_id_++, streams_);
    std::uint64_t entry_count = 0;
    for (const auto& stream : streams_) {
      entry_count += stream.source_manifest.entries.size();
    }
    const auto manifest_id = manifest.manifest_id;
    const auto stream_count = manifest.streams.size();
    std::ostringstream label;
    label << "message=Manifest1"
          << " manifest_id=" << manifest_id
          << " streams=" << stream_count
          << " entries=" << entry_count;
    const auto result = network_.send_control(yisync::Message{manifest}, label.str());
    if (!result.queued) {
      return;
    }
    if (!result.sent) {
      std::cout << "SENDER Manifest1 queued"
                << " manifest_id=" << manifest_id
                << " streams=" << stream_count
                << " entries=" << entry_count << "\n";
    }
  }

  void on_manifest2(yisync::LineId line_id, const yisync::Manifest2& manifest2) {
    std::cout << "SENDER Manifest2 line=" << line_id
              << " manifest_id=" << manifest2.manifest_id
              << " streams=" << manifest2.streams.size() << "\n";
    for (auto& stream : streams_) {
      apply_manifest2_stream(line_id, manifest2, stream);
      if (failed_) {
        return;
      }
    }
    schedule_work();
  }

  void apply_manifest2_stream(yisync::LineId line_id,
                              const yisync::Manifest2& manifest2,
                              StreamSendState& stream) {
    std::optional<yisync::Manifest2ApplyResult> result;
    try {
      result = yisync::apply_manifest2_to_stream(manifest2, stream);
    } catch (const std::exception& ex) {
      std::cerr << "SENDER Manifest2 apply failed line=" << line_id
                << " stream=" << stream.stream_id
                << " error=" << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    if (!result.has_value()) {
      return;
    }
    if (result->in_sync) {
      std::cout << "SENDER Manifest2 stream already in sync line=" << line_id
                << " stream=" << stream.stream_id << "\n";
      check_all_done();
      return;
    }

    auto* task = active_task(stream);
    if (task == nullptr) {
      stream.complete = true;
      check_all_done();
      return;
    }

    std::cout << "SENDER Manifest2 action line=" << line_id
              << " stream=" << stream.stream_id
              << " start_file_id=" << result->start.start_file_id
              << " start_offset=" << result->start.start_offset
              << " action=" << static_cast<int>(result->start.start_action) << "\n";
    if (result->should_start_chunk) {
      send_file_begin(stream, *task);
      return;
    }
    if (result->should_start_append) {
      apply_append_plan(line_id, stream, *task, result->start);
    }
  }

  void apply_append_plan(yisync::LineId line_id,
                         StreamSendState& stream,
                         FileSendTask& task,
                         const yisync::SyncStart& diff) {
    if (yisync::append_inflight(task.append) || done_) {
      return;
    }
    if (diff.start_file_id != task.file_id || diff.start_offset > task.source_size) {
      std::cerr << "SENDER append unsupported diff line=" << line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " start_file_id=" << diff.start_file_id
                << " start_offset=" << diff.start_offset << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    yisync::start_append_plan(task.append, diff, task.seq, task.source_size);
    send_append_if_possible(stream, task);
  }

  void send_append_if_possible(StreamSendState& stream, FileSendTask& task) {
    if (task.chunk_mode || !stream.manifest_applied || done_ || failed_) {
      return;
    }

    if (task.append.needs_create && !task.append.create_sent) {
      const yisync::Create create{
          .stream_id = task.stream_id,
          .seq = task.append.seq,
          .file_id = task.file_id,
          .kind = task.kind,
          .name = task.name,
          .link_target = task.link_target,
          .final_size = task.source_size,
          .prev_file_id = 0,
          .prev_final_size = 0,
      };
      const auto sent = try_send_buffered_kind(yisync::Message{create},
                                               yisync::SendKind::Create,
                                               create.seq,
                                               0,
                                               0,
                                               task.stream_id,
                                               task.file_id,
                                               task.append.create_line_id);
      if (!sent) {
        return;
      }
      yisync::mark_append_create_sent(task.append, task.append.create_line_id);
      std::cout << "SENDER send CREATE line=" << task.append.create_line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " kind=" << static_cast<int>(task.kind)
                << " seq=" << create.seq << "\n";
      return;
    }

    if (task.append.needs_create && !task.append.create_ready) {
      return;
    }

    if (task.append.data_sent) {
      return;
    }

    if (task.append.next_offset < task.source_size) {
      const auto data_len = std::min<std::uint64_t>(options_.chunk_size,
                                                   task.source_size - task.append.next_offset);
      auto payload = yisync::read_task_range(task, task.append.next_offset, data_len);
      auto data = make_data_from_payload(task.stream_id,
                                         task.file_id,
                                         task.append.seq,
                                         task.append.next_offset,
                                         task.source_size,
                                         std::move(payload));
      const auto sent = try_send_buffered_kind(yisync::Message{data},
                                               yisync::SendKind::Data,
                                               task.append.seq,
                                               data.offset,
                                               data.offset + data.raw_len,
                                               task.stream_id,
                                               task.file_id,
                                               task.append.data_line_id);
      if (!sent) {
        return;
      }
      yisync::mark_append_data_sent(task.append, task.append.data_line_id, data_len);
      std::cout << "SENDER send DATA line=" << task.append.data_line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " seq=" << task.append.seq
                << " offset=" << task.append.next_offset
                << " len=" << data_len << "\n";
      return;
    }

  }

  void advance_stream_task(StreamSendState& stream) {
    manifest_recovery_attempts_ = 0;
    if (stream.current_task < stream.tasks.size()) {
      stream.current_task += 1;
    }
    if (stream.current_task >= stream.tasks.size()) {
      stream.complete = true;
      std::cout << "SENDER stream complete stream=" << stream.stream_id << "\n";
      check_all_done();
      return;
    }

    auto* next = active_task(stream);
    if (next == nullptr) {
      stream.complete = true;
      check_all_done();
      return;
    }

    std::cout << "SENDER advance stream=" << stream.stream_id
              << " file_id=" << next->file_id
              << " mode=" << (next->chunk_mode ? "chunk" : "append") << "\n";
    if (next->chunk_mode) {
      send_file_begin(stream, *next);
      schedule_work();
      return;
    }

    const yisync::SyncStart create_missing{
        .stream_id = stream.stream_id,
        .start_file_id = next->file_id,
        .start_offset = 0,
        .start_action = yisync::StartAction::CreateMissing,
    };
    apply_append_plan(0, stream, *next, create_missing);
  }

  void check_all_done() {
    if (failed_ || done_ || streams_.empty()) {
      return;
    }
    const auto complete = std::all_of(streams_.begin(), streams_.end(), [](const auto& stream) {
      return stream.complete;
    });
    if (!complete) {
      return;
    }
    done_ = true;
    std::cout << "SENDER complete all streams=" << streams_.size() << "\n";
    log_metrics();
    if (!options_.watch) {
      network_.stop();
      loop_.stop();
      return;
    }
    for (auto& stream : streams_) {
      stream.has_pending_changes = false;
    }
    if (rescan_pending_) {
      rescan_pending_ = false;
      request_rescan("pending watch event");
    }
  }

  void log_metrics() const {
    std::size_t connected = 0;
    std::size_t negotiated = 0;
    std::size_t healthy = 0;
    std::size_t stale = 0;
    std::uint64_t pending_sends = 0;
    std::uint64_t inflight_bytes = 0;
    for (const auto& line : network_.snapshots()) {
      connected += line.connected ? 1U : 0U;
      negotiated += line.negotiated ? 1U : 0U;
      healthy += line.healthy ? 1U : 0U;
      stale += line.stale ? 1U : 0U;
      pending_sends += line.pending_sends;
      inflight_bytes += line.inflight_bytes;
    }
    std::cout << "SENDER metrics"
              << " streams=" << streams_.size()
              << " send_buffer=" << send_buffer_.size()
              << " retransmit_queue=" << send_buffer_.retransmit_queue_size()
              << " lines_connected=" << connected
              << " lines_negotiated=" << negotiated
              << " lines_healthy=" << healthy
              << " lines_stale=" << stale
              << " pending_sends=" << pending_sends
              << " inflight_bytes=" << inflight_bytes
              << "\n";
  }

  bool try_send_buffered_kind(yisync::Message message,
                              yisync::SendKind kind,
                              std::uint64_t seq,
                              std::uint64_t offset,
                              std::uint64_t end_offset,
                              std::uint64_t stream_id,
                              std::uint64_t file_id,
                              yisync::LineId& line_id_out) {
    const auto wire_bytes = encoded_message_size(message);
    const yisync::SendRequest request{
        .stream_id = stream_id,
        .file_id = file_id,
        .seq = seq,
        .offset = offset,
        .end_offset = end_offset,
        .bytes = wire_bytes,
        .split_allowed = false,
        .kind = kind,
    };
    const auto result = send_buffered(std::move(message), request);
    if (!result.sent) {
      return false;
    }
    line_id_out = result.line_id;
    return true;
  }

  void reset_append_plan_for_reconnect(StreamSendState& stream, FileSendTask& task) {
    yisync::reset_append_inflight(task.append);
    stream.manifest_applied = false;
  }

  void on_lost_sends(const std::vector<yisync::LostSend>& lost_sends) {
    bool changed = false;
    for (const auto& lost : lost_sends) {
      auto* stream = stream_for_id(lost.stream_id);
      if (stream == nullptr || stream->complete) {
        continue;
      }
      auto* task = active_task(*stream);
      if (task == nullptr || task->file_id != lost.file_id) {
        continue;
      }

      const auto lost_key = yisync::SenderSendBuffer::key_for_lost_send(lost);
      if (send_buffer_.enqueue_retransmit(lost_key)) {
        changed = true;
        std::cerr << "SENDER line_lost retransmit queued line=" << lost.line_id
                  << " stream=" << lost.stream_id
                  << " file_id=" << lost.file_id
                  << " kind=" << static_cast<int>(lost.kind)
                  << " seq=" << lost.seq
                  << " offset=" << lost.offset
                  << " chunk_index=" << lost.chunk_index << "\n";
        continue;
      }

      if (!task->chunk_mode) {
        if (yisync::append_lost_matches(task->append, lost)) {
          reset_append_plan_for_reconnect(*stream, *task);
          changed = true;
          std::cerr << "SENDER line_lost append reset line=" << lost.line_id
                    << " stream=" << lost.stream_id
                    << " file_id=" << lost.file_id
                    << " seq=" << lost.seq << "\n";
        }
        continue;
      }

      if (lost.seq != task->seq) {
        continue;
      }
      if (lost.kind == yisync::SendKind::FileBegin && task->begin_sent && !task->begin_ready) {
        task->begin_sent = false;
        task->begin_line_id = 0;
        changed = true;
        std::cerr << "SENDER line_lost FILE_BEGIN reset line=" << lost.line_id
                  << " stream=" << lost.stream_id
                  << " file_id=" << lost.file_id << "\n";
        continue;
      }
      if (lost.kind == yisync::SendKind::Chunk) {
        if (yisync::mark_chunk_lost(task->chunk_resend, lost.chunk_index)) {
          changed = true;
          std::cerr << "SENDER line_lost CHUNK priority line=" << lost.line_id
                    << " stream=" << lost.stream_id
                    << " file_id=" << lost.file_id
                    << " index=" << lost.chunk_index << "\n";
        }
        continue;
      }
      if (lost.kind == yisync::SendKind::FileCommit && task->commit_sent) {
        task->commit_sent = false;
        task->commit_line_id = 0;
        changed = true;
        std::cerr << "SENDER line_lost FILE_COMMIT reset line=" << lost.line_id
                  << " stream=" << lost.stream_id
                  << " file_id=" << lost.file_id << "\n";
      }
    }

    if (changed && !failed_ && !done_) {
      schedule_work();
    }
  }

  void on_heartbeat(yisync::LineId line_id, const yisync::Heartbeat& heartbeat) {
    network_.on_heartbeat(line_id, heartbeat);
    erase_completed_buffered(heartbeat);
    auto* stream = stream_for_id(heartbeat.stream_id);
    if (stream == nullptr) {
      return;
    }
    auto* task = active_task(*stream);
    if (task == nullptr) {
      check_all_done();
      return;
    }

    if (!task->chunk_mode) {
      if (heartbeat.file_id == task->file_id &&
          yisync::mark_append_create_ready_from_heartbeat(task->append, heartbeat)) {
        std::cout << "SENDER CREATE ready via heartbeat line=" << line_id
                  << " stream=" << task->stream_id
                  << " file_id=" << task->file_id << "\n";
      }
      if (heartbeat.file_id == task->file_id &&
          yisync::mark_append_data_ready_from_heartbeat(task->append, heartbeat)) {
        std::cout << "SENDER DATA ready via heartbeat line=" << line_id
                  << " stream=" << task->stream_id
                  << " file_id=" << task->file_id
                  << " next_offset=" << task->append.next_offset << "\n";
      }
      if (heartbeat.file_id == task->file_id &&
          yisync::append_complete_by_heartbeat(task->append,
                                               heartbeat,
                                               task->kind,
                                               task->source_size)) {
        std::cout << "SENDER complete append stream=" << task->stream_id
                  << " file_id=" << task->file_id
                  << " final_size=" << task->source_size
                  << " durable_offset=" << heartbeat.durable_offset << "\n";
        advance_stream_task(*stream);
        return;
      }
      send_append_if_possible(*stream, *task);
      return;
    }

    if (task->begin_sent && !task->begin_ready &&
        heartbeat.file_id == task->file_id &&
        heartbeat.received_chunks.empty()) {
      task->begin_ready = true;
      erase_file_begin_buffered(task->stream_id, task->file_id, task->seq);
      std::cout << "SENDER FILE_BEGIN ready via heartbeat line=" << line_id
                << " stream=" << task->stream_id
                << " file_id=" << task->file_id << "\n";
    }

    for (const auto& received : heartbeat.received_chunks) {
      if (received.seq != task->seq ||
          received.file_id != task->file_id) {
        continue;
      }
      if (!yisync::acknowledge_chunk(task->chunk_resend, received.chunk_index)) {
        continue;
      }
      erase_chunk_buffered(received, heartbeat.stream_id);
      std::cout << "SENDER chunk ack stream=" << task->stream_id
                << " file_id=" << task->file_id
                << " index=" << received.chunk_index
                << " line=" << line_id << "\n";
    }

    for (const auto& range : yisync::apply_missing_hints(task->chunk_resend,
                                                         task->seq,
                                                         task->file_id,
                                                         heartbeat.missing_ranges)) {
      std::cout << "SENDER missing_hint out_of_order gap stream=" << task->stream_id
                << " file_id=" << task->file_id
                << " range=" << range.first_chunk_index << "-" << range.last_chunk_index
                << " line=" << line_id << "\n";
    }

    if (task->commit_sent && heartbeat.next_seq > task->seq) {
      std::cout << "SENDER complete chunk stream=" << task->stream_id
                << " file_id=" << task->file_id
                << " final_size=" << task->source_size << "\n";
      advance_stream_task(*stream);
      return;
    }

    schedule_work();
  }

  void schedule_work() {
    if (failed_ || done_) {
      return;
    }
    if (!enqueue_rto_retransmits()) {
      return;
    }
    if (!flush_retransmits()) {
      return;
    }

    for (auto& stream : streams_) {
      if (!stream.manifest_applied || stream.complete) {
        continue;
      }
      auto* task = active_task(stream);
      if (task == nullptr) {
        stream.complete = true;
        continue;
      }
      if (!task->chunk_mode) {
        send_append_if_possible(stream, *task);
        continue;
      }
      schedule_chunk_work(stream, *task);
    }
    check_all_done();
  }

  void schedule_chunk_work(StreamSendState& stream, FileSendTask& task) {
    if (!task.begin_ready || failed_ || done_) {
      send_file_begin(stream, task);
      return;
    }

    while (true) {
      const auto next_chunk = yisync::next_unsent_chunk_index(task,
                                                              current_tick_,
                                                              current_rto_ticks());
      if (!next_chunk.has_value()) {
        break;
      }
      const auto chunk_index = *next_chunk;
      const auto max_chunk_attempts = options_.max_retransmit_retries == UINT64_MAX
                                          ? UINT64_MAX
                                          : options_.max_retransmit_retries + 1;
      const auto attempts = yisync::chunk_attempts(task.chunk_resend, chunk_index);
      if (attempts >= max_chunk_attempts) {
        recover_with_manifest1("chunk retries exhausted stream=" +
                               std::to_string(task.stream_id) +
                               " file_id=" + std::to_string(task.file_id) +
                               " chunk_index=" + std::to_string(chunk_index) +
                               " attempts=" + std::to_string(attempts));
        return;
      }
      auto chunk = make_chunk_from_payload(task.stream_id,
                                           task.seq,
                                           task.file_id,
                                           chunk_index,
                                           yisync::read_chunk_payload(task, chunk_index, options_.chunk_size),
                                           options_.chunk_size);
      const auto wire_bytes = encoded_message_size(yisync::Message{chunk});
      const yisync::SendRequest request{
          .stream_id = chunk.stream_id,
          .file_id = chunk.file_id,
          .seq = chunk.seq,
          .bytes = wire_bytes,
          .split_allowed = false,
          .kind = yisync::SendKind::Chunk,
          .chunk_index = chunk.chunk_index,
      };
      const auto result = send_buffered(yisync::Message{chunk}, request);
      if (!result.sent) {
        return;
      }
      const auto mark = yisync::mark_chunk_sent(task.chunk_resend,
                                                chunk_index,
                                                result.line_id,
                                                current_tick_);
      std::cout << "SENDER send CHUNK stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " index=" << chunk_index
                << " line=" << result.line_id
                << " wire_bytes=" << wire_bytes
                << " attempt=" << mark.attempt
                << " retransmit=" << mark.retransmit << "\n";
      maybe_drop_line_once(result.line_id);
    }

    if (yisync::all_chunks_acked(task)) {
      send_commit_if_possible(task);
    }
  }

  void send_commit_if_possible(FileSendTask& task) {
    if (task.commit_sent) {
      return;
    }
    const yisync::FileCommit commit{
        .stream_id = task.stream_id,
        .seq = task.seq,
        .file_id = task.file_id,
    };
    const auto message = yisync::Message{commit};
    const auto wire_bytes = encoded_message_size(message);
    const yisync::SendRequest request{
        .stream_id = commit.stream_id,
        .file_id = commit.file_id,
        .seq = commit.seq,
        .bytes = wire_bytes,
        .split_allowed = false,
        .kind = yisync::SendKind::FileCommit,
    };
    const auto result = send_buffered(message, request);
    if (!result.sent) {
      return;
    }
    task.commit_sent = true;
    task.commit_line_id = result.line_id;
    std::cout << "SENDER send FILE_COMMIT line=" << result.line_id
              << " stream=" << task.stream_id
              << " file_id=" << task.file_id
              << " wire_bytes=" << wire_bytes << "\n";
  }

  void maybe_drop_line_once(yisync::LineId line_id) {
    if (options_.drop_line_once == 0 || dropped_line_once_) {
      return;
    }
    if (line_id != options_.drop_line_once) {
      return;
    }
    dropped_line_once_ = true;
    std::cerr << "SENDER test drop line=" << line_id << "\n";
    network_.mark_unavailable(line_id);
  }

  void tick() {
    if (failed_) {
      return;
    }
    current_tick_ += 1;
    network_.refill_ticks(1);
    if (!done_) {
      schedule_work();
    }
    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  std::vector<StreamSendState> streams_;
  std::vector<SourceWatcherState> watchers_;
  yisync::network::SenderNetwork network_;
  yisync::SenderSendBuffer send_buffer_;
  RtoEstimator rto_;
  std::uint64_t next_manifest_id_ = 1;
  std::uint64_t current_tick_ = 0;
  std::uint64_t manifest_recovery_attempts_ = 0;
  bool dropped_line_once_ = false;
  bool rescan_scheduled_ = false;
  bool rescan_pending_ = false;
  bool done_ = false;
  bool failed_ = false;
};

int run_sender(NodeOptions options) {
  return SenderApp(std::move(options)).run();
}

}  // namespace yisync::node
