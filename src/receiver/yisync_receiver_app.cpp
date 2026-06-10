#include "node/yisync_node_apps.hpp"

#include "core/yisync_protocol.hpp"
#include "core/yisync_sync.hpp"
#include "network/yisync_async.hpp"
#include "network/yisync_network.hpp"
#include "receiver/yisync_commit_poller.hpp"
#include "receiver/yisync_disk_writer.hpp"
#include "receiver/yisync_receiver_coordinator.hpp"
#include "receiver/yisync_receiver_streams.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace yisync::node {

class ReceiverApp {
 public:
  explicit ReceiverApp(NodeOptions options)
      : options_(std::move(options)),
        stream_map_(kStreamId, options_.root, &receiver_stream_roots_),
        coordinator_(stream_map_,
                     disk_writer_,
                     options_.recv_window_bytes,
                     options_.max_missing_ranges_per_heartbeat),
        commit_poller_(loop_, options_.receiver_commit_poll_interval),
        network_(loop_,
                 make_line_endpoints(options_),
                 kMaxFrameBytes,
                 yisync::network::make_hello(yisync::Role::Receiver,
                                             "receiver",
                                             static_cast<std::uint32_t>(options_.chunk_size),
                                             options_.recv_window_bytes)) {
    build_receiver_stream_roots();
  }

  int run() {
    std::filesystem::create_directories(options_.root);
    for (const auto& [stream_id, root] : receiver_stream_roots_) {
      (void)stream_id;
      std::filesystem::create_directories(root);
    }
    network_.on_message([this](yisync::LineId id, yisync::Message message) {
      on_message(id, std::move(message));
    });
    network_.on_error([this](yisync::LineId id, std::string error) {
      fail("RECEIVER line error id=" + std::to_string(id) + " error=" + error);
    });
    network_.on_close([this](yisync::LineId id) {
      if (!committed_) {
        std::cerr << "RECEIVER line closed before commit id=" << id << "\n";
      }
    });
    network_.set_heartbeat_ack_batch_size(options_.heartbeat_ack_batch_size);
    network_.listen();

    loop_.call_later(options_.receiver_heartbeat_interval, [this] { heartbeat_tick(); });
    loop_.call_later(std::chrono::seconds(60), [this] {
      if (!options_.watch && !committed_) {
        fail("RECEIVER timeout");
      }
    });
    loop_.run();
    return failed_ || !committed_ ? 1 : 0;
  }

 private:
  std::filesystem::path stream_root_for(std::uint64_t stream_id) const {
    return stream_map_.stream_root_for(stream_id);
  }

  std::filesystem::path mounted_source_root(std::string_view source_root) const {
    std::filesystem::path relative;
    const std::filesystem::path source(source_root);
    for (const auto& part : source) {
      if (part == source.root_name() || part == source.root_directory() ||
          part.empty() || part == "." || part == "..") {
        continue;
      }
      relative /= part;
    }
    if (relative.empty()) {
      return options_.root;
    }
    return options_.root / relative;
  }

  void build_receiver_stream_roots() {
    receiver_stream_roots_[kStreamId] = options_.root;
  }

  std::vector<std::pair<std::uint64_t, std::filesystem::path>> manifest_roots() const {
    std::vector<std::pair<std::uint64_t, std::filesystem::path>> roots;
    for (const auto& [stream_id, root] : receiver_stream_roots_) {
      roots.push_back({stream_id, root});
    }
    if (roots.empty()) {
      roots.push_back({kStreamId, options_.root});
    }

    std::error_code ec;
    if (!std::filesystem::exists(options_.root, ec)) {
      return roots;
    }
    for (const auto& entry : std::filesystem::directory_iterator(options_.root)) {
      if (!entry.is_directory()) {
        continue;
      }
      const auto stream_id = parse_stream_dir_name(entry.path());
      if (!stream_id.has_value() || *stream_id == kStreamId) {
        continue;
      }
      roots.push_back({*stream_id, entry.path()});
    }
    std::sort(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });
    roots.erase(std::unique(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first == rhs.first;
    }), roots.end());
    return roots;
  }

  void schedule_quiet_stop() {
    committed_ = true;
    const auto generation = ++activity_generation_;
    loop_.call_later(std::chrono::milliseconds(750), [this, generation] {
      if (options_.watch) {
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_ &&
          coordinator_.append_durable_idle()) {
        log_metrics();
        loop_.stop();
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_) {
        schedule_quiet_stop_retry(generation);
      }
    });
  }

  void schedule_quiet_stop_retry(std::uint64_t generation) {
    loop_.call_later(options_.receiver_heartbeat_interval, [this, generation] {
      if (options_.watch) {
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_ &&
          coordinator_.append_durable_idle()) {
        log_metrics();
        loop_.stop();
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_) {
        schedule_quiet_stop_retry(generation);
      }
    });
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
    activity_generation_ += 1;
    if (const auto* create = std::get_if<yisync::Create>(&message)) {
      execute_actions(coordinator_.apply_create(line_id, *create));
      return;
    }
    if (const auto* data = std::get_if<yisync::Data>(&message)) {
      execute_actions(coordinator_.apply_data(line_id, *data));
      return;
    }
    if (const auto* begin = std::get_if<yisync::FileBegin>(&message)) {
      execute_actions(coordinator_.apply_begin(line_id, *begin));
      return;
    }
    if (const auto* chunk = std::get_if<yisync::Chunk>(&message)) {
      execute_actions(coordinator_.apply_chunk(line_id, *chunk));
      return;
    }
    if (const auto* commit = std::get_if<yisync::FileCommit>(&message)) {
      execute_actions(coordinator_.apply_commit(line_id, *commit));
      return;
    }
    if (const auto* manifest = std::get_if<yisync::Manifest1>(&message)) {
      apply_manifest1(line_id, *manifest);
      return;
    }
    fail("RECEIVER unexpected message line=" + std::to_string(line_id));
  }

  void send_message(yisync::LineId line_id, const yisync::Message& message) {
    network_.send(line_id, message);
  }

  void send_nack(yisync::LineId line_id, const yisync::Nack& nack) {
    send_message(line_id, yisync::Message{nack});
  }

  void execute_actions(const yisync::ReceiverActionBatch& actions) {
    for (const auto& log : actions.logs) {
      if (log.error) {
        std::cerr << log.text << "\n";
      } else {
        std::cout << log.text << "\n";
      }
    }
    for (const auto& nack : actions.nacks) {
      if (nack.only_if_line_open && !network_.can_send(nack.line_id)) {
        if (!nack.closed_log.empty()) {
          std::cerr << nack.closed_log << "\n";
        }
        continue;
      }
      send_nack(nack.line_id, nack.nack);
    }
    for (const auto& heartbeat : actions.heartbeats) {
      if (heartbeat.only_if_line_open && !network_.can_send(heartbeat.line_id)) {
        continue;
      }
      network_.queue_heartbeat(heartbeat.line_id, heartbeat.heartbeat);
      if (heartbeat.flush) {
        network_.flush_heartbeats(heartbeat.line_id);
      }
    }
    if (actions.schedule_commit_poll) {
      schedule_commit_poll();
    }
    if (actions.schedule_quiet_stop) {
      schedule_quiet_stop();
    }
    if (actions.failed) {
      fail(actions.failure.empty() ? "RECEIVER failed" : actions.failure);
    }
  }

  void heartbeat_tick() {
    if (failed_) {
      return;
    }
    if (!check_disk_writer()) {
      return;
    }
    execute_actions(coordinator_.poll_completions());
    if (failed_) {
      return;
    }
    network_.flush_all_heartbeats();
    loop_.call_later(options_.receiver_heartbeat_interval, [this] { heartbeat_tick(); });
  }

  void schedule_commit_poll() {
    commit_poller_.schedule([this] { commit_poll_tick(); });
  }

  void commit_poll_tick() {
    if (failed_) {
      return;
    }
    if (!check_disk_writer()) {
      return;
    }
    execute_actions(coordinator_.poll_completions());
    if (!failed_ && coordinator_.has_pending_chunk_commit()) {
      schedule_commit_poll();
    }
  }

  bool check_disk_writer() {
    if (!disk_writer_.failed()) {
      return true;
    }
    fail(std::string("RECEIVER disk writer failed: ") + disk_writer_.error());
    return false;
  }

  yisync::Manifest1 receiver_manifest_for_manifest1(const yisync::Manifest1& manifest1) {
    yisync::Manifest1 receiver_manifest;
    receiver_manifest.manifest_id = manifest1.manifest_id;
    const auto roots = manifest_roots();
    std::vector<std::string> stream_dir_names;
    for (const auto& sender_stream : manifest1.streams) {
      if (sender_stream.stream_id != kStreamId) {
        stream_dir_names.push_back(std::to_string(sender_stream.stream_id));
      }
    }
    for (const auto& [stream_id, root] : roots) {
      if (stream_id != kStreamId) {
        stream_dir_names.push_back(root.filename().string());
      }
    }
    std::sort(stream_dir_names.begin(), stream_dir_names.end());
    stream_dir_names.erase(std::unique(stream_dir_names.begin(), stream_dir_names.end()), stream_dir_names.end());

    for (const auto& sender_stream : manifest1.streams) {
      const auto root = sender_stream.root.empty()
                            ? stream_root_for(sender_stream.stream_id)
                            : mounted_source_root(sender_stream.root);
      receiver_stream_roots_[sender_stream.stream_id] = root;
      std::filesystem::create_directories(root);
      if (sender_stream.stream_id == kStreamId) {
        receiver_manifest.streams.push_back(yisync::scan_manifest_stream(sender_stream.stream_id,
                                                                         root,
                                                                         4 * 1024 * 1024,
                                                                         stream_dir_names));
      } else {
        receiver_manifest.streams.push_back(yisync::scan_manifest_stream(sender_stream.stream_id,
                                                                         root,
                                                                         4 * 1024 * 1024));
      }
    }
    return receiver_manifest;
  }

  void apply_manifest1(yisync::LineId line_id, const yisync::Manifest1& manifest1) {
    committed_ = false;
    yisync::Manifest2 manifest2;
    manifest2.manifest_id = manifest1.manifest_id;
    const auto receiver_manifest = receiver_manifest_for_manifest1(manifest1);

    for (const auto& sender_stream : manifest1.streams) {
      const auto receiver_it = std::find_if(receiver_manifest.streams.begin(),
                                            receiver_manifest.streams.end(),
                                            [&](const auto& candidate) {
                                              return candidate.stream_id == sender_stream.stream_id;
                                            });
      const std::vector<yisync::ManifestEntry> empty_receiver_entries;
      const auto& receiver_entries = receiver_it == receiver_manifest.streams.end()
                                         ? empty_receiver_entries
                                         : receiver_it->entries;
      try {
        const auto start = yisync::diff_stream(sender_stream.stream_id,
                                              sender_stream.entries,
                                              receiver_entries);
        manifest2.streams.push_back(yisync::make_manifest2_stream(sender_stream.stream_id, start));
      } catch (const std::exception& ex) {
        send_nack(line_id,
                  yisync::Nack{
                      .stream_id = sender_stream.stream_id,
                      .got_seq = 0,
                      .expected_seq = 0,
                      .file_id = 0,
                      .offset = 0,
                      .expected_file_id = 0,
                      .expected_offset = 0,
                      .reason = yisync::NackReason::SizeConflict,
                      .detail = ex.what(),
                  });
        return;
      }
    }

    std::uint64_t entries = 0;
    for (const auto& stream : manifest1.streams) {
      entries += stream.entries.size();
    }
    send_message(line_id, yisync::Message{manifest2});
    std::cout << "RECEIVER Manifest2 line=" << line_id
              << " manifest_id=" << manifest2.manifest_id
              << " streams=" << manifest2.streams.size()
              << " sender_entries=" << entries << "\n";
  }

  void fail(const std::string& message) {
    if (!message.empty()) {
      std::cerr << message << "\n";
    }
    failed_ = true;
    loop_.stop();
  }

  void log_metrics() {
    if (metrics_logged_) {
      return;
    }
    metrics_logged_ = true;
    std::cout << "RECEIVER metrics"
              << " streams=" << receiver_stream_roots_.size()
              << " pending_chunk_commit=" << (coordinator_.has_pending_chunk_commit() ? 1 : 0)
              << " append_durable_idle=" << (coordinator_.append_durable_idle() ? 1 : 0)
              << " disk_writer_failed=" << (disk_writer_.failed() ? 1 : 0)
              << "\n";
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  yisync::SpscDiskWriter disk_writer_;
  std::unordered_map<std::uint64_t, std::filesystem::path> receiver_stream_roots_;
  yisync::ReceiverStreamMap stream_map_;
  yisync::ReceiverCoordinator coordinator_;
  yisync::CommitCompletionPoller commit_poller_;
  yisync::network::ReceiverNetwork network_;
  std::uint64_t activity_generation_ = 0;
  bool committed_ = false;
  bool failed_ = false;
  bool metrics_logged_ = false;
};

int run_receiver(NodeOptions options) {
  return ReceiverApp(std::move(options)).run();
}

}  // namespace yisync::node
