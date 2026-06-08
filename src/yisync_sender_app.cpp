#include "yisync_node_apps.hpp"

#include "yisync_async.hpp"
#include "yisync_protocol.hpp"
#include "yisync_receiver.hpp"
#include "yisync_scheduler.hpp"
#include "yisync_source.hpp"
#include "yisync_sync.hpp"
#include "yisync_transport.hpp"

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
#include <string_view>
#include <thread>
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
        scheduler_(make_line_configs(options_.lines)) {
    build_source_streams();
    lines_.reserve(options_.lines);
    for (std::uint32_t i = 1; i <= options_.lines; ++i) {
      lines_.push_back(Line{
          .id = i,
          .endpoint = yisync::Endpoint{options_.host, line_port(options_, i)},
      });
    }
  }

  int run() {
    for (auto& line : lines_) {
      start_connect(line.id);
    }

    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
    loop_.call_later(std::chrono::seconds(30), [this] {
      if (!done_) {
        std::cerr << "SENDER timeout\n";
        failed_ = true;
        loop_.stop();
      }
    });
    loop_.run();
    return failed_ || !done_ ? 1 : 0;
  }

 private:
  struct Line {
    yisync::LineId id = 0;
    yisync::Endpoint endpoint;
    std::shared_ptr<yisync::AsyncFrameConnection> connection;
    std::shared_ptr<yisync::PendingTcpConnect> connector;
    bool connected = false;
    bool connecting = false;
    bool reconnect_scheduled = false;
    std::uint64_t reconnect_attempts = 0;
  };

  struct FileSendTask {
    std::uint64_t stream_id = 0;
    std::uint64_t order_seq = 0;
    std::uint64_t file_id = 0;
    yisync::EntryKind kind = yisync::EntryKind::RegularFile;
    std::string name;
    std::string link_target;
    std::uint64_t source_size = 0;
    yisync::FileChecksum source_checksum;
    yisync::FileChecksum range_checksum;
    bool real_source = false;
    std::filesystem::path source_root;
    yisync::Bytes simulated_data;

    bool chunk_mode = false;
    std::uint64_t chunk_count = 0;
    std::vector<std::uint64_t> chunk_order;
    std::vector<bool> chunk_acked;
    std::vector<bool> chunk_sent;
    std::vector<yisync::LineId> chunk_line;
    std::vector<std::uint64_t> chunk_send_tick;
    std::vector<std::uint64_t> chunk_attempts;
    std::vector<bool> chunk_priority;

    bool begin_sent = false;
    bool begin_ready = false;
    bool commit_sent = false;
    bool resume_from_incomplete = false;
    yisync::LineId begin_line_id = 0;
    yisync::LineId commit_line_id = 0;
    std::uint64_t begin_wait_ticks = 0;

    bool append_needs_create = false;
    bool append_data_needed = false;
    bool append_create_sent = false;
    bool append_create_ready = false;
    bool append_data_sent = false;
    yisync::LineId append_create_line_id = 0;
    yisync::LineId append_data_line_id = 0;
    std::uint64_t append_offset = 0;
    std::uint64_t append_next_offset = 0;
    std::uint64_t append_current_data_len = 0;
    std::uint64_t append_create_seq = 0;
    std::uint64_t append_data_seq = 1;
    std::uint64_t append_done_next_seq = 2;
  };

  struct StreamSendState {
    std::uint64_t stream_id = 0;
    std::filesystem::path root;
    std::unique_ptr<yisync::SourceDirectory> source_directory;
    yisync::ManifestStream source_manifest;
    std::vector<FileSendTask> tasks;
    std::size_t current_task = 0;
    std::uint64_t next_append_seq = 1;
    bool manifest_applied = false;
    bool complete = false;
  };

  void build_source_streams() {
    if (options_.source_root.empty()) {
      auto task = make_simulated_task();
      StreamSendState stream;
      stream.stream_id = task.stream_id;
      stream.source_manifest.stream_id = task.stream_id;
      stream.source_manifest.root = "<simulated>";
      stream.source_manifest.entries.push_back(yisync::ManifestEntry{
          .file_id = task.file_id,
          .order_seq = task.order_seq,
          .kind = task.kind,
          .name = task.name,
          .link_target = task.link_target,
          .size = task.source_size,
          .checksum = task.range_checksum,
      });
      stream.tasks.push_back(std::move(task));
      streams_.push_back(std::move(stream));
      return;
    }

    std::vector<std::pair<std::uint64_t, std::filesystem::path>> roots;
    std::error_code ec;
    if (!std::filesystem::exists(options_.source_root, ec)) {
      throw std::runtime_error("source root does not exist: " + options_.source_root.string());
    }
    for (const auto& entry : std::filesystem::directory_iterator(options_.source_root)) {
      if (!entry.is_directory()) {
        continue;
      }
      const auto stream_id = parse_stream_dir_name(entry.path());
      if (stream_id.has_value()) {
        roots.push_back({*stream_id, entry.path()});
      }
    }
    std::sort(roots.begin(), roots.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });

    if (roots.empty()) {
      roots.push_back({kStreamId, options_.source_root});
    }

    for (const auto& [stream_id, root] : roots) {
      auto directory = std::make_unique<yisync::SourceDirectory>(stream_id, root, 4 * 1024 * 1024);
      auto manifest = directory->scan_manifest();
      if (manifest.entries.empty()) {
        continue;
      }

      StreamSendState stream;
      stream.stream_id = stream_id;
      stream.root = root;
      stream.source_manifest = manifest;
      stream.source_directory = std::move(directory);
      for (const auto& file : stream.source_directory->files()) {
        stream.tasks.push_back(make_real_task(stream.stream_id, stream.root, file, *stream.source_directory));
      }
      streams_.push_back(std::move(stream));
    }

    if (streams_.empty()) {
      throw std::runtime_error("source root has no syncable entries");
    }
  }

  FileSendTask make_simulated_task() const {
    auto data = make_sender_bytes(options_.size);
    const auto checksum = full_crc32c_checksum(data);
    const auto chunk_mode = yisync::should_use_chunk_mode(data.size());
    FileSendTask task;
    task.stream_id = kStreamId;
    task.order_seq = kOrderSeq;
    task.file_id = kFileId;
    task.kind = yisync::EntryKind::RegularFile;
    task.name = yisync::file_name_for_id(kFileId);
    task.source_size = static_cast<std::uint64_t>(data.size());
    task.source_checksum = checksum;
    task.range_checksum = checksum;
    task.real_source = false;
    task.simulated_data = std::move(data);
    initialize_file_task(task, chunk_mode);
    return task;
  }

  static FileSendTask make_real_task(std::uint64_t stream_id,
                                     const std::filesystem::path& root,
                                     const yisync::SourceFile& file,
                                     yisync::SourceDirectory& directory) {
    FileSendTask task;
    task.stream_id = stream_id;
    task.order_seq = file.manifest.order_seq == 0 ? file.file_id : file.manifest.order_seq;
    task.file_id = file.file_id;
    task.kind = file.manifest.kind;
    task.name = file.manifest.name;
    task.link_target = file.manifest.link_target;
    task.source_size = file.manifest.kind == yisync::EntryKind::RegularFile ? file.manifest.size : 0;
    task.source_checksum = file.manifest.kind == yisync::EntryKind::RegularFile
                               ? directory.full_checksum(file)
                               : yisync::FileChecksum{};
    task.range_checksum = file.manifest.checksum;
    task.real_source = true;
    task.source_root = root;
    initialize_file_task(task,
                         file.manifest.kind == yisync::EntryKind::RegularFile &&
                             yisync::should_use_chunk_mode(task.source_size));
    return task;
  }

  static void initialize_file_task(FileSendTask& task, bool chunk_mode) {
    task.chunk_mode = chunk_mode;
    task.chunk_count = chunk_mode ? yisync::chunk_count_for_size(task.source_size) : 0;
    task.chunk_order = chunk_send_order(task.chunk_count);
    task.chunk_acked.assign(static_cast<std::size_t>(task.chunk_count), false);
    task.chunk_sent.assign(static_cast<std::size_t>(task.chunk_count), false);
    task.chunk_line.assign(static_cast<std::size_t>(task.chunk_count), yisync::LineId{0});
    task.chunk_send_tick.assign(static_cast<std::size_t>(task.chunk_count), 0);
    task.chunk_attempts.assign(static_cast<std::size_t>(task.chunk_count), 0);
    task.chunk_priority.assign(static_cast<std::size_t>(task.chunk_count), false);
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

  FileSendTask* active_task(StreamSendState& stream) {
    if (stream.current_task >= stream.tasks.size()) {
      return nullptr;
    }
    return &stream.tasks[stream.current_task];
  }

  const FileSendTask* active_task(const StreamSendState& stream) const {
    if (stream.current_task >= stream.tasks.size()) {
      return nullptr;
    }
    return &stream.tasks[stream.current_task];
  }

  Line& find_line(yisync::LineId id) {
    auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
      return line.id == id;
    });
    if (it == lines_.end()) {
      throw std::runtime_error("unknown sender line");
    }
    return *it;
  }

  Line* first_connected_line() {
    auto it = std::find_if(lines_.begin(), lines_.end(), [](const auto& line) {
      return line.connected && line.connection && !line.connection->closed();
    });
    return it == lines_.end() ? nullptr : &*it;
  }

  bool all_chunks_acked(const FileSendTask& task) const {
    return std::all_of(task.chunk_acked.begin(), task.chunk_acked.end(), [](bool acked) {
      return acked;
    });
  }

  std::optional<std::uint64_t> next_unsent_chunk_index(const FileSendTask& task) const {
    for (std::uint64_t index = 0; index < task.chunk_priority.size(); ++index) {
      if (can_send_chunk(task, index, true)) {
        return index;
      }
    }

    for (const auto chunk_index : task.chunk_order) {
      if (can_send_chunk(task, chunk_index, false)) {
        return chunk_index;
      }
    }
    return std::nullopt;
  }

  bool can_send_chunk(const FileSendTask& task, std::uint64_t chunk_index, bool priority_only) const {
    const auto index = static_cast<std::size_t>(chunk_index);
    if (index >= task.chunk_acked.size() || task.chunk_acked[index]) {
      return false;
    }
    if (priority_only && !task.chunk_priority[index]) {
      return false;
    }
    if (!task.chunk_sent[index]) {
      return true;
    }
    return task.chunk_priority[index] &&
           current_tick_ >= task.chunk_send_tick[index] &&
           current_tick_ - task.chunk_send_tick[index] >= kChunkRetransmitTicks;
  }

  yisync::Bytes read_chunk_payload(const FileSendTask& task, std::uint64_t chunk_index) const {
    const auto offset = chunk_index * yisync::kDefaultChunkSizeBytes;
    const auto len = std::min<std::uint64_t>(yisync::kDefaultChunkSizeBytes, task.source_size - offset);
    return read_source_payload(task, offset, len);
  }

  yisync::Bytes read_source_payload(const FileSendTask& task, std::uint64_t offset, std::uint64_t len) const {
    if (task.real_source) {
      yisync::SourceDirectory directory(task.stream_id, task.source_root, 4 * 1024 * 1024);
      return directory.read_range(task.file_id, offset, len);
    }
    return yisync::Bytes(task.simulated_data.begin() + static_cast<std::ptrdiff_t>(offset),
                         task.simulated_data.begin() + static_cast<std::ptrdiff_t>(offset + len));
  }

  void on_connected(yisync::LineId id, std::shared_ptr<yisync::AsyncFrameConnection> connection) {
    auto& line = find_line(id);
    line.connector.reset();
    line.connecting = false;
    line.reconnect_scheduled = false;
    line.reconnect_attempts = 0;
    line.connection = std::move(connection);
    line.connected = true;
    scheduler_.on_line_connected(id);
    line.connection->on_message([this, id](yisync::Message message) {
      on_message(id, std::move(message));
    });
    line.connection->on_error([this, id](std::string error) {
      std::cerr << "SENDER line error id=" << id << " error=" << error << "\n";
      on_line_unavailable(id);
    });
    line.connection->on_close([this, id] {
      if (!done_) {
        std::cerr << "SENDER line closed before completion id=" << id << "\n";
        on_line_unavailable(id);
      }
    });
    line.connection->start(loop_);
    std::cout << "SENDER connected line=" << id << " endpoint="
              << line.endpoint.host << ":" << line.endpoint.port << "\n";

    schedule_work();
  }

  void on_line_unavailable(yisync::LineId id) {
    auto& line = find_line(id);
    if (!line.connected && !line.connecting && line.reconnect_scheduled) {
      return;
    }
    line.connected = false;
    line.connecting = false;
    line.connector.reset();
    auto connection = std::move(line.connection);
    const auto had_connection = static_cast<bool>(connection);
    scheduler_.on_line_disconnected(id);

    if (had_connection) {
      for (auto& stream : streams_) {
        auto* task = active_task(stream);
        if (task == nullptr) {
          continue;
        }
        if (!task->chunk_mode &&
            ((task->append_create_sent && task->append_create_line_id == id) ||
             (task->append_data_sent && task->append_data_line_id == id))) {
          reset_append_plan_for_reconnect(stream, *task);
        }
        if (task->begin_sent && !task->begin_ready && task->begin_line_id == id) {
          task->begin_sent = false;
          task->begin_line_id = 0;
        }
        for (std::size_t index = 0; index < task->chunk_sent.size(); ++index) {
          if (task->chunk_sent[index] && !task->chunk_acked[index] && task->chunk_line[index] == id) {
            task->chunk_sent[index] = false;
            task->chunk_line[index] = 0;
            task->chunk_priority[index] = true;
          }
        }
        if (task->commit_sent && !done_ && task->commit_line_id == id) {
          task->commit_sent = false;
          task->commit_line_id = 0;
        }
      }
    }

    schedule_reconnect(id);
    if (connection && !connection->closed()) {
      connection->close();
    }
  }

  void start_connect(yisync::LineId id) {
    auto& line = find_line(id);
    if (done_ || failed_ || line.connected || line.connecting) {
      return;
    }
    line.reconnect_scheduled = false;
    line.connecting = true;
    std::cout << "SENDER connecting line=" << id << " endpoint="
              << line.endpoint.host << ":" << line.endpoint.port
              << " attempt=" << (line.reconnect_attempts + 1) << "\n";
    line.connector = yisync::async_connect_tcp(
        loop_,
        line.endpoint,
        kMaxFrameBytes,
        [this, id](std::shared_ptr<yisync::AsyncFrameConnection> connection) {
          on_connected(id, std::move(connection));
        },
        [this, id](std::string error) {
          auto& line = find_line(id);
          line.connector.reset();
          line.connecting = false;
          std::cerr << "SENDER connect failed line=" << id << " error=" << error << "\n";
          scheduler_.on_line_disconnected(id);
          schedule_reconnect(id);
        });
  }

  void schedule_reconnect(yisync::LineId id) {
    auto& line = find_line(id);
    if (done_ || failed_ || line.connected || line.connecting || line.reconnect_scheduled) {
      return;
    }
    const auto capped_attempts = std::min<std::uint64_t>(line.reconnect_attempts, 5);
    const auto delay_ms = std::min<std::uint64_t>(kReconnectMaxDelayMs,
                                                 kReconnectBaseDelayMs << capped_attempts);
    line.reconnect_attempts += 1;
    line.reconnect_scheduled = true;
    std::cout << "SENDER reconnect scheduled line=" << id
              << " delay_ms=" << delay_ms
              << " attempt=" << line.reconnect_attempts << "\n";
    loop_.call_later(std::chrono::milliseconds(delay_ms), [this, id] {
      auto& line = find_line(id);
      line.reconnect_scheduled = false;
      if (!done_ && !failed_ && !line.connected && !line.connecting) {
        start_connect(id);
      }
    });
  }

  void send_file_begin(StreamSendState& stream, FileSendTask& task) {
    if (!task.chunk_mode || task.begin_sent || task.resume_from_incomplete || !stream.manifest_applied) {
      return;
    }
    auto* line = first_connected_line();
    if (line == nullptr) {
      return;
    }
    const yisync::FileBegin begin{
        .stream_id = task.stream_id,
        .order_seq = task.order_seq,
        .file_id = task.file_id,
        .name = task.name,
        .final_size = task.source_size,
        .chunk_size = yisync::kDefaultChunkSizeBytes,
        .chunk_count = task.chunk_count,
        .file_checksum = task.source_checksum,
        .prev_file_id = 0,
        .prev_final_size = 0,
    };
    line->connection->send(yisync::Message{begin});
    task.begin_sent = true;
    task.begin_line_id = line->id;
    task.begin_wait_ticks = 0;
    std::cout << "SENDER FILE_BEGIN line=" << line->id
              << " stream=" << task.stream_id
              << " file_id=" << task.file_id
              << " chunks=" << task.chunk_count
              << " size=" << task.source_size << "\n";
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
    if (const auto* manifest = std::get_if<yisync::Manifest>(&message)) {
      on_manifest(line_id, *manifest);
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
      failed_ = true;
      loop_.stop();
      return;
    }
    std::cerr << "SENDER unexpected message line=" << line_id << "\n";
    failed_ = true;
    loop_.stop();
  }

  void on_manifest(yisync::LineId line_id, const yisync::Manifest& manifest) {
    std::uint64_t entry_count = 0;
    std::uint64_t incomplete_count = 0;
    for (const auto& stream : manifest.streams) {
      entry_count += stream.entries.size();
      incomplete_count += stream.incomplete_chunks.size();
    }
    std::cout << "SENDER MANIFEST line=" << line_id
              << " manifest_id=" << manifest.manifest_id
              << " streams=" << manifest.streams.size()
              << " entries=" << entry_count
              << " incomplete_chunks=" << incomplete_count << "\n";
    for (auto& stream : streams_) {
      apply_stream_manifest(line_id, manifest, stream);
      if (failed_) {
        return;
      }
    }
    schedule_work();
  }

  void apply_stream_manifest(yisync::LineId line_id,
                             const yisync::Manifest& manifest,
                             StreamSendState& stream) {
    if (stream.manifest_applied || stream.complete) {
      return;
    }
    const auto stream_it = std::find_if(manifest.streams.begin(), manifest.streams.end(), [&](const auto& candidate) {
      return candidate.stream_id == stream.stream_id;
    });
    const std::vector<yisync::ManifestEntry> empty_receiver_entries;
    const auto& receiver_entries = stream_it == manifest.streams.end() ? empty_receiver_entries : stream_it->entries;

    std::optional<yisync::SyncStart> diff;
    try {
      diff = yisync::diff_stream(stream.stream_id, stream.source_manifest.entries, receiver_entries);
      if (diff.has_value()) {
        std::cout << "SENDER diff line=" << line_id
                  << " stream=" << stream.stream_id
                  << " start_file_id=" << diff->start_file_id
                  << " start_offset=" << diff->start_offset
                  << " action=" << static_cast<int>(diff->start_action) << "\n";
      } else {
        std::cout << "SENDER diff line=" << line_id
                  << " stream=" << stream.stream_id
                  << " in_sync=1\n";
      }
    } catch (const std::exception& ex) {
      std::cerr << "SENDER diff conflict line=" << line_id
                << " stream=" << stream.stream_id
                << " error=" << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    if (!diff.has_value()) {
      stream.current_task = stream.tasks.size();
      stream.manifest_applied = true;
      stream.complete = true;
      std::cout << "SENDER MANIFEST complete stream already in sync line=" << line_id
                << " stream=" << stream.stream_id << "\n";
      check_all_done();
      return;
    }

    auto it = std::find_if(stream.tasks.begin(), stream.tasks.end(), [&](const auto& task) {
      return task.file_id == diff->start_file_id;
    });
    if (it == stream.tasks.end()) {
      std::cerr << "SENDER diff start file not found stream=" << stream.stream_id
                << " file_id=" << diff->start_file_id << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    stream.current_task = static_cast<std::size_t>(std::distance(stream.tasks.begin(), it));
    stream.manifest_applied = true;
    auto* task = active_task(stream);
    if (task == nullptr) {
      stream.complete = true;
      check_all_done();
      return;
    }
    if (diff->start_action == yisync::StartAction::ResumeExisting) {
      task->chunk_mode = false;
      apply_append_plan(line_id, stream, *task, *diff);
    } else if (task->chunk_mode) {
      apply_chunk_resume(line_id, manifest, stream, *task, *diff);
    } else {
      apply_append_plan(line_id, stream, *task, *diff);
    }
  }

  void apply_chunk_resume(yisync::LineId line_id,
                          const yisync::Manifest& manifest,
                          StreamSendState& stream,
                          FileSendTask& task,
                          const yisync::SyncStart& diff) {
    yisync::ChunkResumePlan plan;
    try {
      plan = yisync::plan_chunk_resume_from_manifest(manifest,
                                                     task.stream_id,
                                                     task.order_seq,
                                                     task.file_id,
                                                     task.source_size,
                                                     yisync::kDefaultChunkSizeBytes,
                                                     task.chunk_count,
                                                     task.source_checksum);
    } catch (const std::exception& ex) {
      std::cerr << "SENDER MANIFEST conflict line=" << line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " error=" << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    if (plan.complete) {
      std::cout << "SENDER MANIFEST complete file already present line=" << line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id << "\n";
      advance_stream_task(stream);
      return;
    }
    if (!plan.resume_incomplete) {
      send_file_begin(stream, task);
      return;
    }

    task.resume_from_incomplete = true;
    task.begin_sent = true;
    task.begin_ready = true;
    for (const auto chunk_index : plan.checkpointed_chunks) {
      if (chunk_index >= task.chunk_acked.size()) {
        continue;
      }
      const auto index = static_cast<std::size_t>(chunk_index);
      task.chunk_acked[index] = true;
      task.chunk_sent[index] = true;
      task.chunk_line[index] = 0;
      task.chunk_priority[index] = false;
    }
    std::cout << "SENDER MANIFEST resume incomplete line=" << line_id
              << " stream=" << task.stream_id
              << " file_id=" << task.file_id
              << " checkpointed_chunks=" << plan.checkpointed_chunks.size()
              << " remaining_chunks=" << std::count(task.chunk_acked.begin(), task.chunk_acked.end(), false) << "\n";
    schedule_work();
  }

  void apply_append_plan(yisync::LineId line_id,
                         StreamSendState& stream,
                         FileSendTask& task,
                         const yisync::SyncStart& diff) {
    if (task.append_create_sent || task.append_data_sent || done_) {
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

    task.append_needs_create = diff.start_action == yisync::StartAction::CreateMissing;
    task.append_offset = diff.start_offset;
    task.append_next_offset = diff.start_offset;
    task.append_data_needed = task.append_offset < task.source_size;
    task.append_create_seq = task.append_needs_create ? stream.next_append_seq : 0;
    task.append_data_seq = stream.next_append_seq + (task.append_needs_create ? 1 : 0);
    const auto remaining = task.source_size - task.append_offset;
    const auto data_segments = remaining == 0
                                   ? 0
                                   : yisync::chunk_count_for_size(remaining);
    if (data_segments != 0) {
      task.append_done_next_seq = task.append_data_seq + data_segments;
    } else if (task.append_needs_create) {
      task.append_done_next_seq = task.append_create_seq + 1;
    } else {
      task.append_done_next_seq = stream.next_append_seq;
    }
    send_append_if_possible(stream, task);
  }

  void send_append_if_possible(StreamSendState& stream, FileSendTask& task) {
    if (task.chunk_mode || !stream.manifest_applied || done_ || failed_) {
      return;
    }

    if (task.append_needs_create && !task.append_create_sent) {
      const yisync::Create create{
          .stream_id = task.stream_id,
          .seq = task.append_create_seq,
          .file_id = task.file_id,
          .kind = task.kind,
          .name = task.name,
          .link_target = task.link_target,
          .prev_file_id = 0,
          .prev_final_size = 0,
      };
      const auto sent = try_send_ordered(yisync::Message{create},
                                         create.seq,
                                         task.stream_id,
                                         task.file_id,
                                         task.append_create_line_id);
      if (!sent) {
        return;
      }
      task.append_create_sent = true;
      std::cout << "SENDER send CREATE line=" << task.append_create_line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " kind=" << static_cast<int>(task.kind)
                << " seq=" << create.seq << "\n";
      return;
    }

    if (task.append_needs_create && !task.append_create_ready) {
      return;
    }

    if (task.append_data_sent) {
      return;
    }

    if (task.append_next_offset < task.source_size) {
      const auto data_len = std::min<std::uint64_t>(yisync::kDefaultChunkSizeBytes,
                                                   task.source_size - task.append_next_offset);
      auto payload = read_source_payload(task, task.append_next_offset, data_len);
      auto data = make_data_from_payload(task.stream_id,
                                         task.file_id,
                                         task.append_data_seq,
                                         task.append_next_offset,
                                         std::move(payload));
      const auto sent = try_send_ordered(yisync::Message{data},
                                         task.append_data_seq,
                                         task.stream_id,
                                         task.file_id,
                                         task.append_data_line_id);
      if (!sent) {
        return;
      }
      task.append_data_sent = true;
      task.append_current_data_len = data_len;
      std::cout << "SENDER send DATA line=" << task.append_data_line_id
                << " stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " seq=" << task.append_data_seq
                << " offset=" << task.append_next_offset
                << " len=" << data_len << "\n";
      return;
    }

  }

  void advance_stream_task(StreamSendState& stream) {
    auto* previous = active_task(stream);
    if (previous != nullptr && !previous->chunk_mode) {
      stream.next_append_seq = previous->append_done_next_seq;
    }

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
    loop_.stop();
  }

  bool try_send_ordered(const yisync::Message& message,
                        std::uint64_t seq,
                        std::uint64_t stream_id,
                        std::uint64_t file_id,
                        yisync::LineId& line_id_out) {
    const auto wire_bytes = encoded_message_size(message);
    const yisync::SendRequest request{
        .stream_id = stream_id,
        .file_id = file_id,
        .seq = seq,
        .bytes = wire_bytes,
        .split_allowed = false,
        .kind = yisync::SendKind::Ordered,
    };
    const auto grant = scheduler_.try_acquire(request);
    if (!grant.has_value()) {
      return false;
    }
    auto& line = find_line(grant->line_id);
    if (!line.connected || !line.connection || line.connection->closed()) {
      on_line_unavailable(line.id);
      return false;
    }
    line.connection->send(message);
    line_id_out = grant->line_id;
    return true;
  }

  void reset_append_plan_for_reconnect(StreamSendState& stream, FileSendTask& task) {
    task.append_create_sent = false;
    task.append_create_ready = false;
    task.append_data_sent = false;
    task.append_create_line_id = 0;
    task.append_data_line_id = 0;
    task.append_current_data_len = 0;
    stream.manifest_applied = false;
  }

  void on_heartbeat(yisync::LineId line_id, const yisync::Heartbeat& heartbeat) {
    scheduler_.on_heartbeat(line_id, heartbeat);
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
          task->append_needs_create &&
          task->append_create_sent &&
          !task->append_create_ready &&
          heartbeat.next_seq > task->append_create_seq) {
        task->append_create_ready = true;
        std::cout << "SENDER CREATE ready via heartbeat line=" << line_id
                  << " stream=" << task->stream_id
                  << " file_id=" << task->file_id << "\n";
      }
      if (heartbeat.file_id == task->file_id &&
          task->append_data_sent &&
          heartbeat.next_seq > task->append_data_seq) {
        task->append_next_offset += task->append_current_data_len;
        task->append_data_sent = false;
        task->append_data_line_id = 0;
        task->append_current_data_len = 0;
        task->append_data_seq += 1;
        std::cout << "SENDER DATA ready via heartbeat line=" << line_id
                  << " stream=" << task->stream_id
                  << " file_id=" << task->file_id
                  << " next_offset=" << task->append_next_offset << "\n";
      }
      if (heartbeat.file_id == task->file_id &&
          !task->append_data_sent &&
          task->append_next_offset >= task->source_size &&
          heartbeat.next_seq >= task->append_done_next_seq &&
          (task->kind != yisync::EntryKind::RegularFile ||
           heartbeat.durable_offset >= task->source_size)) {
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
      task->begin_wait_ticks = 0;
      std::cout << "SENDER FILE_BEGIN ready via heartbeat line=" << line_id
                << " stream=" << task->stream_id
                << " file_id=" << task->file_id << "\n";
    }

    for (const auto& received : heartbeat.received_chunks) {
      if (received.order_seq != task->order_seq ||
          received.file_id != task->file_id ||
          received.chunk_index >= task->chunk_acked.size()) {
        continue;
      }
      const auto index = static_cast<std::size_t>(received.chunk_index);
      task->chunk_acked[index] = true;
      task->chunk_sent[index] = true;
      task->chunk_line[index] = 0;
      task->chunk_priority[index] = false;
      std::cout << "SENDER chunk ack stream=" << task->stream_id
                << " file_id=" << task->file_id
                << " index=" << received.chunk_index
                << " line=" << line_id << "\n";
    }

    for (const auto& range : heartbeat.missing_ranges) {
      if (range.order_seq != task->order_seq || range.file_id != task->file_id) {
        continue;
      }
      if (task->chunk_count == 0) {
        continue;
      }
      const auto first = std::min<std::uint64_t>(range.first_chunk_index, task->chunk_count);
      const auto last = std::min<std::uint64_t>(range.last_chunk_index,
                                                task->chunk_count == 0 ? 0 : task->chunk_count - 1);
      if (first > last) {
        continue;
      }
      for (std::uint64_t chunk_index = first; chunk_index <= last; ++chunk_index) {
        const auto index = static_cast<std::size_t>(chunk_index);
        if (!task->chunk_acked[index]) {
          task->chunk_priority[index] = true;
        }
      }
      std::cout << "SENDER missing hint stream=" << task->stream_id
                << " file_id=" << task->file_id
                << " range=" << first << "-" << last
                << " line=" << line_id << "\n";
    }

    if (task->commit_sent && heartbeat.next_seq > task->order_seq) {
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
      const auto next_chunk = next_unsent_chunk_index(task);
      if (!next_chunk.has_value()) {
        break;
      }
      const auto chunk_index = *next_chunk;
      auto chunk = make_chunk_from_payload(task.stream_id,
                                           task.order_seq,
                                           task.file_id,
                                           chunk_index,
                                           read_chunk_payload(task, chunk_index));
      const auto wire_bytes = encoded_message_size(yisync::Message{chunk});
      const yisync::SendRequest request{
          .stream_id = chunk.stream_id,
          .file_id = chunk.file_id,
          .seq = chunk.order_seq,
          .bytes = wire_bytes,
          .split_allowed = false,
          .kind = yisync::SendKind::Chunk,
          .order_seq = chunk.order_seq,
          .chunk_index = chunk.chunk_index,
      };
      const auto grant = scheduler_.try_acquire(request);
      if (!grant.has_value()) {
        return;
      }
      auto& line = find_line(grant->line_id);
      if (!line.connected || !line.connection || line.connection->closed()) {
        on_line_unavailable(line.id);
        return;
      }
      line.connection->send(yisync::Message{chunk});
      const auto chunk_slot = static_cast<std::size_t>(chunk_index);
      const bool is_retransmit = task.chunk_attempts[chunk_slot] > 0;
      task.chunk_sent[chunk_slot] = true;
      task.chunk_line[chunk_slot] = grant->line_id;
      task.chunk_send_tick[chunk_slot] = current_tick_;
      task.chunk_attempts[chunk_slot] += 1;
      task.chunk_priority[chunk_slot] = false;
      std::cout << "SENDER send CHUNK stream=" << task.stream_id
                << " file_id=" << task.file_id
                << " index=" << chunk_index
                << " line=" << grant->line_id
                << " wire_bytes=" << wire_bytes
                << " attempt=" << task.chunk_attempts[chunk_slot]
                << " retransmit=" << is_retransmit << "\n";
      maybe_drop_line_once(grant->line_id);
    }

    if (all_chunks_acked(task)) {
      send_commit_if_possible(task);
    }
  }

  void send_commit_if_possible(FileSendTask& task) {
    if (task.commit_sent) {
      return;
    }
    const yisync::FileCommit commit{
        .stream_id = task.stream_id,
        .order_seq = task.order_seq,
        .file_id = task.file_id,
    };
    const auto wire_bytes = encoded_message_size(yisync::Message{commit});
    const yisync::SendRequest request{
        .stream_id = commit.stream_id,
        .file_id = commit.file_id,
        .seq = commit.order_seq,
        .bytes = wire_bytes,
        .split_allowed = false,
        .kind = yisync::SendKind::Ordered,
        .order_seq = commit.order_seq,
    };
    const auto grant = scheduler_.try_acquire(request);
    if (!grant.has_value()) {
      return;
    }
    auto& line = find_line(grant->line_id);
    if (!line.connected || !line.connection || line.connection->closed()) {
      on_line_unavailable(line.id);
      return;
    }
    line.connection->send(yisync::Message{commit});
    task.commit_sent = true;
    task.commit_line_id = grant->line_id;
    std::cout << "SENDER send FILE_COMMIT line=" << grant->line_id
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
    on_line_unavailable(line_id);
  }

  void requeue_timed_out_lines() {
    for (auto& stream : streams_) {
      auto* task = active_task(stream);
      if (task == nullptr || !task->begin_sent || task->begin_ready) {
        continue;
      }
      task->begin_wait_ticks += 1;
      if (task->begin_wait_ticks > kHeartbeatTimeoutTicks && task->begin_line_id != 0) {
        std::cerr << "SENDER FILE_BEGIN heartbeat timeout line=" << task->begin_line_id
                  << " stream=" << task->stream_id
                  << " file_id=" << task->file_id << "\n";
        on_line_unavailable(task->begin_line_id);
      }
    }

    for (const auto& snapshot : scheduler_.snapshots()) {
      if (snapshot.connected && snapshot.stale && snapshot.pending_sends > 0) {
        std::cerr << "SENDER line heartbeat timeout id=" << snapshot.id
                  << " pending=" << snapshot.pending_sends << "\n";
        on_line_unavailable(snapshot.id);
      }
    }
  }

  void tick() {
    if (failed_ || done_) {
      return;
    }
    current_tick_ += 1;
    scheduler_.refill_ticks(1);
    requeue_timed_out_lines();
    schedule_work();
    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  yisync::MultiLineScheduler scheduler_;
  std::vector<StreamSendState> streams_;
  std::vector<Line> lines_;
  std::uint64_t current_tick_ = 0;
  bool dropped_line_once_ = false;
  bool done_ = false;
  bool failed_ = false;
};

int run_sender(NodeOptions options) {
  return SenderApp(std::move(options)).run();
}

}  // namespace yisync::node
