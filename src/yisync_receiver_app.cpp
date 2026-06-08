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
namespace {

yisync::NackReason chunk_commit_error_reason(std::string_view error) {
  if (error.find("checksum") != std::string_view::npos) {
    return yisync::NackReason::ChecksumMismatch;
  }
  if (error.find("already exists") != std::string_view::npos) {
    return yisync::NackReason::FileExists;
  }
  return yisync::NackReason::IoError;
}

}  // namespace

class ReceiverApp {
 public:
  explicit ReceiverApp(NodeOptions options)
      : options_(std::move(options)) {
    lines_.reserve(options_.lines);
    for (std::uint32_t i = 1; i <= options_.lines; ++i) {
      lines_.push_back(Line{
          .id = i,
          .bind = yisync::Endpoint{options_.host, line_port(options_, i)},
      });
    }
  }

  int run() {
    std::filesystem::create_directories(options_.root);
    for (auto& line : lines_) {
      line.listener = yisync::listen_async_tcp(line.bind);
      const auto endpoint = line.listener->local_endpoint();
      std::cout << "RECEIVER listening line=" << line.id
                << " endpoint=" << endpoint.host << ":" << endpoint.port << "\n";
      line.listener->start(loop_, [this, id = line.id](int fd, yisync::Endpoint peer) {
        on_accept(id, fd, std::move(peer));
      });
    }

    loop_.call_later(kReceiverCheckpointInterval, [this] { checkpoint_tick(); });
    loop_.call_later(kReceiverHeartbeatInterval, [this] { heartbeat_tick(); });
    loop_.call_later(std::chrono::seconds(60), [this] {
      if (!committed_) {
        std::cerr << "RECEIVER timeout\n";
        failed_ = true;
        loop_.stop();
      }
    });
    loop_.run();
    return failed_ || !committed_ ? 1 : 0;
  }

 private:
  class DiskWriter {
   public:
    DiskWriter()
        : worker_([this] { run(); }) {}

    ~DiskWriter() {
      stop();
    }

    DiskWriter(const DiskWriter&) = delete;
    DiskWriter& operator=(const DiskWriter&) = delete;

    bool enqueue(std::function<void()> task) {
      if (stopping_.load(std::memory_order_acquire)) {
        return false;
      }
      const auto tail = tail_.load(std::memory_order_relaxed);
      const auto next_tail = increment(tail);
      if (next_tail == head_.load(std::memory_order_acquire)) {
        return false;
      }
      queue_[tail] = std::move(task);
      tail_.store(next_tail, std::memory_order_release);
      return true;
    }

    void drain() {
      while (head_.load(std::memory_order_acquire) != tail_.load(std::memory_order_acquire) ||
             busy_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    bool failed() const noexcept {
      return failed_.load(std::memory_order_acquire);
    }

    const char* error() const noexcept {
      return error_.data();
    }

    void stop() {
      const bool was_stopping = stopping_.exchange(true, std::memory_order_acq_rel);
      if (was_stopping) {
        return;
      }
      if (worker_.joinable()) {
        worker_.join();
      }
    }

   private:
    void run() {
      while (true) {
        const auto head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
          if (stopping_.load(std::memory_order_acquire)) {
            return;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        busy_.store(true, std::memory_order_release);
        auto task = std::move(queue_[head]);
        queue_[head] = nullptr;
        head_.store(increment(head), std::memory_order_release);
        try {
          task();
        } catch (const std::exception& ex) {
          const auto message = std::string_view(ex.what());
          const auto count = std::min<std::size_t>(message.size(), error_.size() - 1);
          std::copy_n(message.begin(), count, error_.begin());
          error_[count] = '\0';
          failed_.store(true, std::memory_order_release);
        }
        busy_.store(false, std::memory_order_release);
      }
    }

    static constexpr std::size_t kCapacity = kDiskWriterQueueCapacity + 1;

    static std::size_t increment(std::size_t value) noexcept {
      return (value + 1) % kCapacity;
    }

    std::array<std::function<void()>, kCapacity> queue_;
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
    std::atomic<bool> busy_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> failed_{false};
    std::array<char, 160> error_{};
    std::thread worker_;
  };

  struct Line {
    yisync::LineId id = 0;
    yisync::Endpoint bind;
    std::unique_ptr<yisync::AsyncTcpListener> listener;
    std::shared_ptr<yisync::AsyncFrameConnection> connection;
  };

  struct ChunkCommitCompletion {
    std::atomic<bool> completed{false};
    std::atomic<bool> failed{false};
    std::array<char, 160> error{};
  };

  struct StreamReceivers {
    std::uint64_t stream_id = 0;
    std::filesystem::path root;
    std::unique_ptr<yisync::ReceiverStream> append;
    std::unique_ptr<yisync::ChunkedReceiverStream> chunk;
    std::uint64_t append_durable_file_id = 0;
    std::filesystem::path append_durable_path;
    std::uint64_t append_durable_offset = 0;
    std::uint64_t append_durable_target = 0;
    std::uint64_t append_fsync_active_target = 0;
    yisync::LineId append_heartbeat_line_id = 0;
    bool append_fsync_queued = false;
    std::atomic<std::uint64_t> append_fsync_completed{0};
    std::atomic<bool> append_fsync_failed{false};
    std::array<char, 160> append_fsync_error{};
    bool chunk_commit_queued = false;
    yisync::LineId chunk_commit_line_id = 0;
    std::uint64_t chunk_commit_order_seq = 0;
    std::uint64_t chunk_commit_file_id = 0;
    std::uint64_t chunk_commit_final_size = 0;
    std::filesystem::path chunk_commit_final_path;
    std::shared_ptr<ChunkCommitCompletion> chunk_commit_completion;

    StreamReceivers() = default;
    StreamReceivers(const StreamReceivers&) = delete;
    StreamReceivers& operator=(const StreamReceivers&) = delete;

    StreamReceivers(StreamReceivers&& other) noexcept
        : stream_id(other.stream_id),
          root(std::move(other.root)),
          append(std::move(other.append)),
          chunk(std::move(other.chunk)),
          append_durable_file_id(other.append_durable_file_id),
          append_durable_path(std::move(other.append_durable_path)),
          append_durable_offset(other.append_durable_offset),
          append_durable_target(other.append_durable_target),
          append_fsync_active_target(other.append_fsync_active_target),
          append_heartbeat_line_id(other.append_heartbeat_line_id),
          append_fsync_queued(other.append_fsync_queued),
          append_fsync_completed(other.append_fsync_completed.load(std::memory_order_relaxed)),
          append_fsync_failed(other.append_fsync_failed.load(std::memory_order_relaxed)),
          append_fsync_error(other.append_fsync_error),
          chunk_commit_queued(other.chunk_commit_queued),
          chunk_commit_line_id(other.chunk_commit_line_id),
          chunk_commit_order_seq(other.chunk_commit_order_seq),
          chunk_commit_file_id(other.chunk_commit_file_id),
          chunk_commit_final_size(other.chunk_commit_final_size),
          chunk_commit_final_path(std::move(other.chunk_commit_final_path)),
          chunk_commit_completion(std::move(other.chunk_commit_completion)) {}

    StreamReceivers& operator=(StreamReceivers&& other) noexcept {
      if (this == &other) {
        return *this;
      }
      stream_id = other.stream_id;
      root = std::move(other.root);
      append = std::move(other.append);
      chunk = std::move(other.chunk);
      append_durable_file_id = other.append_durable_file_id;
      append_durable_path = std::move(other.append_durable_path);
      append_durable_offset = other.append_durable_offset;
      append_durable_target = other.append_durable_target;
      append_fsync_active_target = other.append_fsync_active_target;
      append_heartbeat_line_id = other.append_heartbeat_line_id;
      append_fsync_queued = other.append_fsync_queued;
      append_fsync_completed.store(other.append_fsync_completed.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);
      append_fsync_failed.store(other.append_fsync_failed.load(std::memory_order_relaxed),
                                std::memory_order_relaxed);
      append_fsync_error = other.append_fsync_error;
      chunk_commit_queued = other.chunk_commit_queued;
      chunk_commit_line_id = other.chunk_commit_line_id;
      chunk_commit_order_seq = other.chunk_commit_order_seq;
      chunk_commit_file_id = other.chunk_commit_file_id;
      chunk_commit_final_size = other.chunk_commit_final_size;
      chunk_commit_final_path = std::move(other.chunk_commit_final_path);
      chunk_commit_completion = std::move(other.chunk_commit_completion);
      return *this;
    }
  };

  struct PendingHeartbeat {
    yisync::LineId line_id = 0;
    std::uint64_t stream_id = 0;
    yisync::Heartbeat heartbeat;
    bool dirty = false;
  };

  struct CommittedFileInfo {
    std::filesystem::path path;
    std::uint64_t size = 0;
  };

  struct HeartbeatKey {
    yisync::LineId line_id = 0;
    std::uint64_t stream_id = 0;

    bool operator==(const HeartbeatKey& other) const noexcept {
      return line_id == other.line_id && stream_id == other.stream_id;
    }
  };

  struct HeartbeatKeyHash {
    std::size_t operator()(const HeartbeatKey& key) const noexcept {
      const auto lhs = std::hash<yisync::LineId>{}(key.line_id);
      const auto rhs = std::hash<std::uint64_t>{}(key.stream_id);
      return lhs ^ (rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6) + (lhs >> 2));
    }
  };

  Line& find_line(yisync::LineId id) {
    auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
      return line.id == id;
    });
    if (it == lines_.end()) {
      throw std::runtime_error("unknown receiver line");
    }
    return *it;
  }

  std::filesystem::path stream_root_for(std::uint64_t stream_id) const {
    if (stream_id == kStreamId) {
      return options_.root;
    }
    return options_.root / std::to_string(stream_id);
  }

  CommittedFileInfo committed_file_info(std::uint64_t stream_id, std::uint64_t file_id) const {
    const auto root = stream_root_for(stream_id);
    const auto manifest = yisync::scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
    auto path = root / yisync::file_name_for_id(file_id);
    auto size = local_file_size_or_zero(path);
    const auto entry_it = std::find_if(manifest.entries.begin(), manifest.entries.end(), [&](const auto& entry) {
      return entry.file_id == file_id;
    });
    if (entry_it != manifest.entries.end()) {
      path = root / entry_it->name;
      size = entry_it->size;
    }
    return CommittedFileInfo{.path = std::move(path), .size = size};
  }

  StreamReceivers& receivers_for(std::uint64_t stream_id) {
    auto it = stream_receivers_.find(stream_id);
    if (it != stream_receivers_.end()) {
      return it->second;
    }
    StreamReceivers context;
    context.stream_id = stream_id;
    context.root = stream_root_for(stream_id);
    std::filesystem::create_directories(context.root);
    auto [inserted, _] = stream_receivers_.emplace(stream_id, std::move(context));
    return inserted->second;
  }

  yisync::ReceiverStream& append_receiver_for(std::uint64_t stream_id) {
    auto& context = receivers_for(stream_id);
    if (!context.append) {
      context.append = std::make_unique<yisync::ReceiverStream>(stream_id, context.root);
    }
    return *context.append;
  }

  yisync::ChunkedReceiverStream& chunk_receiver_for(std::uint64_t stream_id) {
    auto& context = receivers_for(stream_id);
    if (!context.chunk) {
      context.chunk = std::make_unique<yisync::ChunkedReceiverStream>(stream_id, context.root);
    }
    return *context.chunk;
  }

  void reset_append_durable_context(StreamReceivers& context,
                                    std::uint64_t file_id,
                                    std::filesystem::path path,
                                    std::uint64_t durable_offset) {
    if (context.append_durable_file_id == file_id) {
      if (!path.empty()) {
        context.append_durable_path = std::move(path);
      }
      return;
    }
    context.append_durable_file_id = file_id;
    context.append_durable_path = std::move(path);
    context.append_durable_offset = durable_offset;
    context.append_durable_target = durable_offset;
    context.append_fsync_active_target = 0;
    context.append_fsync_queued = false;
    context.append_fsync_completed.store(durable_offset, std::memory_order_release);
    context.append_fsync_failed.store(false, std::memory_order_release);
    std::fill(context.append_fsync_error.begin(), context.append_fsync_error.end(), '\0');
  }

  void maybe_enqueue_append_fsync(StreamReceivers& context,
                                  std::uint64_t target_offset) {
    if (target_offset <= context.append_durable_offset) {
      return;
    }
    context.append_durable_target = std::max(context.append_durable_target, target_offset);
    if (context.append_fsync_queued) {
      return;
    }

    auto* completed = &context.append_fsync_completed;
    auto* failed = &context.append_fsync_failed;
    auto* error = &context.append_fsync_error;
    const auto path = context.append_durable_path;
    if (path.empty()) {
      std::cerr << "RECEIVER append fsync missing path stream=" << context.stream_id
                << " target_offset=" << target_offset << "\n";
      return;
    }
    const auto durable_target = context.append_durable_target;
    std::fill(error->begin(), error->end(), '\0');
    failed->store(false, std::memory_order_release);

    const auto queued = disk_writer_.enqueue([path, durable_target, completed, failed, error] {
      try {
        fsync_file_for_durable_offset(path);
        completed->store(durable_target, std::memory_order_release);
      } catch (const std::exception& ex) {
        const auto message = std::string_view(ex.what());
        const auto count = std::min<std::size_t>(message.size(), error->size() - 1);
        std::copy_n(message.begin(), count, error->begin());
        (*error)[count] = '\0';
        failed->store(true, std::memory_order_release);
      }
    });
    if (queued) {
      context.append_fsync_queued = true;
      context.append_fsync_active_target = durable_target;
    } else {
      std::cerr << "RECEIVER append fsync writer queue full stream=" << context.stream_id
                << " target_offset=" << target_offset << "\n";
    }
  }

  void poll_append_fsync(StreamReceivers& context) {
    if (context.append_fsync_failed.load(std::memory_order_acquire)) {
      std::cerr << "RECEIVER append fsync failed stream=" << context.stream_id
                << " error=" << context.append_fsync_error.data() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    const auto completed = context.append_fsync_completed.load(std::memory_order_acquire);
    if (completed > context.append_durable_offset) {
      context.append_durable_offset = completed;
      if (context.append && context.append_heartbeat_line_id != 0) {
        queue_heartbeat(context.append_heartbeat_line_id,
                        context.append->heartbeat(kLineWindowBytes,
                                                  context.append_durable_offset));
      }
    }
    if (context.append_fsync_queued && completed >= context.append_fsync_active_target) {
      context.append_fsync_queued = false;
      context.append_fsync_active_target = 0;
    }
    if (!context.append_fsync_queued && context.append_durable_target > context.append_durable_offset) {
      maybe_enqueue_append_fsync(context, context.append_durable_target);
    }
  }

  void poll_chunk_commit(StreamReceivers& context) {
    if (!context.chunk_commit_queued) {
      return;
    }
    auto completion = context.chunk_commit_completion;
    if (!completion) {
      std::cerr << "RECEIVER chunk commit missing completion stream=" << context.stream_id << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    if (completion->failed.load(std::memory_order_acquire)) {
      std::cerr << "RECEIVER chunk commit failed stream=" << context.stream_id
                << " file_id=" << context.chunk_commit_file_id
                << " error=" << completion->error.data() << "\n";
      if (context.chunk) {
        context.chunk->abort_commit(context.chunk_commit_order_seq);
      }
      if (context.chunk_commit_line_id != 0 && can_send_on_line(context.chunk_commit_line_id)) {
        send_nack(context.chunk_commit_line_id,
                  yisync::Nack{
                      .stream_id = context.stream_id,
                      .got_seq = context.chunk_commit_order_seq,
                      .expected_seq = context.chunk ? context.chunk->expected_order_seq() : context.chunk_commit_order_seq,
                      .file_id = context.chunk_commit_file_id,
                      .offset = 0,
                      .expected_file_id = context.chunk_commit_file_id,
                      .expected_offset = 0,
                      .reason = chunk_commit_error_reason(completion->error.data()),
                      .detail = completion->error.data(),
                  });
      } else {
        std::cerr << "RECEIVER chunk commit failed after line closed stream="
                  << context.stream_id
                  << " file_id=" << context.chunk_commit_file_id << "\n";
      }
      context.chunk_commit_queued = false;
      context.chunk_commit_completion.reset();
      failed_ = true;
      loop_.stop();
      return;
    }
    if (!completion->completed.load(std::memory_order_acquire)) {
      return;
    }
    if (!context.chunk) {
      std::cerr << "RECEIVER chunk commit completed without receiver stream="
                << context.stream_id << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    yisync::ChunkCommitResult result{
        .stream_id = context.stream_id,
        .order_seq = context.chunk_commit_order_seq,
        .file_id = context.chunk_commit_file_id,
        .final_size = context.chunk_commit_final_size,
        .final_path = context.chunk_commit_final_path,
    };
    try {
      context.chunk->finish_commit(result);
    } catch (const std::exception& ex) {
      std::cerr << "RECEIVER chunk commit finish failed stream=" << context.stream_id
                << " error=" << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }

    if (context.append) {
      context.append->refresh_committed_from_disk();
    }
    if (context.chunk_commit_line_id != 0 && can_send_on_line(context.chunk_commit_line_id)) {
      queue_heartbeat(context.chunk_commit_line_id,
                      yisync::Heartbeat{
                          .stream_id = context.stream_id,
                          .next_seq = context.chunk->expected_order_seq(),
                          .file_id = context.chunk_commit_file_id,
                          .offset = context.chunk_commit_final_size,
                          .durable_offset = context.chunk_commit_final_size,
                          .recv_window_bytes = kLineWindowBytes,
                      });
      flush_heartbeat(context.chunk_commit_line_id);
    }
    std::cout << "RECEIVER FILE_COMMIT complete line=" << context.chunk_commit_line_id
              << " stream=" << context.stream_id
              << " file_id=" << context.chunk_commit_file_id
              << " final_path=" << context.chunk_commit_final_path
              << " expected_order_seq=" << context.chunk->expected_order_seq() << "\n";

    context.chunk_commit_queued = false;
    context.chunk_commit_line_id = 0;
    context.chunk_commit_order_seq = 0;
    context.chunk_commit_file_id = 0;
    context.chunk_commit_final_size = 0;
    context.chunk_commit_final_path.clear();
    context.chunk_commit_completion.reset();
    schedule_quiet_stop();
  }

  std::vector<std::pair<std::uint64_t, std::filesystem::path>> manifest_roots() const {
    std::vector<std::pair<std::uint64_t, std::filesystem::path>> roots;
    roots.push_back({kStreamId, options_.root});

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
      if (!failed_ && committed_ && generation == activity_generation_ && append_durable_idle()) {
        loop_.stop();
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_) {
        schedule_quiet_stop_retry(generation);
      }
    });
  }

  void schedule_quiet_stop_retry(std::uint64_t generation) {
    loop_.call_later(kReceiverHeartbeatInterval, [this, generation] {
      if (!failed_ && committed_ && generation == activity_generation_ && append_durable_idle()) {
        loop_.stop();
        return;
      }
      if (!failed_ && committed_ && generation == activity_generation_) {
        schedule_quiet_stop_retry(generation);
      }
    });
  }

  bool append_durable_idle() const {
    for (const auto& [stream_id, context] : stream_receivers_) {
      (void)stream_id;
      if (context.append_fsync_queued || context.append_durable_target > context.append_durable_offset) {
        return false;
      }
      if (context.chunk_commit_queued) {
        return false;
      }
    }
    return true;
  }

  void on_accept(yisync::LineId id, int fd, yisync::Endpoint peer) {
    auto& line = find_line(id);
    if (line.connection && !line.connection->closed()) {
      ::close(fd);
      return;
    }
    auto connection = std::make_shared<yisync::AsyncFrameConnection>(fd, kMaxFrameBytes);
    connection->on_message([this, id](yisync::Message message) {
      on_message(id, std::move(message));
    });
    connection->on_error([this, id](std::string error) {
      std::cerr << "RECEIVER line error id=" << id << " error=" << error << "\n";
      failed_ = true;
      loop_.stop();
    });
    connection->on_close([this, id] {
      if (!committed_) {
        std::cerr << "RECEIVER line closed before commit id=" << id << "\n";
      }
    });
    connection->start(loop_);
    line.connection = std::move(connection);
    std::cout << "RECEIVER accepted line=" << id
              << " peer=" << peer.host << ":" << peer.port << "\n";
    send_manifest(id);
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
    activity_generation_ += 1;
    if (const auto* create = std::get_if<yisync::Create>(&message)) {
      apply_create(line_id, *create);
      return;
    }
    if (const auto* data = std::get_if<yisync::Data>(&message)) {
      apply_data(line_id, *data);
      return;
    }
    if (const auto* begin = std::get_if<yisync::FileBegin>(&message)) {
      apply_begin(line_id, *begin);
      return;
    }
    if (const auto* chunk = std::get_if<yisync::Chunk>(&message)) {
      apply_chunk(line_id, *chunk);
      return;
    }
    if (const auto* commit = std::get_if<yisync::FileCommit>(&message)) {
      apply_commit(line_id, *commit);
      return;
    }
    std::cerr << "RECEIVER unexpected message line=" << line_id << "\n";
    failed_ = true;
    loop_.stop();
  }

  void send_message(yisync::LineId line_id, const yisync::Message& message) {
    auto& line = find_line(line_id);
    if (!line.connection || line.connection->closed()) {
      throw std::runtime_error("receiver line has no active connection");
    }
    line.connection->send(message);
  }

  bool can_send_on_line(yisync::LineId line_id) {
    auto& line = find_line(line_id);
    return line.connection && !line.connection->closed();
  }

  void send_nack(yisync::LineId line_id, const yisync::Nack& nack) {
    send_message(line_id, yisync::Message{nack});
  }

  void queue_heartbeat(yisync::LineId line_id, yisync::Heartbeat heartbeat) {
    const HeartbeatKey key{line_id, heartbeat.stream_id};
    auto& pending = pending_heartbeats_[key];
    if (!pending.dirty) {
      pending.line_id = line_id;
      pending.stream_id = heartbeat.stream_id;
      pending.heartbeat = std::move(heartbeat);
      pending.dirty = true;
      return;
    }

    auto& target = pending.heartbeat;
    target.stream_id = heartbeat.stream_id;
    target.next_seq = heartbeat.next_seq;
    target.file_id = heartbeat.file_id;
    target.offset = heartbeat.offset;
    target.durable_offset = heartbeat.durable_offset;
    target.recv_window_bytes = heartbeat.recv_window_bytes;
    target.received_chunks.insert(target.received_chunks.end(),
                                  heartbeat.received_chunks.begin(),
                                  heartbeat.received_chunks.end());
    target.missing_ranges = std::move(heartbeat.missing_ranges);
  }

  void flush_heartbeat(yisync::LineId line_id) {
    std::vector<HeartbeatKey> keys;
    for (const auto& [key, pending] : pending_heartbeats_) {
      if (pending.dirty && key.line_id == line_id) {
        keys.push_back(key);
      }
    }
    for (const auto& key : keys) {
      auto it = pending_heartbeats_.find(key);
      if (it == pending_heartbeats_.end() || !it->second.dirty) {
        continue;
      }
      auto heartbeat = std::move(it->second.heartbeat);
      it->second = PendingHeartbeat{};
      send_message(line_id, yisync::Message{heartbeat});
    }
  }

  void flush_all_heartbeats() {
    std::vector<HeartbeatKey> keys;
    keys.reserve(pending_heartbeats_.size());
    for (const auto& [key, pending] : pending_heartbeats_) {
      if (pending.dirty) {
        keys.push_back(key);
      }
    }
    for (const auto& key : keys) {
      auto it = pending_heartbeats_.find(key);
      if (it == pending_heartbeats_.end() || !it->second.dirty) {
        continue;
      }
      auto heartbeat = std::move(it->second.heartbeat);
      it->second = PendingHeartbeat{};
      send_message(key.line_id, yisync::Message{heartbeat});
    }
  }

  void heartbeat_tick() {
    if (failed_) {
      return;
    }
    if (disk_writer_.failed()) {
      std::cerr << "RECEIVER disk writer failed: " << disk_writer_.error() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    poll_disk_writer_completions();
    flush_all_heartbeats();
    loop_.call_later(kReceiverHeartbeatInterval, [this] { heartbeat_tick(); });
  }

  bool has_pending_chunk_commit() const {
    for (const auto& [stream_id, context] : stream_receivers_) {
      (void)stream_id;
      if (context.chunk_commit_queued) {
        return true;
      }
    }
    return false;
  }

  void schedule_commit_poll() {
    if (commit_poll_scheduled_) {
      return;
    }
    commit_poll_scheduled_ = true;
    loop_.call_later(kReceiverCommitPollInterval, [this] { commit_poll_tick(); });
  }

  void commit_poll_tick() {
    commit_poll_scheduled_ = false;
    if (failed_) {
      return;
    }
    if (disk_writer_.failed()) {
      std::cerr << "RECEIVER disk writer failed: " << disk_writer_.error() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    poll_disk_writer_completions();
    if (!failed_ && has_pending_chunk_commit()) {
      schedule_commit_poll();
    }
  }

  void poll_disk_writer_completions() {
    for (auto& [stream_id, context] : stream_receivers_) {
      (void)stream_id;
      poll_append_fsync(context);
      if (failed_) {
        return;
      }
      poll_chunk_commit(context);
      if (failed_) {
        return;
      }
    }
  }

  void send_manifest(yisync::LineId line_id) {
    yisync::Manifest manifest;
    manifest.manifest_id = next_manifest_id_++;
    const auto roots = manifest_roots();
    std::vector<std::string> stream_dir_names;
    for (const auto& [stream_id, root] : roots) {
      if (stream_id != kStreamId) {
        stream_dir_names.push_back(root.filename().string());
      }
    }
    for (const auto& [stream_id, root] : roots) {
      if (stream_id == kStreamId) {
        manifest.streams.push_back(yisync::scan_manifest_stream(stream_id,
                                                                root,
                                                                4 * 1024 * 1024,
                                                                stream_dir_names));
      } else {
        manifest.streams.push_back(yisync::scan_manifest_stream(stream_id, root, 4 * 1024 * 1024));
      }
    }
    std::uint64_t entry_count = 0;
    std::uint64_t incomplete_count = 0;
    for (const auto& stream : manifest.streams) {
      entry_count += stream.entries.size();
      incomplete_count += stream.incomplete_chunks.size();
    }
    send_message(line_id, yisync::Message{manifest});
    std::cout << "RECEIVER MANIFEST line=" << line_id
              << " manifest_id=" << manifest.manifest_id
              << " streams=" << manifest.streams.size()
              << " entries=" << entry_count
              << " incomplete_chunks=" << incomplete_count << "\n";
  }

  void apply_create(yisync::LineId line_id, const yisync::Create& create) {
    auto& append_receiver = append_receiver_for(create.stream_id);
    const auto result = append_receiver.apply(create);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    auto& context = receivers_for(create.stream_id);
    reset_append_durable_context(context, create.file_id, append_receiver.current_path(), 0);
    if (context.chunk) {
      context.chunk->refresh_committed_from_disk();
    }
    queue_heartbeat(line_id,
                    append_receiver.heartbeat(kLineWindowBytes,
                                              append_receiver.state().current_offset));
    flush_heartbeat(line_id);
    std::cout << "RECEIVER CREATE line=" << line_id
              << " stream=" << create.stream_id
              << " file_id=" << create.file_id
              << " seq=" << create.seq << "\n";
    schedule_quiet_stop();
  }

  void apply_data(yisync::LineId line_id, const yisync::Data& data) {
    auto& append_receiver = append_receiver_for(data.stream_id);
    const auto result = append_receiver.apply(data);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    auto& context = receivers_for(data.stream_id);
    context.append_heartbeat_line_id = line_id;
    reset_append_durable_context(context, data.file_id, append_receiver.current_path(), data.offset);
    maybe_enqueue_append_fsync(context, append_receiver.state().current_offset);
    queue_heartbeat(line_id,
                    append_receiver.heartbeat(kLineWindowBytes,
                                              context.append_durable_offset));
    if (context.chunk) {
      context.chunk->refresh_committed_from_disk();
    }
    std::cout << "RECEIVER DATA line=" << line_id
              << " stream=" << data.stream_id
              << " file_id=" << data.file_id
              << " seq=" << data.seq
              << " offset=" << data.offset
              << " len=" << data.raw_len << "\n";
    schedule_quiet_stop();
  }

  void apply_begin(yisync::LineId line_id, const yisync::FileBegin& begin) {
    auto& receiver = chunk_receiver_for(begin.stream_id);
    const auto result = receiver.apply(begin);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    queue_heartbeat(line_id,
                    yisync::Heartbeat{
                        .stream_id = begin.stream_id,
                        .next_seq = receiver.expected_order_seq(),
                        .file_id = begin.file_id,
                        .offset = 0,
                        .durable_offset = 0,
                        .recv_window_bytes = kLineWindowBytes,
                    });
    flush_heartbeat(line_id);
    std::cout << "RECEIVER FILE_BEGIN line=" << line_id
              << " stream=" << begin.stream_id
              << " file_id=" << begin.file_id
              << " chunks=" << begin.chunk_count
              << " size=" << begin.final_size << "\n";
  }

  void apply_chunk(yisync::LineId line_id, const yisync::Chunk& chunk) {
    auto& receiver = chunk_receiver_for(chunk.stream_id);
    const auto result = receiver.apply(chunk);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    queue_heartbeat(line_id,
                    yisync::Heartbeat{
                        .stream_id = chunk.stream_id,
                        .next_seq = receiver.expected_order_seq(),
                        .file_id = chunk.file_id,
                        .offset = 0,
                        .durable_offset = 0,
                        .recv_window_bytes = kLineWindowBytes,
                        .received_chunks = {
                            yisync::ReceivedChunk{
                                .order_seq = chunk.order_seq,
                                .file_id = chunk.file_id,
                                .chunk_index = chunk.chunk_index,
                            },
                        },
                        .missing_ranges = receiver.missing_ranges(chunk.order_seq,
                                                                  kMaxMissingRangesPerHeartbeat),
                    });
    std::cout << "RECEIVER CHUNK line=" << line_id
              << " stream=" << chunk.stream_id
              << " file_id=" << chunk.file_id
              << " index=" << chunk.chunk_index
              << " offset=" << chunk.offset
              << " len=" << chunk.raw_len << "\n";
    if (receiver.pending_checkpoint_bytes() >= kReceiverCheckpointBytes && !checkpoint_scheduled_) {
      checkpoint_scheduled_ = true;
      loop_.call_later(std::chrono::milliseconds(0), [this] { checkpoint_now(); });
    }
  }

  void apply_commit(yisync::LineId line_id, const yisync::FileCommit& commit) {
    auto& context = receivers_for(commit.stream_id);
    if (context.chunk_commit_queued) {
      if (context.chunk_commit_order_seq == commit.order_seq &&
          context.chunk_commit_file_id == commit.file_id) {
        std::cout << "RECEIVER FILE_COMMIT duplicate while queued line=" << line_id
                  << " stream=" << commit.stream_id
                  << " file_id=" << commit.file_id << "\n";
        return;
      }
      send_nack(line_id,
                yisync::Nack{
                    .stream_id = commit.stream_id,
                    .got_seq = commit.order_seq,
                    .expected_seq = context.chunk ? context.chunk->expected_order_seq() : commit.order_seq,
                    .file_id = commit.file_id,
                    .offset = 0,
                    .expected_file_id = context.chunk_commit_file_id,
                    .expected_offset = 0,
                    .reason = yisync::NackReason::BadCommit,
                    .detail = "another commit is already pending",
                });
      return;
    }

    auto& receiver = chunk_receiver_for(commit.stream_id);
    yisync::ChunkCommitTask task;
    const auto result = receiver.prepare_commit(commit, task);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    if (task.order_seq == 0) {
      if (commit.order_seq < receiver.expected_order_seq()) {
        const auto info = committed_file_info(commit.stream_id, commit.file_id);
        queue_heartbeat(line_id,
                        yisync::Heartbeat{
                            .stream_id = commit.stream_id,
                            .next_seq = receiver.expected_order_seq(),
                            .file_id = commit.file_id,
                            .offset = info.size,
                            .durable_offset = info.size,
                            .recv_window_bytes = kLineWindowBytes,
                        });
        flush_heartbeat(line_id);
        std::cout << "RECEIVER FILE_COMMIT duplicate after complete line=" << line_id
                  << " stream=" << commit.stream_id
                  << " file_id=" << commit.file_id
                  << " final_path=" << info.path
                  << " expected_order_seq=" << receiver.expected_order_seq() << "\n";
      }
      return;
    }

    context.chunk_commit_queued = true;
    context.chunk_commit_line_id = line_id;
    context.chunk_commit_order_seq = task.order_seq;
    context.chunk_commit_file_id = task.file_id;
    context.chunk_commit_final_size = task.final_size;
    context.chunk_commit_final_path = task.final_path;
    auto completion = std::make_shared<ChunkCommitCompletion>();
    context.chunk_commit_completion = completion;

    const auto queued = disk_writer_.enqueue([task = std::move(task), completion] {
      try {
        (void)yisync::ChunkedReceiverStream::write_commit_task(task);
        completion->completed.store(true, std::memory_order_release);
      } catch (const std::exception& ex) {
        const auto message = std::string_view(ex.what());
        const auto count = std::min<std::size_t>(message.size(), completion->error.size() - 1);
        std::copy_n(message.begin(), count, completion->error.begin());
        completion->error[count] = '\0';
        completion->failed.store(true, std::memory_order_release);
      }
    });
    if (!queued) {
      receiver.abort_commit(commit.order_seq);
      context.chunk_commit_queued = false;
      context.chunk_commit_line_id = 0;
      context.chunk_commit_order_seq = 0;
      context.chunk_commit_file_id = 0;
      context.chunk_commit_final_size = 0;
      context.chunk_commit_final_path.clear();
      context.chunk_commit_completion.reset();
      send_nack(line_id,
                yisync::Nack{
                    .stream_id = commit.stream_id,
                    .got_seq = commit.order_seq,
                    .expected_seq = receiver.expected_order_seq(),
                    .file_id = commit.file_id,
                    .offset = 0,
                    .expected_file_id = commit.file_id,
                    .expected_offset = 0,
                    .reason = yisync::NackReason::IoError,
                    .detail = "disk writer queue full while enqueueing commit",
                });
      return;
    }

    std::cout << "RECEIVER FILE_COMMIT queued line=" << line_id
              << " stream=" << commit.stream_id
              << " file_id=" << commit.file_id
              << " final_path=" << context.chunk_commit_final_path
              << " expected_order_seq=" << receiver.expected_order_seq() << "\n";
    schedule_commit_poll();
  }

  void checkpoint_tick() {
    if (failed_) {
      return;
    }
    checkpoint_now();
    loop_.call_later(kReceiverCheckpointInterval, [this] { checkpoint_tick(); });
  }

  void checkpoint_now() {
    if (failed_) {
      checkpoint_scheduled_ = false;
      return;
    }
    try {
      for (auto& [stream_id, context] : stream_receivers_) {
        (void)stream_id;
        if (!context.chunk) {
          continue;
        }
        auto batch = context.chunk->checkpoint();
        if (batch.result.chunks != 0) {
          bool all_queued = true;
          for (auto task : batch.tasks) {
            const auto queued = disk_writer_.enqueue([task = std::move(task)] {
              yisync::ChunkedReceiverStream::write_checkpoint_task(task);
            });
            if (!queued) {
              all_queued = false;
              break;
            }
          }
          if (!all_queued) {
            std::cerr << "RECEIVER checkpoint writer queue full stream=" << context.stream_id << "\n";
            failed_ = true;
            loop_.stop();
            return;
          }
          std::cout << "RECEIVER CHECKPOINT stream=" << context.stream_id
                    << " files=" << batch.result.files
                    << " chunks=" << batch.result.chunks
                    << " bytes=" << batch.result.bytes
                    << " queued=" << all_queued << "\n";
          total_checkpointed_chunks_ += batch.result.chunks;
          if (options_.exit_after_checkpoint_chunks != 0 &&
              total_checkpointed_chunks_ >= options_.exit_after_checkpoint_chunks) {
            std::cout << "RECEIVER exit after checkpoint chunks=" << total_checkpointed_chunks_ << "\n";
            loop_.stop();
            return;
          }
        }
      }
    } catch (const std::exception& ex) {
      std::cerr << "RECEIVER checkpoint failed: " << ex.what() << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    checkpoint_scheduled_ = false;
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  DiskWriter disk_writer_;
  std::unordered_map<std::uint64_t, StreamReceivers> stream_receivers_;
  std::unordered_map<HeartbeatKey, PendingHeartbeat, HeartbeatKeyHash> pending_heartbeats_;
  std::vector<Line> lines_;
  std::uint64_t next_manifest_id_ = 1;
  std::uint64_t total_checkpointed_chunks_ = 0;
  std::uint64_t activity_generation_ = 0;
  bool checkpoint_scheduled_ = false;
  bool commit_poll_scheduled_ = false;
  bool committed_ = false;
  bool failed_ = false;
};

int run_receiver(NodeOptions options) {
  return ReceiverApp(std::move(options)).run();
}

}  // namespace yisync::node
