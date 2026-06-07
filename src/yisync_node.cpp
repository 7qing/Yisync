#include "yisync_async.hpp"
#include "yisync_protocol.hpp"
#include "yisync_scheduler.hpp"
#include "yisync_transport.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>

namespace {

constexpr std::uint64_t kStreamId = 9001;
constexpr std::uint64_t kOrderSeq = 1;
constexpr std::uint64_t kFileId = 1;
constexpr std::uint64_t kLineBudgetBytes = 96 * 1024;
constexpr std::uint64_t kLineWindowBytes = 2 * kLineBudgetBytes;
constexpr std::uint64_t kMaxFrameBytes = 1024 * 1024;

struct NodeOptions {
  std::string mode;
  std::string host = "127.0.0.1";
  std::uint16_t base_port = 19000;
  std::uint32_t lines = 2;
  std::uint64_t size = 150 * 1024;
  std::filesystem::path root = std::filesystem::temp_directory_path() / "yisync_ab_sink";
};

yisync::Bytes make_source_bytes(std::uint64_t size) {
  yisync::Bytes bytes;
  bytes.reserve(static_cast<std::size_t>(size));
  for (std::uint64_t i = 0; i < size; ++i) {
    bytes.push_back(static_cast<std::byte>('A' + (i % 26)));
  }
  return bytes;
}

yisync::FileChecksum full_crc32c_checksum(const yisync::Bytes& bytes) {
  return yisync::FileChecksum{
      .algo = yisync::ChecksumAlgo::Crc32c,
      .scope = yisync::ChecksumScope::Full,
      .offset = 0,
      .len = static_cast<std::uint64_t>(bytes.size()),
      .value = yisync::crc32c_bytes(bytes),
  };
}

yisync::Chunk make_chunk(std::uint64_t chunk_index, const yisync::Bytes& source) {
  const auto offset = chunk_index * yisync::kDefaultChunkSizeBytes;
  const auto len = std::min<std::uint64_t>(yisync::kDefaultChunkSizeBytes, source.size() - offset);
  yisync::Bytes payload(source.begin() + static_cast<std::ptrdiff_t>(offset),
                        source.begin() + static_cast<std::ptrdiff_t>(offset + len));
  return yisync::Chunk{
      .stream_id = kStreamId,
      .order_seq = kOrderSeq,
      .file_id = kFileId,
      .chunk_index = chunk_index,
      .offset = offset,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = yisync::Compression::None,
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

std::uint64_t encoded_message_size(const yisync::Message& message) {
  return yisync::encode_frame(message).size();
}

std::vector<std::uint64_t> chunk_send_order(std::uint64_t chunk_count) {
  std::vector<std::uint64_t> order;
  if (chunk_count == 0) {
    return order;
  }
  order.push_back(chunk_count - 1);
  for (std::uint64_t i = 0; i + 1 < chunk_count; ++i) {
    order.push_back(i);
  }
  return order;
}

std::uint16_t line_port(const NodeOptions& options, yisync::LineId line_id) {
  return static_cast<std::uint16_t>(options.base_port + line_id - 1);
}

std::vector<yisync::LineConfig> make_line_configs(std::uint32_t lines) {
  std::vector<yisync::LineConfig> configs;
  configs.reserve(lines);
  for (std::uint32_t i = 1; i <= lines; ++i) {
    configs.push_back(yisync::LineConfig{
        .id = i,
        .name = "tcp-line-" + std::to_string(i),
        .limiter = yisync::TokenBucketConfig{
            .tokens_per_tick = kLineBudgetBytes,
            .capacity = kLineBudgetBytes,
            .tick = std::chrono::milliseconds(10),
        },
        .initial_recv_window_bytes = kLineWindowBytes,
    });
  }
  return configs;
}

void print_usage() {
  std::cerr
      << "usage:\n"
      << "  yisync_node sink --host 127.0.0.1 --base-port 19000 --lines 2 --root /tmp/yisync_sink\n"
      << "  yisync_node source --host 127.0.0.1 --base-port 19000 --lines 2 --size 153600\n";
}

NodeOptions parse_options(int argc, char** argv) {
  if (argc < 2) {
    print_usage();
    throw std::runtime_error("missing mode");
  }

  NodeOptions options;
  options.mode = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view arg = argv[i];
    const auto need_value = [&](std::string_view name) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + std::string(name));
      }
      return argv[++i];
    };

    if (arg == "--host") {
      options.host = need_value(arg);
    } else if (arg == "--base-port") {
      options.base_port = static_cast<std::uint16_t>(std::stoul(need_value(arg)));
    } else if (arg == "--lines") {
      options.lines = static_cast<std::uint32_t>(std::stoul(need_value(arg)));
    } else if (arg == "--root") {
      options.root = need_value(arg);
    } else if (arg == "--size") {
      options.size = static_cast<std::uint64_t>(std::stoull(need_value(arg)));
    } else {
      throw std::runtime_error("unknown option: " + std::string(arg));
    }
  }

  if (options.mode != "sink" && options.mode != "source") {
    throw std::runtime_error("mode must be sink or source");
  }
  if (options.lines < 2) {
    throw std::runtime_error("at least two lines are required");
  }
  if (!yisync::should_use_chunk_mode(options.size)) {
    throw std::runtime_error("source size must be larger than 64KB for chunk mode");
  }
  return options;
}

class SourceApp {
 public:
  explicit SourceApp(NodeOptions options)
      : options_(std::move(options)),
        scheduler_(make_line_configs(options_.lines)),
        source_(make_source_bytes(options_.size)),
        chunk_count_(yisync::chunk_count_for_size(source_.size())),
        chunk_order_(chunk_send_order(chunk_count_)),
        chunk_acked_(static_cast<std::size_t>(chunk_count_), false) {
    lines_.reserve(options_.lines);
    for (std::uint32_t i = 1; i <= options_.lines; ++i) {
      lines_.push_back(Line{
          .id = i,
          .endpoint = yisync::Endpoint{options_.host, line_port(options_, i)},
      });
    }
  }

  int run() {
    for (const auto& line : lines_) {
      auto pending = yisync::async_connect_tcp(
          loop_,
          line.endpoint,
          kMaxFrameBytes,
          [this, id = line.id](std::shared_ptr<yisync::AsyncFrameConnection> connection) {
            on_connected(id, std::move(connection));
          },
          [this, id = line.id](std::string error) {
            std::cerr << "SOURCE connect failed line=" << id << " error=" << error << "\n";
            failed_ = true;
            loop_.stop();
          });
      connectors_.push_back(std::move(pending));
    }

    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
    loop_.call_later(std::chrono::seconds(30), [this] {
      if (!done_) {
        std::cerr << "SOURCE timeout\n";
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
    bool connected = false;
  };

  Line& find_line(yisync::LineId id) {
    auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
      return line.id == id;
    });
    if (it == lines_.end()) {
      throw std::runtime_error("unknown source line");
    }
    return *it;
  }

  bool all_connected() const {
    return std::all_of(lines_.begin(), lines_.end(), [](const auto& line) {
      return line.connected;
    });
  }

  bool all_chunks_acked() const {
    return std::all_of(chunk_acked_.begin(), chunk_acked_.end(), [](bool acked) {
      return acked;
    });
  }

  void on_connected(yisync::LineId id, std::shared_ptr<yisync::AsyncFrameConnection> connection) {
    auto& line = find_line(id);
    line.connection = std::move(connection);
    line.connected = true;
    line.connection->on_message([this, id](yisync::Message message) {
      on_message(id, std::move(message));
    });
    line.connection->on_error([this, id](std::string error) {
      std::cerr << "SOURCE line error id=" << id << " error=" << error << "\n";
      failed_ = true;
      loop_.stop();
    });
    line.connection->on_close([this, id] {
      if (!done_) {
        std::cerr << "SOURCE line closed before completion id=" << id << "\n";
        failed_ = true;
        loop_.stop();
      }
    });
    line.connection->start(loop_);
    std::cout << "SOURCE connected line=" << id << " endpoint="
              << line.endpoint.host << ":" << line.endpoint.port << "\n";

    if (all_connected()) {
      send_file_begin();
    }
  }

  void send_file_begin() {
    if (begin_sent_) {
      return;
    }
    begin_sent_ = true;
    auto& line = find_line(1);
    const yisync::FileBegin begin{
        .stream_id = kStreamId,
        .order_seq = kOrderSeq,
        .file_id = kFileId,
        .name = yisync::file_name_for_id(kFileId),
        .final_size = static_cast<std::uint64_t>(source_.size()),
        .chunk_size = yisync::kDefaultChunkSizeBytes,
        .chunk_count = chunk_count_,
        .file_checksum = full_crc32c_checksum(source_),
        .prev_file_id = 0,
        .prev_final_size = 0,
    };
    line.connection->send(yisync::Message{begin});
    std::cout << "SOURCE FILE_BEGIN line=1 chunks=" << chunk_count_
              << " size=" << source_.size() << "\n";
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
    if (const auto* heartbeat = std::get_if<yisync::Heartbeat>(&message)) {
      on_heartbeat(line_id, *heartbeat);
      return;
    }
    if (const auto* nack = std::get_if<yisync::Nack>(&message)) {
      std::cerr << "SOURCE NACK line=" << line_id
                << " reason=" << static_cast<int>(nack->reason)
                << " detail=" << nack->detail << "\n";
      failed_ = true;
      loop_.stop();
      return;
    }
    std::cerr << "SOURCE unexpected message line=" << line_id << "\n";
    failed_ = true;
    loop_.stop();
  }

  void on_heartbeat(yisync::LineId line_id, const yisync::Heartbeat& heartbeat) {
    scheduler_.on_heartbeat(line_id, heartbeat);

    if (begin_sent_ && !begin_ready_ && heartbeat.stream_id == kStreamId &&
        heartbeat.file_id == kFileId && heartbeat.received_chunks.empty()) {
      begin_ready_ = true;
      std::cout << "SOURCE FILE_BEGIN ready via heartbeat line=" << line_id << "\n";
    }

    for (const auto& received : heartbeat.received_chunks) {
      if (received.order_seq == kOrderSeq &&
          received.file_id == kFileId &&
          received.chunk_index < chunk_acked_.size()) {
        chunk_acked_[static_cast<std::size_t>(received.chunk_index)] = true;
        std::cout << "SOURCE chunk ack index=" << received.chunk_index
                  << " line=" << line_id << "\n";
      }
    }

    if (commit_sent_ && heartbeat.next_seq > kOrderSeq) {
      done_ = true;
      std::cout << "SOURCE complete file_id=" << kFileId
                << " final_size=" << source_.size() << "\n";
      loop_.stop();
      return;
    }

    schedule_work();
  }

  void schedule_work() {
    if (!begin_ready_ || failed_ || done_) {
      return;
    }

    while (next_chunk_order_index_ < chunk_order_.size()) {
      const auto chunk_index = chunk_order_[next_chunk_order_index_];
      auto chunk = make_chunk(chunk_index, source_);
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
      line.connection->send(yisync::Message{chunk});
      std::cout << "SOURCE send CHUNK index=" << chunk_index
                << " line=" << grant->line_id
                << " wire_bytes=" << wire_bytes << "\n";
      ++next_chunk_order_index_;
    }

    if (all_chunks_acked()) {
      send_commit_if_possible();
    }
  }

  void send_commit_if_possible() {
    if (commit_sent_) {
      return;
    }
    const yisync::FileCommit commit{
        .stream_id = kStreamId,
        .order_seq = kOrderSeq,
        .file_id = kFileId,
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
    line.connection->send(yisync::Message{commit});
    commit_sent_ = true;
    std::cout << "SOURCE send FILE_COMMIT line=" << grant->line_id
              << " wire_bytes=" << wire_bytes << "\n";
  }

  void tick() {
    if (failed_ || done_) {
      return;
    }
    scheduler_.refill_ticks(1);
    schedule_work();
    loop_.call_later(std::chrono::milliseconds(10), [this] { tick(); });
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  yisync::MultiLineScheduler scheduler_;
  yisync::Bytes source_;
  std::uint64_t chunk_count_ = 0;
  std::vector<std::uint64_t> chunk_order_;
  std::vector<bool> chunk_acked_;
  std::vector<Line> lines_;
  std::vector<std::shared_ptr<yisync::PendingTcpConnect>> connectors_;
  std::size_t next_chunk_order_index_ = 0;
  bool begin_sent_ = false;
  bool begin_ready_ = false;
  bool commit_sent_ = false;
  bool done_ = false;
  bool failed_ = false;
};

class SinkApp {
 public:
  explicit SinkApp(NodeOptions options)
      : options_(std::move(options)),
        sink_(kStreamId, options_.root) {
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
      std::cout << "SINK listening line=" << line.id
                << " endpoint=" << endpoint.host << ":" << endpoint.port << "\n";
      line.listener->start(loop_, [this, id = line.id](int fd, yisync::Endpoint peer) {
        on_accept(id, fd, std::move(peer));
      });
    }

    loop_.call_later(std::chrono::seconds(60), [this] {
      if (!committed_) {
        std::cerr << "SINK timeout\n";
        failed_ = true;
        loop_.stop();
      }
    });
    loop_.run();
    return failed_ || !committed_ ? 1 : 0;
  }

 private:
  struct Line {
    yisync::LineId id = 0;
    yisync::Endpoint bind;
    std::unique_ptr<yisync::AsyncTcpListener> listener;
    std::shared_ptr<yisync::AsyncFrameConnection> connection;
  };

  Line& find_line(yisync::LineId id) {
    auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
      return line.id == id;
    });
    if (it == lines_.end()) {
      throw std::runtime_error("unknown sink line");
    }
    return *it;
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
      std::cerr << "SINK line error id=" << id << " error=" << error << "\n";
      failed_ = true;
      loop_.stop();
    });
    connection->on_close([this, id] {
      if (!committed_) {
        std::cerr << "SINK line closed before commit id=" << id << "\n";
      }
    });
    connection->start(loop_);
    line.connection = std::move(connection);
    std::cout << "SINK accepted line=" << id
              << " peer=" << peer.host << ":" << peer.port << "\n";
  }

  void on_message(yisync::LineId line_id, yisync::Message message) {
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
    std::cerr << "SINK unexpected message line=" << line_id << "\n";
    failed_ = true;
    loop_.stop();
  }

  void send_message(yisync::LineId line_id, const yisync::Message& message) {
    auto& line = find_line(line_id);
    if (!line.connection || line.connection->closed()) {
      throw std::runtime_error("sink line has no active connection");
    }
    line.connection->send(message);
  }

  void send_nack(yisync::LineId line_id, const yisync::Nack& nack) {
    send_message(line_id, yisync::Message{nack});
  }

  void apply_begin(yisync::LineId line_id, const yisync::FileBegin& begin) {
    const auto result = sink_.apply(begin);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    send_message(line_id,
                 yisync::Message{yisync::Heartbeat{
                     .stream_id = kStreamId,
                     .next_seq = sink_.expected_order_seq(),
                     .file_id = begin.file_id,
                     .offset = 0,
                     .durable_offset = 0,
                     .recv_window_bytes = kLineWindowBytes,
                 }});
    std::cout << "SINK FILE_BEGIN line=" << line_id
              << " chunks=" << begin.chunk_count
              << " size=" << begin.final_size << "\n";
  }

  void apply_chunk(yisync::LineId line_id, const yisync::Chunk& chunk) {
    const auto result = sink_.apply(chunk);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    send_message(line_id,
                 yisync::Message{yisync::Heartbeat{
                     .stream_id = kStreamId,
                     .next_seq = sink_.expected_order_seq(),
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
                 }});
    std::cout << "SINK CHUNK index=" << chunk.chunk_index
              << " line=" << line_id
              << " offset=" << chunk.offset
              << " len=" << chunk.raw_len << "\n";
  }

  void apply_commit(yisync::LineId line_id, const yisync::FileCommit& commit) {
    const auto result = sink_.apply(commit);
    if (result.has_value()) {
      send_nack(line_id, *result);
      return;
    }
    send_message(line_id,
                 yisync::Message{yisync::Heartbeat{
                     .stream_id = kStreamId,
                     .next_seq = sink_.expected_order_seq(),
                     .file_id = commit.file_id,
                     .offset = options_.size,
                     .durable_offset = options_.size,
                     .recv_window_bytes = kLineWindowBytes,
                 }});
    committed_ = true;
    std::cout << "SINK FILE_COMMIT line=" << line_id
              << " final_path=" << (options_.root / yisync::file_name_for_id(commit.file_id))
              << " expected_order_seq=" << sink_.expected_order_seq() << "\n";
    loop_.call_later(std::chrono::milliseconds(250), [this] {
      loop_.stop();
    });
  }

  NodeOptions options_;
  yisync::EventLoop loop_;
  yisync::ChunkedSinkStream sink_;
  std::vector<Line> lines_;
  bool committed_ = false;
  bool failed_ = false;
};

}  // namespace

int main(int argc, char** argv) {
  std::cout.setf(std::ios::unitbuf);
  std::cerr.setf(std::ios::unitbuf);
  try {
    const auto options = parse_options(argc, argv);
    if (options.mode == "sink") {
      return SinkApp(options).run();
    }
    return SourceApp(options).run();
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }
}
