#include "yisync_node_common.hpp"

#include <charconv>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>

namespace yisync::node {

Bytes make_sender_bytes(std::uint64_t size) {
  Bytes bytes;
  bytes.reserve(static_cast<std::size_t>(size));
  for (std::uint64_t i = 0; i < size; ++i) {
    bytes.push_back(static_cast<std::byte>('A' + (i % 26)));
  }
  return bytes;
}

std::uint64_t local_file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  return ec ? 0 : size;
}

void fsync_file_for_durable_offset(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    throw std::runtime_error("failed to open append file for fsync: " + path.string());
  }
  if (::fsync(fd) != 0) {
    const auto error = errno;
    ::close(fd);
    throw std::runtime_error("failed to fsync append file: " + std::string(std::strerror(error)));
  }
  ::close(fd);
}

FileChecksum full_crc32c_checksum(const Bytes& bytes) {
  return FileChecksum{
      .algo = ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(bytes.size()),
      .value = crc32c_bytes(bytes),
  };
}

Chunk make_chunk_from_payload(std::uint64_t stream_id,
                              std::uint64_t order_seq,
                              std::uint64_t file_id,
                              std::uint64_t chunk_index,
                              Bytes payload) {
  const auto offset = chunk_index * kDefaultChunkSizeBytes;
  return Chunk{
      .stream_id = stream_id,
      .order_seq = order_seq,
      .file_id = file_id,
      .chunk_index = chunk_index,
      .offset = offset,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = Compression::None,
      .checksum_algo = ChecksumAlgo::Crc32c,
      .checksum = crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

Data make_data_from_payload(std::uint64_t stream_id,
                            std::uint64_t file_id,
                            std::uint64_t seq,
                            std::uint64_t offset,
                            Bytes payload) {
  return Data{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .offset = offset,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = Compression::None,
      .checksum_algo = ChecksumAlgo::Crc32c,
      .checksum = crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

std::uint64_t encoded_message_size(const Message& message) {
  return encode_frame(message).size();
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

std::optional<std::uint64_t> parse_u64_text(std::string_view text) {
  std::uint64_t value = 0;
  const auto* begin = text.data();
  const auto* end = text.data() + text.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::uint64_t> parse_stream_dir_name(const std::filesystem::path& path) {
  return parse_u64_text(path.filename().string());
}

std::uint16_t line_port(const NodeOptions& options, LineId line_id) {
  return static_cast<std::uint16_t>(options.base_port + line_id - 1);
}

std::vector<LineConfig> make_line_configs(std::uint32_t lines) {
  std::vector<LineConfig> configs;
  configs.reserve(lines);
  for (std::uint32_t i = 1; i <= lines; ++i) {
    configs.push_back(LineConfig{
        .id = i,
        .name = "tcp-line-" + std::to_string(i),
        .limiter = TokenBucketConfig{
            .tokens_per_tick = kLineBudgetBytes,
            .capacity = kLineBudgetBytes,
            .tick = std::chrono::milliseconds(10),
        },
        .initial_recv_window_bytes = kLineWindowBytes,
        .heartbeat_timeout_ticks = kHeartbeatTimeoutTicks,
        .initially_connected = false,
    });
  }
  return configs;
}

namespace {

void print_usage() {
  std::cerr
      << "usage:\n"
      << "  yisync_node receiver --host 127.0.0.1 --base-port 19000 --lines 2 --root /tmp/yisync_receiver\n"
      << "  yisync_node sender --host 127.0.0.1 --base-port 19000 --lines 2 --size 153600\n"
      << "  yisync_node sender --host 127.0.0.1 --base-port 19000 --lines 2 --source-root /tmp/yisync_source\n";
}

}  // namespace

NodeOptions parse_options(int argc, char** argv) {
  if (argc < 2) {
    print_usage();
    throw std::runtime_error("missing mode");
  }

  NodeOptions options;
  options.mode = argv[1];
  if (options.mode == "source") {
    options.mode = "sender";
  } else if (options.mode == "sink") {
    options.mode = "receiver";
  }
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
    } else if (arg == "--source-root") {
      options.source_root = need_value(arg);
    } else if (arg == "--size") {
      options.size = static_cast<std::uint64_t>(std::stoull(need_value(arg)));
    } else if (arg == "--drop-line-once") {
      options.drop_line_once = static_cast<LineId>(std::stoul(need_value(arg)));
    } else if (arg == "--exit-after-checkpoint-chunks") {
      options.exit_after_checkpoint_chunks = static_cast<std::uint64_t>(std::stoull(need_value(arg)));
    } else {
      throw std::runtime_error("unknown option: " + std::string(arg));
    }
  }

  if (options.mode != "receiver" && options.mode != "sender") {
    throw std::runtime_error("mode must be receiver or sender");
  }
  if (options.lines < 2) {
    throw std::runtime_error("at least two lines are required");
  }
  return options;
}

}  // namespace yisync::node
