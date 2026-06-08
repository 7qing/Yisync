#pragma once

#include "yisync_protocol.hpp"
#include "yisync_scheduler.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yisync::node {

inline constexpr std::uint64_t kStreamId = 9001;
inline constexpr std::uint64_t kOrderSeq = 1;
inline constexpr std::uint64_t kFileId = 1;
inline constexpr std::uint64_t kLineBudgetBytes = 96 * 1024;
inline constexpr std::uint64_t kLineWindowBytes = 2 * kLineBudgetBytes;
inline constexpr std::uint64_t kMaxFrameBytes = 1024 * 1024;
inline constexpr std::uint64_t kHeartbeatTimeoutTicks = 300;
inline constexpr std::uint64_t kChunkRetransmitTicks = 100;
inline constexpr std::uint64_t kMaxMissingRangesPerHeartbeat = 64;
inline constexpr std::uint64_t kReconnectBaseDelayMs = 100;
inline constexpr std::uint64_t kReconnectMaxDelayMs = 2000;
inline constexpr std::uint64_t kReceiverCheckpointBytes = 4 * 1024 * 1024;
inline constexpr std::chrono::milliseconds kReceiverCheckpointInterval{100};
inline constexpr std::chrono::milliseconds kReceiverHeartbeatInterval{50};
inline constexpr std::chrono::milliseconds kReceiverCommitPollInterval{2};
inline constexpr std::size_t kDiskWriterQueueCapacity = 128;

struct NodeOptions {
  std::string mode;
  std::string host = "127.0.0.1";
  std::uint16_t base_port = 19000;
  std::uint32_t lines = 2;
  std::uint64_t size = 150 * 1024;
  std::filesystem::path root = std::filesystem::temp_directory_path() / "yisync_ab_receiver";
  std::filesystem::path source_root;
  LineId drop_line_once = 0;
  std::uint64_t exit_after_checkpoint_chunks = 0;
};

Bytes make_sender_bytes(std::uint64_t size);
std::uint64_t local_file_size_or_zero(const std::filesystem::path& path);
void fsync_file_for_durable_offset(const std::filesystem::path& path);
FileChecksum full_crc32c_checksum(const Bytes& bytes);

Chunk make_chunk_from_payload(std::uint64_t stream_id,
                              std::uint64_t order_seq,
                              std::uint64_t file_id,
                              std::uint64_t chunk_index,
                              Bytes payload);

Data make_data_from_payload(std::uint64_t stream_id,
                            std::uint64_t file_id,
                            std::uint64_t seq,
                            std::uint64_t offset,
                            Bytes payload);

std::uint64_t encoded_message_size(const Message& message);
std::vector<std::uint64_t> chunk_send_order(std::uint64_t chunk_count);
std::optional<std::uint64_t> parse_u64_text(std::string_view text);
std::optional<std::uint64_t> parse_stream_dir_name(const std::filesystem::path& path);
std::uint16_t line_port(const NodeOptions& options, LineId line_id);
std::vector<LineConfig> make_line_configs(std::uint32_t lines);
NodeOptions parse_options(int argc, char** argv);

}  // namespace yisync::node
