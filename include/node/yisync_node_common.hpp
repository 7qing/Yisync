#pragma once

#include "core/yisync_protocol.hpp"
#include "network/yisync_network.hpp"
#include "network/yisync_scheduler.hpp"
#include "sender/yisync_source.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yisync::node {

inline constexpr std::uint64_t kStreamId = 9001;
inline constexpr std::uint64_t kSeq = 1;
inline constexpr std::uint64_t kFileId = 1;
inline constexpr std::uint64_t kLineBudgetBytes = 96 * 1024;
inline constexpr std::uint64_t kLineWindowBytes = 2 * kLineBudgetBytes;
inline constexpr std::uint64_t kMaxFrameBytes = 1024 * 1024;
inline constexpr std::uint64_t kHeartbeatTimeoutTicks = 300;
inline constexpr std::uint64_t kChunkRetransmitTicks = 100;
inline constexpr std::uint64_t kMaxRetransmitRetries = 5;
inline constexpr std::uint64_t kMaxManifestRecoveryAttempts = 3;
inline constexpr std::uint64_t kMaxMissingRangesPerHeartbeat = 64;
inline constexpr std::uint64_t kHeartbeatAckBatchSize = 20;
inline constexpr std::uint64_t kReconnectBaseDelayMs = 100;
inline constexpr std::uint64_t kReconnectMaxDelayMs = 2000;
inline constexpr std::chrono::milliseconds kReceiverHeartbeatInterval{50};
inline constexpr std::chrono::milliseconds kReceiverCommitPollInterval{2};
inline constexpr std::chrono::milliseconds kWatchPollInterval{500};
inline constexpr std::chrono::milliseconds kWatchRescanDebounce{200};
inline constexpr std::size_t kDiskWriterQueueCapacity = 128;

struct StreamRootConfig {
  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::string entry_name_regex;
};

struct NodeOptions {
  std::string mode;
  std::string host = "127.0.0.1";
  std::uint16_t base_port = 19000;
  std::uint32_t lines = 2;
  std::uint64_t size = 150 * 1024;
  std::filesystem::path root = std::filesystem::temp_directory_path() / "yisync_ab_receiver";
  std::filesystem::path source_root;
  LineId drop_line_once = 0;
  std::filesystem::path config_path;
  std::vector<StreamRootConfig> source_streams;
  std::vector<network::LineEndpoint> line_endpoints;
  std::vector<LineConfig> line_configs;
  Compression compression = Compression::None;
  ChecksumAlgo checksum_algo = ChecksumAlgo::Crc32c;
  std::uint64_t recv_window_bytes = kLineWindowBytes;
  std::uint64_t chunk_size = kDefaultChunkSizeBytes;
  std::uint64_t heartbeat_timeout_ticks = kHeartbeatTimeoutTicks;
  std::uint64_t chunk_retransmit_ticks = kChunkRetransmitTicks;
  std::uint64_t max_retransmit_retries = kMaxRetransmitRetries;
  std::uint64_t max_manifest_recovery_attempts = kMaxManifestRecoveryAttempts;
  std::uint64_t max_missing_ranges_per_heartbeat = kMaxMissingRangesPerHeartbeat;
  std::uint64_t heartbeat_ack_batch_size = kHeartbeatAckBatchSize;
  std::uint64_t reconnect_base_delay_ms = kReconnectBaseDelayMs;
  std::uint64_t reconnect_max_delay_ms = kReconnectMaxDelayMs;
  std::chrono::milliseconds receiver_heartbeat_interval = kReceiverHeartbeatInterval;
  std::chrono::milliseconds receiver_commit_poll_interval = kReceiverCommitPollInterval;
  bool watch = false;
  WatchBackend watch_backend = WatchBackend::Auto;
  std::chrono::milliseconds watch_poll_interval = kWatchPollInterval;
  std::chrono::milliseconds watch_rescan_debounce = kWatchRescanDebounce;
};

Bytes make_sender_bytes(std::uint64_t size);
std::uint64_t local_file_size_or_zero(const std::filesystem::path& path);
void fsync_file_for_durable_offset(const std::filesystem::path& path);
FileChecksum full_crc32c_checksum(const Bytes& bytes);

Chunk make_chunk_from_payload(std::uint64_t stream_id,
                              std::uint64_t seq,
                              std::uint64_t file_id,
                              std::uint64_t chunk_index,
                              Bytes payload,
                              std::uint64_t chunk_size = kDefaultChunkSizeBytes);

Data make_data_from_payload(std::uint64_t stream_id,
                            std::uint64_t file_id,
                            std::uint64_t seq,
                            std::uint64_t offset,
                            std::uint64_t final_size,
                            Bytes payload);

std::uint64_t encoded_message_size(const Message& message);
std::vector<std::uint64_t> chunk_send_order(std::uint64_t chunk_count);
std::optional<std::uint64_t> parse_u64_text(std::string_view text);
std::optional<std::uint64_t> parse_stream_dir_name(const std::filesystem::path& path);
std::uint16_t line_port(const NodeOptions& options, LineId line_id);
std::vector<LineConfig> make_line_configs(const NodeOptions& options);
std::vector<network::LineEndpoint> make_line_endpoints(const NodeOptions& options);
NodeOptions parse_options(int argc, char** argv);

}  // namespace yisync::node
