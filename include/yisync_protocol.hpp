#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace yisync {

using Bytes = std::vector<std::byte>;

enum class MessageType : std::uint16_t {
  Hello = 1,
  Manifest = 2,
  Create = 3,
  Data = 4,
  Heartbeat = 5,
  Nack = 6,
  Ping = 7,
  Goaway = 8,
  FileBegin = 9,
  Chunk = 10,
  FileCommit = 11,
};

enum class Role : std::uint8_t {
  Source = 1,
  Sink = 2,
};

enum class Compression : std::uint8_t {
  None = 0,
  Lz4 = 1,
  Zstd = 2,
};

enum class ChecksumAlgo : std::uint8_t {
  None = 0,
  Crc32c = 1,
  Md5 = 2,
};

enum class ChecksumScope : std::uint8_t {
  Range = 1,
  Full = 2,
};

enum class StartAction : std::uint8_t {
  ResumeExisting = 1,
  CreateMissing = 2,
};

enum class CreateMode : std::uint8_t {
  MustNotExist = 1,
  AllowEmptyExisting = 2,
};

enum class NackReason : std::uint16_t {
  BadSession = 1,
  BadSeq = 2,
  BadOffset = 3,
  BadChecksum = 4,
  BadFileOrder = 5,
  BadCreate = 6,
  FileExists = 7,
  PrevFileIncomplete = 8,
  SizeConflict = 9,
  ChecksumMismatch = 10,
  UnsupportedCompression = 11,
  DecodeError = 12,
  IoError = 13,
  BadChunk = 14,
  BadCommit = 15,
};

inline constexpr std::uint64_t kChunkModeThresholdBytes = 64 * 1024;
inline constexpr std::uint64_t kDefaultChunkSizeBytes = 64 * 1024;

struct FileChecksum {
  ChecksumAlgo algo = ChecksumAlgo::None;
  ChecksumScope scope = ChecksumScope::Range;
  std::uint64_t offset = 0;
  std::uint64_t len = 0;
  Bytes value;
};

struct MessageHeader {
  static constexpr std::uint32_t kMagic = 0x59495359;
  static constexpr std::uint16_t kVersion = 1;

  std::uint32_t magic = kMagic;
  std::uint16_t version = kVersion;
  MessageType msg_type = MessageType::Hello;
  std::uint32_t header_len = 20;
  std::uint32_t body_len = 0;
  std::uint32_t flags = 0;
};

struct Hello {
  std::string node_id;
  Role role = Role::Source;
  std::uint32_t chunk_size = 256 * 1024;
  std::uint64_t max_inflight_bytes = 4 * 1024 * 1024;
  std::vector<Compression> supported_compression{Compression::None};
  std::vector<ChecksumAlgo> supported_checksum{ChecksumAlgo::Crc32c};
  std::uint32_t flags = 0;
};

struct ManifestEntry {
  std::uint64_t file_id = 0;
  std::string name;
  std::uint64_t size = 0;
  FileChecksum checksum;
};

struct ManifestStream {
  std::uint64_t stream_id = 0;
  std::string root;
  std::vector<ManifestEntry> entries;
};

struct Manifest {
  std::uint64_t manifest_id = 0;
  std::vector<ManifestStream> streams;
};

struct Create {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::string name;
  CreateMode create_mode = CreateMode::MustNotExist;
  std::uint64_t prev_file_id = 0;
  std::uint64_t prev_final_size = 0;
  FileChecksum prev_checksum;
};

struct Data {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint32_t raw_len = 0;
  Compression compression = Compression::None;
  ChecksumAlgo checksum_algo = ChecksumAlgo::Crc32c;
  Bytes checksum;
  Bytes payload;
};

struct FileBegin {
  std::uint64_t stream_id = 0;
  std::uint64_t order_seq = 0;
  std::uint64_t file_id = 0;
  std::string name;
  std::uint64_t final_size = 0;
  std::uint64_t chunk_size = kDefaultChunkSizeBytes;
  std::uint64_t chunk_count = 0;
  FileChecksum file_checksum;
  std::uint64_t prev_file_id = 0;
  std::uint64_t prev_final_size = 0;
  FileChecksum prev_checksum;
};

struct Chunk {
  std::uint64_t stream_id = 0;
  std::uint64_t order_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t chunk_index = 0;
  std::uint64_t offset = 0;
  std::uint32_t raw_len = 0;
  Compression compression = Compression::None;
  ChecksumAlgo checksum_algo = ChecksumAlgo::Crc32c;
  Bytes checksum;
  Bytes payload;
};

struct FileCommit {
  std::uint64_t stream_id = 0;
  std::uint64_t order_seq = 0;
  std::uint64_t file_id = 0;
};

struct ReceivedChunk {
  std::uint64_t order_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t chunk_index = 0;
};

struct Heartbeat {
  std::uint64_t stream_id = 0;
  std::uint64_t next_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t durable_offset = 0;
  std::uint64_t recv_window_bytes = 0;
  std::vector<ReceivedChunk> received_chunks;
};

struct Nack {
  std::uint64_t stream_id = 0;
  std::uint64_t got_seq = 0;
  std::uint64_t expected_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t expected_file_id = 0;
  std::uint64_t expected_offset = 0;
  NackReason reason = NackReason::DecodeError;
  std::string detail;
};

using Message = std::variant<Hello, Manifest, Create, Data, FileBegin, Chunk, FileCommit, Heartbeat, Nack>;

struct Frame {
  MessageHeader header;
  Bytes body;
};

struct SinkStreamState {
  bool active = false;
  std::uint64_t expected_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::uint64_t next_create_file_id = 0;
};

struct SyncStart {
  std::uint64_t stream_id = 0;
  std::uint64_t start_file_id = 0;
  std::uint64_t start_offset = 0;
  StartAction start_action = StartAction::ResumeExisting;
};

std::uint32_t crc32c(std::span<const std::byte> bytes);
Bytes crc32c_bytes(std::span<const std::byte> bytes);
bool checksum_matches(const FileChecksum& checksum,
                      const std::filesystem::path& file_path);
FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len);

std::string file_name_for_id(std::uint64_t file_id);
std::optional<std::uint64_t> parse_file_id(std::string_view name);

ManifestStream scan_manifest_stream(std::uint64_t stream_id,
                                    const std::filesystem::path& root,
                                    std::uint64_t checksum_range_len);
std::optional<SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<ManifestEntry>& source,
                                     const std::vector<ManifestEntry>& sink);
bool should_use_chunk_mode(std::uint64_t file_size) noexcept;
std::uint64_t chunk_count_for_size(std::uint64_t file_size,
                                   std::uint64_t chunk_size = kDefaultChunkSizeBytes);

Frame encode_message(const Message& message);
Message decode_message(const Frame& frame);
Bytes encode_frame(const Message& message);
Frame decode_frame(std::span<const std::byte> bytes);

class SinkStream {
 public:
  SinkStream(std::uint64_t stream_id, std::filesystem::path root);

  const SinkStreamState& state() const noexcept;
  Heartbeat heartbeat(std::uint64_t recv_window_bytes, std::uint64_t durable_offset = 0) const;
  std::optional<Nack> apply(const Create& create);
  std::optional<Nack> apply(const Data& data);

 private:
  std::uint64_t stream_id_ = 0;
  std::filesystem::path root_;
  SinkStreamState state_;

  void reset_session_from_disk();
  std::filesystem::path path_for_file(std::uint64_t file_id) const;
  Nack nack(std::uint64_t stream_id,
            std::uint64_t got_seq,
            std::uint64_t file_id,
            std::uint64_t offset,
            NackReason reason,
            std::string detail) const;
};

class ChunkedSinkStream {
 public:
  ChunkedSinkStream(std::uint64_t stream_id, std::filesystem::path root);
  ~ChunkedSinkStream();

  ChunkedSinkStream(const ChunkedSinkStream&) = delete;
  ChunkedSinkStream& operator=(const ChunkedSinkStream&) = delete;
  ChunkedSinkStream(ChunkedSinkStream&&) noexcept;
  ChunkedSinkStream& operator=(ChunkedSinkStream&&) noexcept;

  std::uint64_t expected_order_seq() const noexcept;
  std::optional<Nack> apply(const FileBegin& begin);
  std::optional<Nack> apply(const Chunk& chunk);
  std::optional<Nack> apply(const FileCommit& commit);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yisync
