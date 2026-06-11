#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace yisync {

using Bytes = std::vector<std::byte>;

enum class EM_MessageType : std::uint8_t {
  HELLO = 1,
  MANIFEST1 = 2,
  CREATE = 3,
  DATA = 4,
  HEARTBEAT = 5,
  NACK = 6,
  FILE_BEGIN = 7,
  CHUNK = 8,
  FILE_COMMIT = 9,
  MANIFEST2 = 10,
};

enum class EM_Role : std::uint8_t {
  SENDER = 1,
  RECEIVER = 2,
};

enum class EM_Compression : std::uint8_t {
  NONE = 0,
  LZ4 = 1,
  ZSTD = 2,
};

enum class EM_ChecksumAlgo : std::uint8_t {
  NONE = 0,
  CRC32C = 1,
  MD5 = 2,
};

enum class EM_EntryKind : std::uint8_t {
  REGULAR_FILE = 1,
  DIRECTORY = 2,
  SYMLINK = 3,
};

enum class EM_NackReason : std::uint8_t {
  BAD_SESSION = 1,
  BAD_SEQ = 2,
  BAD_OFFSET = 3,
  BAD_CHECKSUM = 4,
  BAD_FILE_ORDER = 5,
  BAD_CREATE = 6,
  FILE_EXISTS = 7,
  PREV_FILE_INCOMPLETE = 8,
  SIZE_CONFLICT = 9,
  CHECKSUM_MISMATCH = 10,
  UNSUPPORTED_COMPRESSION = 11,
  DECODE_ERROR = 12,
  IO_ERROR = 13,
  BAD_CHUNK = 14,
  BAD_COMMIT = 15,
};

enum class EM_Manifest2Action : std::uint8_t {
  IN_SYNC = 0,
  RESUME_EXISTING = 1,
  CREATE_MISSING = 2,
};

inline constexpr std::uint64_t kDefaultChunkSizeBytes = 64 * 1024;
inline constexpr std::uint16_t kProtocolVersion = 1;

enum class EM_FeatureFlag : std::uint64_t {
  MANIFEST12 = 1ULL << 0,
  DIRECTORY_ENTRY = 1ULL << 1,
  SYMLINK_ENTRY = 1ULL << 2,
  CHUNK_TRANSFER = 1ULL << 3,
  HEARTBEAT_ACK = 1ULL << 4,
  MISSING_RANGES = 1ULL << 5,
  DYNAMIC_RTO = 1ULL << 6,
};

inline constexpr std::uint64_t kRequiredFeatureFlags =
    static_cast<std::uint64_t>(EM_FeatureFlag::MANIFEST12) |
    static_cast<std::uint64_t>(EM_FeatureFlag::CHUNK_TRANSFER) |
    static_cast<std::uint64_t>(EM_FeatureFlag::HEARTBEAT_ACK);

inline constexpr std::uint64_t kSupportedFeatureFlags =
    kRequiredFeatureFlags |
    static_cast<std::uint64_t>(EM_FeatureFlag::DIRECTORY_ENTRY) |
    static_cast<std::uint64_t>(EM_FeatureFlag::SYMLINK_ENTRY) |
    static_cast<std::uint64_t>(EM_FeatureFlag::MISSING_RANGES) |
    static_cast<std::uint64_t>(EM_FeatureFlag::DYNAMIC_RTO);

struct T_FileChecksum {
  EM_ChecksumAlgo algo = EM_ChecksumAlgo::NONE;
  std::uint64_t offset = 0;
  std::uint64_t len = 0;
  Bytes value;
};

struct T_MessageHeader {
  static constexpr std::uint32_t kMagic = 0x59495359;
  static constexpr std::uint8_t kVersion = 1;
  static constexpr std::uint16_t kHeaderLen = 12;

  std::uint32_t magic = kMagic;
  std::uint8_t version = kVersion;
  EM_MessageType msg_type = EM_MessageType::HELLO;
  std::uint16_t header_len = kHeaderLen;
  std::uint32_t body_len = 0;
};

struct T_Hello {
  std::string node_id;
  EM_Role role = EM_Role::SENDER;
  std::uint16_t min_version = kProtocolVersion;
  std::uint16_t max_version = kProtocolVersion;
  std::uint64_t feature_flags = kSupportedFeatureFlags;
  std::uint64_t required_feature_flags = kRequiredFeatureFlags;
  std::uint32_t chunk_size = 256 * 1024;
  std::uint64_t max_inflight_bytes = 4 * 1024 * 1024;
  std::vector<EM_Compression> supported_compression{EM_Compression::NONE};
  std::vector<EM_ChecksumAlgo> supported_checksum{EM_ChecksumAlgo::CRC32C};
};

struct T_ManifestEntry {
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  EM_EntryKind kind = EM_EntryKind::REGULAR_FILE;
  std::string name;
  std::string link_target;
  std::uint64_t size = 0;
  T_FileChecksum checksum;
};

struct T_Manifest1Stream {
  std::uint64_t stream_id = 0;
  std::string root;
  std::vector<T_ManifestEntry> entries;
};

struct T_Manifest1 {
  std::uint64_t manifest_id = 0;
  std::vector<T_Manifest1Stream> streams;
};

struct T_Manifest2Stream {
  std::uint64_t stream_id = 0;
  EM_Manifest2Action action = EM_Manifest2Action::IN_SYNC;
  std::uint64_t start_file_id = 0;
  std::uint64_t start_offset = 0;
};

struct T_Manifest2 {
  std::uint64_t manifest_id = 0;
  std::vector<T_Manifest2Stream> streams;
};

struct T_Create {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  EM_EntryKind kind = EM_EntryKind::REGULAR_FILE;
  std::string name;
  std::string link_target;
  std::uint64_t final_size = 0;
  std::uint64_t prev_file_id = 0;
  std::uint64_t prev_final_size = 0;
  T_FileChecksum prev_checksum;
};

struct T_Data {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t final_size = 0;
  std::uint32_t raw_len = 0;
  EM_Compression compression = EM_Compression::NONE;
  EM_ChecksumAlgo checksum_algo = EM_ChecksumAlgo::CRC32C;
  Bytes checksum;
  Bytes payload;
};

struct T_FileBegin {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::string name;
  std::uint64_t final_size = 0;
  std::uint64_t chunk_size = kDefaultChunkSizeBytes;
  std::uint64_t chunk_count = 0;
  T_FileChecksum file_checksum;
  std::uint64_t prev_file_id = 0;
  std::uint64_t prev_final_size = 0;
  T_FileChecksum prev_checksum;
};

struct T_Chunk {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t chunk_index = 0;
  std::uint64_t offset = 0;
  std::uint32_t raw_len = 0;
  EM_Compression compression = EM_Compression::NONE;
  EM_ChecksumAlgo checksum_algo = EM_ChecksumAlgo::CRC32C;
  Bytes checksum;
  Bytes payload;
};

struct T_FileCommit {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
};

struct T_ReceivedChunk {
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t chunk_index = 0;
};

struct T_MissingChunkRange {
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t first_chunk_index = 0;
  std::uint64_t last_chunk_index = 0;
};

struct T_Heartbeat {
  std::uint64_t stream_id = 0;
  std::uint64_t next_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t durable_offset = 0;
  std::uint64_t recv_window_bytes = 0;
  std::vector<T_ReceivedChunk> received_chunks;
  std::vector<T_MissingChunkRange> missing_ranges;
};

struct T_Nack {
  std::uint64_t stream_id = 0;
  std::uint64_t got_seq = 0;
  std::uint64_t expected_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t expected_file_id = 0;
  std::uint64_t expected_offset = 0;
  EM_NackReason reason = EM_NackReason::DECODE_ERROR;
  std::string detail;
};

using T_Message = std::variant<T_Hello, T_Manifest1, T_Manifest2, T_Create, T_Data, T_FileBegin, T_Chunk, T_FileCommit, T_Heartbeat, T_Nack>;

struct T_Frame {
  T_MessageHeader header;
  Bytes body;
};

std::uint32_t crc32c(std::span<const std::byte> bytes);
Bytes crc32c_bytes(std::span<const std::byte> bytes);

T_Frame encode_message(const T_Message& message);
T_Message decode_message(const T_Frame& frame);
Bytes encode_frame(const T_Message& message);
T_Frame decode_frame(std::span<const std::byte> bytes);

}  // namespace yisync
