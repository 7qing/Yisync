#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace yisync {

using Bytes = std::vector<std::byte>;

enum class MessageType : std::uint8_t {
  Hello = 1,
  Manifest = 2,
  Create = 3,
  Data = 4,
  Heartbeat = 5,
  Nack = 6,
  FileBegin = 7,
  Chunk = 8,
  FileCommit = 9,
};

enum class Role : std::uint8_t {
  Sender = 1,
  Receiver = 2,
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

enum class EntryKind : std::uint8_t {
  RegularFile = 1,
  Directory = 2,
  Symlink = 3,
};

enum class NackReason : std::uint8_t {
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

inline constexpr std::uint64_t kDefaultChunkSizeBytes = 64 * 1024;

struct FileChecksum {
  ChecksumAlgo algo = ChecksumAlgo::None;
  std::uint64_t offset = 0;
  std::uint64_t len = 0;
  Bytes value;
};

struct MessageHeader {
  static constexpr std::uint32_t kMagic = 0x59495359;
  static constexpr std::uint8_t kVersion = 1;
  static constexpr std::uint16_t kHeaderLen = 12;

  std::uint32_t magic = kMagic;
  std::uint8_t version = kVersion;
  MessageType msg_type = MessageType::Hello;
  std::uint16_t header_len = kHeaderLen;
  std::uint32_t body_len = 0;
};

struct Hello {
  std::string node_id;
  Role role = Role::Sender;
  std::uint32_t chunk_size = 256 * 1024;
  std::uint64_t max_inflight_bytes = 4 * 1024 * 1024;
  std::vector<Compression> supported_compression{Compression::None};
  std::vector<ChecksumAlgo> supported_checksum{ChecksumAlgo::Crc32c};
};

struct ManifestEntry {
  std::uint64_t file_id = 0;
  std::uint64_t order_seq = 0;
  EntryKind kind = EntryKind::RegularFile;
  std::string name;
  std::string link_target;
  std::uint64_t size = 0;
  FileChecksum checksum;
};

struct IncompleteChunkFile {
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
  std::vector<std::uint64_t> received_chunks;
};

struct ManifestStream {
  std::uint64_t stream_id = 0;
  std::string root;
  std::vector<ManifestEntry> entries;
  std::vector<IncompleteChunkFile> incomplete_chunks;
};

struct Manifest {
  std::uint64_t manifest_id = 0;
  std::vector<ManifestStream> streams;
};

struct Create {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  EntryKind kind = EntryKind::RegularFile;
  std::string name;
  std::string link_target;
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

struct MissingChunkRange {
  std::uint64_t order_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t first_chunk_index = 0;
  std::uint64_t last_chunk_index = 0;
};

struct Heartbeat {
  std::uint64_t stream_id = 0;
  std::uint64_t next_seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t durable_offset = 0;
  std::uint64_t recv_window_bytes = 0;
  std::vector<ReceivedChunk> received_chunks;
  std::vector<MissingChunkRange> missing_ranges;
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

std::uint32_t crc32c(std::span<const std::byte> bytes);
Bytes crc32c_bytes(std::span<const std::byte> bytes);

Frame encode_message(const Message& message);
Message decode_message(const Frame& frame);
Bytes encode_frame(const Message& message);
Frame decode_frame(std::span<const std::byte> bytes);

}  // namespace yisync
