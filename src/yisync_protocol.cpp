#include "yisync_protocol.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <fstream>
#include <unordered_map>
#include <stdexcept>
#include <system_error>

namespace yisync {
namespace {

constexpr std::size_t kHeaderLen = 20;

class Writer {
 public:
  void u8(std::uint8_t value) { out_.push_back(static_cast<std::byte>(value)); }

  void bool_value(bool value) { u8(value ? 1 : 0); }

  void u16(std::uint16_t value) {
    out_.push_back(static_cast<std::byte>(value & 0xff));
    out_.push_back(static_cast<std::byte>((value >> 8) & 0xff));
  }

  void u32(std::uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
      out_.push_back(static_cast<std::byte>((value >> shift) & 0xff));
    }
  }

  void u64(std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
      out_.push_back(static_cast<std::byte>((value >> shift) & 0xff));
    }
  }

  void bytes(std::span<const std::byte> value) {
    u32(static_cast<std::uint32_t>(value.size()));
    out_.insert(out_.end(), value.begin(), value.end());
  }

  void string(std::string_view value) {
    u32(static_cast<std::uint32_t>(value.size()));
    for (char ch : value) {
      out_.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
    }
  }

  Bytes take() { return std::move(out_); }

 private:
  Bytes out_;
};

class Reader {
 public:
  explicit Reader(std::span<const std::byte> input) : input_(input) {}

  std::uint8_t u8() {
    require(1);
    return static_cast<std::uint8_t>(input_[pos_++]);
  }

  bool bool_value() { return u8() != 0; }

  std::uint16_t u16() {
    require(2);
    std::uint16_t value = 0;
    for (int shift = 0; shift < 16; shift += 8) {
      value |= static_cast<std::uint16_t>(static_cast<std::uint8_t>(input_[pos_++])) << shift;
    }
    return value;
  }

  std::uint32_t u32() {
    require(4);
    std::uint32_t value = 0;
    for (int shift = 0; shift < 32; shift += 8) {
      value |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(input_[pos_++])) << shift;
    }
    return value;
  }

  std::uint64_t u64() {
    require(8);
    std::uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 8) {
      value |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(input_[pos_++])) << shift;
    }
    return value;
  }

  Bytes bytes() {
    const auto len = u32();
    require(len);
    Bytes value(input_.begin() + static_cast<std::ptrdiff_t>(pos_),
                input_.begin() + static_cast<std::ptrdiff_t>(pos_ + len));
    pos_ += len;
    return value;
  }

  std::string string() {
    const auto len = u32();
    require(len);
    std::string value;
    value.reserve(len);
    for (std::uint32_t i = 0; i < len; ++i) {
      value.push_back(static_cast<char>(input_[pos_++]));
    }
    return value;
  }

  void done() const {
    if (pos_ != input_.size()) {
      throw std::runtime_error("trailing bytes in message body");
    }
  }

 private:
  void require(std::size_t len) const {
    if (pos_ + len > input_.size()) {
      throw std::runtime_error("truncated message body");
    }
  }

  std::span<const std::byte> input_;
  std::size_t pos_ = 0;
};

template <typename Enum>
std::uint8_t to_u8(Enum value) {
  return static_cast<std::uint8_t>(value);
}

template <typename Enum>
std::uint16_t to_u16(Enum value) {
  return static_cast<std::uint16_t>(value);
}

void write_checksum(Writer& writer, const FileChecksum& checksum) {
  writer.u8(to_u8(checksum.algo));
  writer.u8(to_u8(checksum.scope));
  writer.u64(checksum.offset);
  writer.u64(checksum.len);
  writer.bytes(checksum.value);
}

FileChecksum read_checksum(Reader& reader) {
  FileChecksum checksum;
  checksum.algo = static_cast<ChecksumAlgo>(reader.u8());
  checksum.scope = static_cast<ChecksumScope>(reader.u8());
  checksum.offset = reader.u64();
  checksum.len = reader.u64();
  checksum.value = reader.bytes();
  return checksum;
}

void write_manifest_entry(Writer& writer, const ManifestEntry& entry) {
  writer.u64(entry.file_id);
  writer.string(entry.name);
  writer.u64(entry.size);
  write_checksum(writer, entry.checksum);
}

ManifestEntry read_manifest_entry(Reader& reader) {
  ManifestEntry entry;
  entry.file_id = reader.u64();
  entry.name = reader.string();
  entry.size = reader.u64();
  entry.checksum = read_checksum(reader);
  return entry;
}

void write_received_chunk(Writer& writer, const ReceivedChunk& chunk) {
  writer.u64(chunk.order_seq);
  writer.u64(chunk.file_id);
  writer.u64(chunk.chunk_index);
}

ReceivedChunk read_received_chunk(Reader& reader) {
  ReceivedChunk chunk;
  chunk.order_seq = reader.u64();
  chunk.file_id = reader.u64();
  chunk.chunk_index = reader.u64();
  return chunk;
}

void write_header_prefix(Writer& writer, const MessageHeader& header) {
  writer.u32(header.magic);
  writer.u16(header.version);
  writer.u16(to_u16(header.msg_type));
  writer.u32(header.header_len);
  writer.u32(header.body_len);
  writer.u32(header.flags);
}

MessageHeader read_header_prefix(Reader& reader) {
  MessageHeader header;
  header.magic = reader.u32();
  header.version = reader.u16();
  header.msg_type = static_cast<MessageType>(reader.u16());
  header.header_len = reader.u32();
  header.body_len = reader.u32();
  header.flags = reader.u32();
  return header;
}

Bytes encode_body(const Hello& message) {
  Writer writer;
  writer.string(message.node_id);
  writer.u8(to_u8(message.role));
  writer.u32(message.chunk_size);
  writer.u64(message.max_inflight_bytes);
  writer.u32(static_cast<std::uint32_t>(message.supported_compression.size()));
  for (const auto compression : message.supported_compression) {
    writer.u8(to_u8(compression));
  }
  writer.u32(static_cast<std::uint32_t>(message.supported_checksum.size()));
  for (const auto checksum : message.supported_checksum) {
    writer.u8(to_u8(checksum));
  }
  writer.u32(message.flags);
  return writer.take();
}

Bytes encode_body(const Manifest& message) {
  Writer writer;
  writer.u64(message.manifest_id);
  writer.u32(static_cast<std::uint32_t>(message.streams.size()));
  for (const auto& stream : message.streams) {
    writer.u64(stream.stream_id);
    writer.string(stream.root);
    writer.u32(static_cast<std::uint32_t>(stream.entries.size()));
    for (const auto& entry : stream.entries) {
      write_manifest_entry(writer, entry);
    }
  }
  return writer.take();
}

Bytes encode_body(const Create& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
  writer.u64(message.file_id);
  writer.string(message.name);
  writer.u8(to_u8(message.create_mode));
  writer.u64(message.prev_file_id);
  writer.u64(message.prev_final_size);
  write_checksum(writer, message.prev_checksum);
  return writer.take();
}

Bytes encode_body(const Data& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
  writer.u64(message.file_id);
  writer.u64(message.offset);
  writer.u32(message.raw_len);
  writer.u32(static_cast<std::uint32_t>(message.payload.size()));
  writer.u8(to_u8(message.compression));
  writer.u8(to_u8(message.checksum_algo));
  writer.bytes(message.checksum);
  writer.bytes(message.payload);
  return writer.take();
}

Bytes encode_body(const FileBegin& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.order_seq);
  writer.u64(message.file_id);
  writer.string(message.name);
  writer.u64(message.final_size);
  writer.u64(message.chunk_size);
  writer.u64(message.chunk_count);
  write_checksum(writer, message.file_checksum);
  writer.u64(message.prev_file_id);
  writer.u64(message.prev_final_size);
  write_checksum(writer, message.prev_checksum);
  return writer.take();
}

Bytes encode_body(const Chunk& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.order_seq);
  writer.u64(message.file_id);
  writer.u64(message.chunk_index);
  writer.u64(message.offset);
  writer.u32(message.raw_len);
  writer.u32(static_cast<std::uint32_t>(message.payload.size()));
  writer.u8(to_u8(message.compression));
  writer.u8(to_u8(message.checksum_algo));
  writer.bytes(message.checksum);
  writer.bytes(message.payload);
  return writer.take();
}

Bytes encode_body(const FileCommit& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.order_seq);
  writer.u64(message.file_id);
  return writer.take();
}

Bytes encode_body(const Heartbeat& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.next_seq);
  writer.u64(message.file_id);
  writer.u64(message.offset);
  writer.u64(message.durable_offset);
  writer.u64(message.recv_window_bytes);
  writer.u32(static_cast<std::uint32_t>(message.received_chunks.size()));
  for (const auto& chunk : message.received_chunks) {
    write_received_chunk(writer, chunk);
  }
  return writer.take();
}

Bytes encode_body(const Nack& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.got_seq);
  writer.u64(message.expected_seq);
  writer.u64(message.file_id);
  writer.u64(message.offset);
  writer.u64(message.expected_file_id);
  writer.u64(message.expected_offset);
  writer.u16(to_u16(message.reason));
  writer.string(message.detail);
  return writer.take();
}

Hello decode_hello(Reader& reader) {
  Hello message;
  message.node_id = reader.string();
  message.role = static_cast<Role>(reader.u8());
  message.chunk_size = reader.u32();
  message.max_inflight_bytes = reader.u64();
  const auto compression_count = reader.u32();
  message.supported_compression.clear();
  for (std::uint32_t i = 0; i < compression_count; ++i) {
    message.supported_compression.push_back(static_cast<Compression>(reader.u8()));
  }
  const auto checksum_count = reader.u32();
  message.supported_checksum.clear();
  for (std::uint32_t i = 0; i < checksum_count; ++i) {
    message.supported_checksum.push_back(static_cast<ChecksumAlgo>(reader.u8()));
  }
  message.flags = reader.u32();
  return message;
}

Manifest decode_manifest(Reader& reader) {
  Manifest message;
  message.manifest_id = reader.u64();
  const auto stream_count = reader.u32();
  message.streams.reserve(stream_count);
  for (std::uint32_t stream_i = 0; stream_i < stream_count; ++stream_i) {
    ManifestStream stream;
    stream.stream_id = reader.u64();
    stream.root = reader.string();
    const auto entry_count = reader.u32();
    stream.entries.reserve(entry_count);
    for (std::uint32_t entry_i = 0; entry_i < entry_count; ++entry_i) {
      stream.entries.push_back(read_manifest_entry(reader));
    }
    message.streams.push_back(std::move(stream));
  }
  return message;
}

Create decode_create(Reader& reader) {
  Create message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  message.name = reader.string();
  message.create_mode = static_cast<CreateMode>(reader.u8());
  message.prev_file_id = reader.u64();
  message.prev_final_size = reader.u64();
  message.prev_checksum = read_checksum(reader);
  return message;
}

Data decode_data(Reader& reader) {
  Data message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  message.offset = reader.u64();
  message.raw_len = reader.u32();
  const auto payload_len = reader.u32();
  message.compression = static_cast<Compression>(reader.u8());
  message.checksum_algo = static_cast<ChecksumAlgo>(reader.u8());
  message.checksum = reader.bytes();
  message.payload = reader.bytes();
  if (message.payload.size() != payload_len) {
    throw std::runtime_error("DATA payload length mismatch");
  }
  return message;
}

FileBegin decode_file_begin(Reader& reader) {
  FileBegin message;
  message.stream_id = reader.u64();
  message.order_seq = reader.u64();
  message.file_id = reader.u64();
  message.name = reader.string();
  message.final_size = reader.u64();
  message.chunk_size = reader.u64();
  message.chunk_count = reader.u64();
  message.file_checksum = read_checksum(reader);
  message.prev_file_id = reader.u64();
  message.prev_final_size = reader.u64();
  message.prev_checksum = read_checksum(reader);
  return message;
}

Chunk decode_chunk(Reader& reader) {
  Chunk message;
  message.stream_id = reader.u64();
  message.order_seq = reader.u64();
  message.file_id = reader.u64();
  message.chunk_index = reader.u64();
  message.offset = reader.u64();
  message.raw_len = reader.u32();
  const auto payload_len = reader.u32();
  message.compression = static_cast<Compression>(reader.u8());
  message.checksum_algo = static_cast<ChecksumAlgo>(reader.u8());
  message.checksum = reader.bytes();
  message.payload = reader.bytes();
  if (message.payload.size() != payload_len) {
    throw std::runtime_error("CHUNK payload length mismatch");
  }
  return message;
}

FileCommit decode_file_commit(Reader& reader) {
  FileCommit message;
  message.stream_id = reader.u64();
  message.order_seq = reader.u64();
  message.file_id = reader.u64();
  return message;
}

Heartbeat decode_heartbeat(Reader& reader) {
  Heartbeat message;
  message.stream_id = reader.u64();
  message.next_seq = reader.u64();
  message.file_id = reader.u64();
  message.offset = reader.u64();
  message.durable_offset = reader.u64();
  message.recv_window_bytes = reader.u64();
  const auto received_count = reader.u32();
  message.received_chunks.reserve(received_count);
  for (std::uint32_t i = 0; i < received_count; ++i) {
    message.received_chunks.push_back(read_received_chunk(reader));
  }
  return message;
}

Nack decode_nack(Reader& reader) {
  Nack message;
  message.stream_id = reader.u64();
  message.got_seq = reader.u64();
  message.expected_seq = reader.u64();
  message.file_id = reader.u64();
  message.offset = reader.u64();
  message.expected_file_id = reader.u64();
  message.expected_offset = reader.u64();
  message.reason = static_cast<NackReason>(reader.u16());
  message.detail = reader.string();
  return message;
}

std::uint64_t file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return 0;
  }
  return size;
}

Bytes read_file_range(const std::filesystem::path& path, std::uint64_t offset, std::uint64_t len) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("failed to open file for checksum: " + path.string());
  }
  input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  Bytes bytes(static_cast<std::size_t>(len));
  if (len > 0) {
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (static_cast<std::uint64_t>(input.gcount()) != len) {
      throw std::runtime_error("failed to read requested checksum range: " + path.string());
    }
  }
  return bytes;
}

Bytes raw_data_for_data_message(const Data& data) {
  if (data.compression != Compression::None) {
    throw std::runtime_error("compression is not implemented in this prototype");
  }
  if (data.payload.size() != data.raw_len) {
    throw std::runtime_error("raw_len does not match uncompressed payload length");
  }
  return data.payload;
}

Bytes raw_data_for_chunk_message(const Chunk& chunk) {
  if (chunk.compression != Compression::None) {
    throw std::runtime_error("compression is not implemented in this prototype");
  }
  if (chunk.payload.size() != chunk.raw_len) {
    throw std::runtime_error("raw_len does not match uncompressed chunk payload length");
  }
  return chunk.payload;
}

bool bytes_equal(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

}  // namespace

std::uint32_t crc32c(std::span<const std::byte> bytes) {
  return crc32c::Crc32c(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

Bytes crc32c_bytes(std::span<const std::byte> bytes) {
  const auto value = crc32c(bytes);
  return Bytes{
      static_cast<std::byte>(value & 0xff),
      static_cast<std::byte>((value >> 8) & 0xff),
      static_cast<std::byte>((value >> 16) & 0xff),
      static_cast<std::byte>((value >> 24) & 0xff),
  };
}

bool checksum_matches(const FileChecksum& checksum, const std::filesystem::path& file_path) {
  if (checksum.algo == ChecksumAlgo::None) {
    return checksum.len == 0;
  }
  if (checksum.algo != ChecksumAlgo::Crc32c) {
    throw std::runtime_error("only CRC32C file checksums are implemented in this prototype");
  }
  const auto size = file_size_or_zero(file_path);
  if (checksum.offset > size || checksum.len > size || checksum.offset + checksum.len > size) {
    return false;
  }
  const auto bytes = read_file_range(file_path, checksum.offset, checksum.len);
  return bytes_equal(crc32c_bytes(bytes), checksum.value);
}

FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len) {
  const auto size = file_size_or_zero(file_path);
  FileChecksum checksum;
  checksum.algo = size == 0 ? ChecksumAlgo::None : ChecksumAlgo::Crc32c;
  checksum.scope = ChecksumScope::Range;
  checksum.len = std::min(size, max_len);
  checksum.offset = size - checksum.len;
  if (checksum.len > 0) {
    checksum.value = crc32c_bytes(read_file_range(file_path, checksum.offset, checksum.len));
  }
  return checksum;
}

std::string file_name_for_id(std::uint64_t file_id) {
  return std::to_string(file_id) + ".file";
}

std::optional<std::uint64_t> parse_file_id(std::string_view name) {
  constexpr std::string_view suffix = ".file";
  if (name.size() <= suffix.size() || name.substr(name.size() - suffix.size()) != suffix) {
    return std::nullopt;
  }
  const auto number = name.substr(0, name.size() - suffix.size());
  std::uint64_t file_id = 0;
  const auto* begin = number.data();
  const auto* end = number.data() + number.size();
  const auto result = std::from_chars(begin, end, file_id);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return file_id;
}

ManifestStream scan_manifest_stream(std::uint64_t stream_id,
                                    const std::filesystem::path& root,
                                    std::uint64_t checksum_range_len) {
  ManifestStream stream;
  stream.stream_id = stream_id;
  stream.root = root.string();

  std::error_code ec;
  if (!std::filesystem::exists(root, ec)) {
    return stream;
  }

  for (const auto& entry : std::filesystem::directory_iterator(root)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const auto file_id = parse_file_id(entry.path().filename().string());
    if (!file_id.has_value()) {
      continue;
    }
    ManifestEntry manifest_entry;
    manifest_entry.file_id = *file_id;
    manifest_entry.name = entry.path().filename().string();
    manifest_entry.size = entry.file_size();
    manifest_entry.checksum = make_crc32c_range_checksum(entry.path(), checksum_range_len);
    stream.entries.push_back(std::move(manifest_entry));
  }

  std::sort(stream.entries.begin(), stream.entries.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.file_id < rhs.file_id;
  });
  return stream;
}

std::optional<SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<ManifestEntry>& source,
                                     const std::vector<ManifestEntry>& sink) {
  std::size_t source_i = 0;
  std::size_t sink_i = 0;

  while (source_i < source.size()) {
    const auto& source_entry = source[source_i];
    if (sink_i >= sink.size()) {
      return SyncStart{stream_id, source_entry.file_id, 0, StartAction::CreateMissing};
    }

    const auto& sink_entry = sink[sink_i];
    if (sink_entry.file_id < source_entry.file_id) {
      ++sink_i;
      continue;
    }
    if (sink_entry.file_id > source_entry.file_id) {
      return SyncStart{stream_id, source_entry.file_id, 0, StartAction::CreateMissing};
    }

    if (sink_entry.size > source_entry.size) {
      throw std::runtime_error("sink file is larger than source for " + source_entry.name);
    }
    if (sink_entry.size < source_entry.size) {
      return SyncStart{stream_id, source_entry.file_id, sink_entry.size, StartAction::ResumeExisting};
    }
    if (!bytes_equal(source_entry.checksum.value, sink_entry.checksum.value) ||
        source_entry.checksum.algo != sink_entry.checksum.algo ||
        source_entry.checksum.offset != sink_entry.checksum.offset ||
        source_entry.checksum.len != sink_entry.checksum.len) {
      throw std::runtime_error("checksum mismatch for " + source_entry.name);
    }

    ++source_i;
    ++sink_i;
  }
  return std::nullopt;
}

bool should_use_chunk_mode(std::uint64_t file_size) noexcept {
  return file_size > kChunkModeThresholdBytes;
}

std::uint64_t chunk_count_for_size(std::uint64_t file_size, std::uint64_t chunk_size) {
  if (chunk_size == 0) {
    throw std::invalid_argument("chunk_size must be non-zero");
  }
  return file_size == 0 ? 0 : ((file_size + chunk_size - 1) / chunk_size);
}

Frame encode_message(const Message& message) {
  Frame frame;
  frame.body = std::visit([](const auto& value) { return encode_body(value); }, message);
  frame.header.body_len = static_cast<std::uint32_t>(frame.body.size());
  frame.header.header_len = static_cast<std::uint32_t>(kHeaderLen);
  frame.header.version = MessageHeader::kVersion;
  frame.header.magic = MessageHeader::kMagic;
  frame.header.msg_type = std::visit(
      [](const auto& value) -> MessageType {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, Hello>) return MessageType::Hello;
        if constexpr (std::is_same_v<T, Manifest>) return MessageType::Manifest;
        if constexpr (std::is_same_v<T, Create>) return MessageType::Create;
        if constexpr (std::is_same_v<T, Data>) return MessageType::Data;
        if constexpr (std::is_same_v<T, FileBegin>) return MessageType::FileBegin;
        if constexpr (std::is_same_v<T, Chunk>) return MessageType::Chunk;
        if constexpr (std::is_same_v<T, FileCommit>) return MessageType::FileCommit;
        if constexpr (std::is_same_v<T, Heartbeat>) return MessageType::Heartbeat;
        if constexpr (std::is_same_v<T, Nack>) return MessageType::Nack;
      },
      message);
  return frame;
}

Message decode_message(const Frame& frame) {
  if (frame.header.magic != MessageHeader::kMagic) {
    throw std::runtime_error("bad frame magic");
  }
  if (frame.header.version != MessageHeader::kVersion) {
    throw std::runtime_error("unsupported frame version");
  }
  Reader reader(frame.body);
  Message message;
  switch (frame.header.msg_type) {
    case MessageType::Hello:
      message = decode_hello(reader);
      break;
    case MessageType::Manifest:
      message = decode_manifest(reader);
      break;
    case MessageType::Create:
      message = decode_create(reader);
      break;
    case MessageType::Data:
      message = decode_data(reader);
      break;
    case MessageType::FileBegin:
      message = decode_file_begin(reader);
      break;
    case MessageType::Chunk:
      message = decode_chunk(reader);
      break;
    case MessageType::FileCommit:
      message = decode_file_commit(reader);
      break;
    case MessageType::Heartbeat:
      message = decode_heartbeat(reader);
      break;
    case MessageType::Nack:
      message = decode_nack(reader);
      break;
    default:
      throw std::runtime_error("unsupported message type");
  }
  reader.done();
  return message;
}

Bytes encode_frame(const Message& message) {
  const auto frame = encode_message(message);
  Writer writer;
  write_header_prefix(writer, frame.header);
  auto bytes = writer.take();
  bytes.insert(bytes.end(), frame.body.begin(), frame.body.end());
  return bytes;
}

Frame decode_frame(std::span<const std::byte> bytes) {
  if (bytes.size() < kHeaderLen) {
    throw std::runtime_error("truncated frame header");
  }
  Reader header_reader(bytes.subspan(0, kHeaderLen));
  auto header = read_header_prefix(header_reader);
  header_reader.done();
  if (header.header_len != kHeaderLen) {
    throw std::runtime_error("unsupported header length");
  }
  if (bytes.size() != header.header_len + header.body_len) {
    throw std::runtime_error("frame length mismatch");
  }
  Frame frame;
  frame.header = header;
  frame.body = Bytes(bytes.begin() + static_cast<std::ptrdiff_t>(header.header_len), bytes.end());
  return frame;
}

SinkStream::SinkStream(std::uint64_t stream_id, std::filesystem::path root)
    : stream_id_(stream_id), root_(std::move(root)) {
  std::filesystem::create_directories(root_);
  reset_session_from_disk();
}

const SinkStreamState& SinkStream::state() const noexcept {
  return state_;
}

Heartbeat SinkStream::heartbeat(std::uint64_t recv_window_bytes, std::uint64_t durable_offset) const {
  return Heartbeat{stream_id_, state_.expected_seq, state_.current_file_id, state_.current_offset, durable_offset, recv_window_bytes};
}

std::optional<Nack> SinkStream::apply(const Create& create) {
  if (!state_.active || create.stream_id != stream_id_) {
    return nack(create.stream_id, create.seq, create.file_id, 0, NackReason::BadSession, "inactive or wrong stream");
  }
  if (create.seq < state_.expected_seq) {
    return std::nullopt;
  }
  if (create.seq > state_.expected_seq) {
    return nack(create.stream_id, create.seq, create.file_id, 0, NackReason::BadSeq, "future seq");
  }
  if (create.file_id != state_.next_create_file_id) {
    return nack(create.stream_id,
                create.seq,
                create.file_id,
                0,
                NackReason::BadFileOrder,
                "unexpected create file id");
  }

  if (create.prev_file_id != 0) {
    const auto prev_path = path_for_file(create.prev_file_id);
    if (!std::filesystem::exists(prev_path)) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::PrevFileIncomplete,
                  "previous file does not exist");
    }
    const auto prev_size = file_size_or_zero(prev_path);
    if (prev_size != create.prev_final_size) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::PrevFileIncomplete,
                  "previous file size mismatch");
    }
    if (!checksum_matches(create.prev_checksum, prev_path)) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::ChecksumMismatch,
                  "previous file checksum mismatch");
    }
  }

  const auto path = path_for_file(create.file_id);
  if (std::filesystem::exists(path)) {
    const auto existing_size = file_size_or_zero(path);
    if (create.create_mode != CreateMode::AllowEmptyExisting || existing_size != 0) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  existing_size,
                  NackReason::FileExists,
                  "target file already exists");
    }
  } else {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::IoError,
                  "failed to create target file");
    }
  }

  state_.expected_seq += 1;
  state_.current_file_id = create.file_id;
  state_.current_offset = 0;
  state_.next_create_file_id = create.file_id + 1;
  return std::nullopt;
}

std::optional<Nack> SinkStream::apply(const Data& data) {
  if (!state_.active || data.stream_id != stream_id_) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadSession, "inactive or wrong stream");
  }
  if (data.seq < state_.expected_seq) {
    return std::nullopt;
  }
  if (data.seq > state_.expected_seq) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadSeq, "future seq");
  }
  if (data.file_id != state_.current_file_id) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadFileOrder, "wrong file id");
  }

  const auto path = path_for_file(data.file_id);
  if (!std::filesystem::exists(path)) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadOffset, "target file missing");
  }
  const auto size = file_size_or_zero(path);
  if (data.offset != size) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadOffset, "offset mismatch");
  }
  if (data.raw_len == 0) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::DecodeError, "empty DATA");
  }

  Bytes raw;
  try {
    raw = raw_data_for_data_message(data);
  } catch (const std::exception& ex) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::DecodeError, ex.what());
  }

  if (data.checksum_algo != ChecksumAlgo::Crc32c) {
    return nack(data.stream_id,
                data.seq,
                data.file_id,
                data.offset,
                NackReason::BadChecksum,
                "only CRC32C DATA checksum is implemented");
  }
  if (!bytes_equal(crc32c_bytes(raw), data.checksum)) {
    return nack(data.stream_id,
                data.seq,
                data.file_id,
                data.offset,
                NackReason::BadChecksum,
                "DATA checksum mismatch");
  }

  std::ofstream output(path, std::ios::binary | std::ios::app);
  if (!output) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::IoError, "failed to open target file");
  }
  output.write(reinterpret_cast<const char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
  if (!output) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::IoError, "failed to append target file");
  }

  state_.expected_seq += 1;
  state_.current_offset = data.offset + data.raw_len;
  return std::nullopt;
}

void SinkStream::reset_session_from_disk() {
  state_ = {};
  state_.active = true;
  state_.expected_seq = 1;

  const auto manifest = scan_manifest_stream(stream_id_, root_, 4 * 1024 * 1024);
  if (manifest.entries.empty()) {
    state_.current_file_id = 0;
    state_.current_offset = 0;
    state_.next_create_file_id = 1;
    return;
  }

  const auto& latest = manifest.entries.back();
  state_.current_file_id = latest.file_id;
  state_.current_offset = latest.size;
  state_.next_create_file_id = latest.file_id + 1;
}

std::filesystem::path SinkStream::path_for_file(std::uint64_t file_id) const {
  return root_ / file_name_for_id(file_id);
}

Nack SinkStream::nack(std::uint64_t stream_id,
                      std::uint64_t got_seq,
                      std::uint64_t file_id,
                      std::uint64_t offset,
                      NackReason reason,
                      std::string detail) const {
  return Nack{stream_id,
              got_seq,
              state_.expected_seq,
              file_id,
              offset,
              state_.current_file_id,
              state_.current_offset,
              reason,
              std::move(detail)};
}

struct ChunkedSinkStream::Impl {
  struct ActiveFile {
    std::uint64_t order_seq = 0;
    std::uint64_t file_id = 0;
    std::string name;
    std::uint64_t final_size = 0;
    std::uint64_t chunk_size = kDefaultChunkSizeBytes;
    std::uint64_t chunk_count = 0;
    FileChecksum file_checksum;
    std::filesystem::path temp_path;
    std::filesystem::path final_path;
    std::vector<bool> received;
    std::uint64_t received_count = 0;
  };

  Impl(std::uint64_t stream_id_value, std::filesystem::path root_value)
      : stream_id(stream_id_value), root(std::move(root_value)), temp_root(root / ".yisync_tmp") {
    std::filesystem::create_directories(root);
    std::filesystem::create_directories(temp_root);
  }

  std::filesystem::path final_path_for(const FileBegin& begin) const {
    const auto filename = begin.name.empty()
                              ? std::filesystem::path(file_name_for_id(begin.file_id))
                              : std::filesystem::path(begin.name).filename();
    return root / filename;
  }

  std::filesystem::path temp_path_for(const FileBegin& begin) const {
    return temp_root / (std::to_string(begin.order_seq) + "_" + file_name_for_id(begin.file_id) + ".tmp");
  }

  Nack nack(const FileBegin& begin, NackReason reason, std::string detail) const {
    return Nack{begin.stream_id,
                begin.order_seq,
                expected_order_seq,
                begin.file_id,
                0,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const Chunk& chunk, NackReason reason, std::string detail) const {
    return Nack{chunk.stream_id,
                chunk.order_seq,
                expected_order_seq,
                chunk.file_id,
                chunk.offset,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const FileCommit& commit, NackReason reason, std::string detail) const {
    return Nack{commit.stream_id,
                commit.order_seq,
                expected_order_seq,
                commit.file_id,
                0,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::filesystem::path temp_root;
  std::uint64_t expected_order_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::unordered_map<std::uint64_t, ActiveFile> active;
};

ChunkedSinkStream::ChunkedSinkStream(std::uint64_t stream_id, std::filesystem::path root)
    : impl_(std::make_unique<Impl>(stream_id, std::move(root))) {}

ChunkedSinkStream::~ChunkedSinkStream() = default;

ChunkedSinkStream::ChunkedSinkStream(ChunkedSinkStream&&) noexcept = default;

ChunkedSinkStream& ChunkedSinkStream::operator=(ChunkedSinkStream&&) noexcept = default;

std::uint64_t ChunkedSinkStream::expected_order_seq() const noexcept {
  return impl_->expected_order_seq;
}

std::optional<Nack> ChunkedSinkStream::apply(const FileBegin& begin) {
  if (begin.stream_id != impl_->stream_id) {
    return impl_->nack(begin, NackReason::BadSession, "wrong stream");
  }
  if (begin.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (begin.order_seq > impl_->expected_order_seq) {
    return impl_->nack(begin, NackReason::BadSeq, "future order_seq");
  }
  if (!should_use_chunk_mode(begin.final_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "file size does not require chunk mode");
  }
  if (begin.chunk_size != kDefaultChunkSizeBytes) {
    return impl_->nack(begin, NackReason::BadChunk, "unsupported chunk_size");
  }
  if (begin.chunk_count != chunk_count_for_size(begin.final_size, begin.chunk_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "chunk_count does not match final_size");
  }
  if (impl_->active.contains(begin.order_seq)) {
    return std::nullopt;
  }

  if (begin.prev_file_id != 0) {
    const auto prev_path = impl_->root / file_name_for_id(begin.prev_file_id);
    if (!std::filesystem::exists(prev_path)) {
      return impl_->nack(begin, NackReason::PrevFileIncomplete, "previous file does not exist");
    }
    const auto prev_size = file_size_or_zero(prev_path);
    if (prev_size != begin.prev_final_size) {
      return impl_->nack(begin, NackReason::PrevFileIncomplete, "previous file size mismatch");
    }
    if (!checksum_matches(begin.prev_checksum, prev_path)) {
      return impl_->nack(begin, NackReason::ChecksumMismatch, "previous file checksum mismatch");
    }
  }

  auto final_path = impl_->final_path_for(begin);
  if (std::filesystem::exists(final_path)) {
    return impl_->nack(begin, NackReason::FileExists, "final file already exists");
  }

  auto temp_path = impl_->temp_path_for(begin);
  {
    std::ofstream temp(temp_path, std::ios::binary | std::ios::trunc);
    if (!temp) {
      return impl_->nack(begin, NackReason::IoError, "failed to create chunk temp file");
    }
  }

  Impl::ActiveFile active;
  active.order_seq = begin.order_seq;
  active.file_id = begin.file_id;
  active.name = begin.name;
  active.final_size = begin.final_size;
  active.chunk_size = begin.chunk_size;
  active.chunk_count = begin.chunk_count;
  active.file_checksum = begin.file_checksum;
  active.temp_path = std::move(temp_path);
  active.final_path = std::move(final_path);
  active.received.assign(static_cast<std::size_t>(begin.chunk_count), false);
  impl_->active.emplace(begin.order_seq, std::move(active));
  return std::nullopt;
}

std::optional<Nack> ChunkedSinkStream::apply(const Chunk& chunk) {
  if (chunk.stream_id != impl_->stream_id) {
    return impl_->nack(chunk, NackReason::BadSession, "wrong stream");
  }
  if (chunk.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (chunk.order_seq > impl_->expected_order_seq) {
    return impl_->nack(chunk, NackReason::BadSeq, "future order_seq");
  }

  auto it = impl_->active.find(chunk.order_seq);
  if (it == impl_->active.end()) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk received before FILE_BEGIN");
  }

  auto& active = it->second;
  if (chunk.file_id != active.file_id) {
    return impl_->nack(chunk, NackReason::BadFileOrder, "chunk file_id mismatch");
  }
  if (chunk.chunk_index >= active.chunk_count) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk_index out of range");
  }

  const auto expected_offset = chunk.chunk_index * active.chunk_size;
  if (chunk.offset != expected_offset) {
    return impl_->nack(chunk, NackReason::BadOffset, "chunk offset mismatch");
  }
  const auto expected_len = static_cast<std::uint32_t>(
      std::min<std::uint64_t>(active.chunk_size, active.final_size - chunk.offset));
  if (chunk.raw_len != expected_len || chunk.offset + chunk.raw_len > active.final_size) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk raw_len mismatch");
  }
  if (active.received[static_cast<std::size_t>(chunk.chunk_index)]) {
    return std::nullopt;
  }

  Bytes raw;
  try {
    raw = raw_data_for_chunk_message(chunk);
  } catch (const std::exception& ex) {
    return impl_->nack(chunk, NackReason::DecodeError, ex.what());
  }
  if (chunk.checksum_algo != ChecksumAlgo::Crc32c) {
    return impl_->nack(chunk, NackReason::BadChecksum, "only CRC32C CHUNK checksum is implemented");
  }
  if (!bytes_equal(crc32c_bytes(raw), chunk.checksum)) {
    return impl_->nack(chunk, NackReason::BadChecksum, "CHUNK checksum mismatch");
  }

  std::fstream temp(active.temp_path, std::ios::binary | std::ios::in | std::ios::out);
  if (!temp) {
    return impl_->nack(chunk, NackReason::IoError, "failed to open chunk temp file");
  }
  temp.seekp(static_cast<std::streamoff>(chunk.offset), std::ios::beg);
  temp.write(reinterpret_cast<const char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
  if (!temp) {
    return impl_->nack(chunk, NackReason::IoError, "failed to write chunk temp file");
  }

  active.received[static_cast<std::size_t>(chunk.chunk_index)] = true;
  active.received_count += 1;
  return std::nullopt;
}

std::optional<Nack> ChunkedSinkStream::apply(const FileCommit& commit) {
  if (commit.stream_id != impl_->stream_id) {
    return impl_->nack(commit, NackReason::BadSession, "wrong stream");
  }
  if (commit.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (commit.order_seq > impl_->expected_order_seq) {
    return impl_->nack(commit, NackReason::BadSeq, "future order_seq");
  }

  auto it = impl_->active.find(commit.order_seq);
  if (it == impl_->active.end()) {
    return impl_->nack(commit, NackReason::BadCommit, "commit received before FILE_BEGIN");
  }
  auto& active = it->second;
  if (commit.file_id != active.file_id) {
    return impl_->nack(commit, NackReason::BadFileOrder, "commit file_id mismatch");
  }
  if (active.received_count != active.chunk_count) {
    return impl_->nack(commit, NackReason::BadCommit, "not all chunks have been received");
  }
  if (!checksum_matches(active.file_checksum, active.temp_path)) {
    return impl_->nack(commit, NackReason::ChecksumMismatch, "final file checksum mismatch");
  }
  if (std::filesystem::exists(active.final_path)) {
    return impl_->nack(commit, NackReason::FileExists, "final file already exists");
  }

  std::error_code ec;
  std::filesystem::rename(active.temp_path, active.final_path, ec);
  if (ec) {
    return impl_->nack(commit, NackReason::IoError, "failed to commit chunk temp file: " + ec.message());
  }

  impl_->current_file_id = active.file_id;
  impl_->current_offset = active.final_size;
  impl_->expected_order_seq += 1;
  impl_->active.erase(it);
  return std::nullopt;
}

}  // namespace yisync
