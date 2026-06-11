#include "core/yisync_protocol.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace yisync {
namespace {

constexpr std::size_t kHeaderLen = T_MessageHeader::kHeaderLen;

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

void write_checksum(Writer& writer, const T_FileChecksum& checksum) {
  writer.u8(to_u8(checksum.algo));
  writer.u64(checksum.offset);
  writer.u64(checksum.len);
  writer.bytes(checksum.value);
}

T_FileChecksum read_checksum(Reader& reader) {
  T_FileChecksum checksum;
  checksum.algo = static_cast<EM_ChecksumAlgo>(reader.u8());
  checksum.offset = reader.u64();
  checksum.len = reader.u64();
  checksum.value = reader.bytes();
  return checksum;
}

void write_manifest_entry(Writer& writer, const T_ManifestEntry& entry) {
  writer.u64(entry.file_id);
  writer.u64(entry.seq);
  writer.u8(to_u8(entry.kind));
  writer.string(entry.name);
  writer.string(entry.link_target);
  writer.u64(entry.size);
  write_checksum(writer, entry.checksum);
}

T_ManifestEntry read_manifest_entry(Reader& reader) {
  T_ManifestEntry entry;
  entry.file_id = reader.u64();
  entry.seq = reader.u64();
  entry.kind = static_cast<EM_EntryKind>(reader.u8());
  entry.name = reader.string();
  entry.link_target = reader.string();
  entry.size = reader.u64();
  entry.checksum = read_checksum(reader);
  return entry;
}

void write_manifest2_stream(Writer& writer, const T_Manifest2Stream& stream) {
  writer.u64(stream.stream_id);
  writer.u8(to_u8(stream.action));
  writer.u64(stream.start_file_id);
  writer.u64(stream.start_offset);
}

T_Manifest2Stream read_manifest2_stream(Reader& reader) {
  T_Manifest2Stream stream;
  stream.stream_id = reader.u64();
  stream.action = static_cast<EM_Manifest2Action>(reader.u8());
  stream.start_file_id = reader.u64();
  stream.start_offset = reader.u64();
  return stream;
}

void write_received_chunk(Writer& writer, const T_ReceivedChunk& chunk) {
  writer.u64(chunk.seq);
  writer.u64(chunk.file_id);
  writer.u64(chunk.chunk_index);
}

T_ReceivedChunk read_received_chunk(Reader& reader) {
  T_ReceivedChunk chunk;
  chunk.seq = reader.u64();
  chunk.file_id = reader.u64();
  chunk.chunk_index = reader.u64();
  return chunk;
}

void write_missing_chunk_range(Writer& writer, const T_MissingChunkRange& range) {
  writer.u64(range.seq);
  writer.u64(range.file_id);
  writer.u64(range.first_chunk_index);
  writer.u64(range.last_chunk_index);
}

T_MissingChunkRange read_missing_chunk_range(Reader& reader) {
  T_MissingChunkRange range;
  range.seq = reader.u64();
  range.file_id = reader.u64();
  range.first_chunk_index = reader.u64();
  range.last_chunk_index = reader.u64();
  return range;
}

void write_header_prefix(Writer& writer, const T_MessageHeader& header) {
  writer.u32(header.magic);
  writer.u8(header.version);
  writer.u8(to_u8(header.msg_type));
  writer.u16(header.header_len);
  writer.u32(header.body_len);
}

T_MessageHeader read_header_prefix(Reader& reader) {
  T_MessageHeader header;
  header.magic = reader.u32();
  header.version = reader.u8();
  header.msg_type = static_cast<EM_MessageType>(reader.u8());
  header.header_len = reader.u16();
  header.body_len = reader.u32();
  return header;
}

Bytes encode_body(const T_Hello& message) {
  Writer writer;
  writer.string(message.node_id);
  writer.u8(to_u8(message.role));
  writer.u16(message.min_version);
  writer.u16(message.max_version);
  writer.u64(message.feature_flags);
  writer.u64(message.required_feature_flags);
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
  return writer.take();
}

Bytes encode_body(const T_Manifest1& message) {
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

Bytes encode_body(const T_Manifest2& message) {
  Writer writer;
  writer.u64(message.manifest_id);
  writer.u32(static_cast<std::uint32_t>(message.streams.size()));
  for (const auto& stream : message.streams) {
    write_manifest2_stream(writer, stream);
  }
  return writer.take();
}

Bytes encode_body(const T_Create& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
  writer.u64(message.file_id);
  writer.u8(to_u8(message.kind));
  writer.string(message.name);
  writer.string(message.link_target);
  writer.u64(message.final_size);
  writer.u64(message.prev_file_id);
  writer.u64(message.prev_final_size);
  write_checksum(writer, message.prev_checksum);
  return writer.take();
}

Bytes encode_body(const T_Data& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
  writer.u64(message.file_id);
  writer.u64(message.offset);
  writer.u64(message.final_size);
  writer.u32(message.raw_len);
  writer.u32(static_cast<std::uint32_t>(message.payload.size()));
  writer.u8(to_u8(message.compression));
  writer.u8(to_u8(message.checksum_algo));
  writer.bytes(message.checksum);
  writer.bytes(message.payload);
  return writer.take();
}

Bytes encode_body(const T_FileBegin& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
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

Bytes encode_body(const T_Chunk& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
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

Bytes encode_body(const T_FileCommit& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.seq);
  writer.u64(message.file_id);
  return writer.take();
}

Bytes encode_body(const T_Heartbeat& message) {
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
  writer.u32(static_cast<std::uint32_t>(message.missing_ranges.size()));
  for (const auto& range : message.missing_ranges) {
    write_missing_chunk_range(writer, range);
  }
  return writer.take();
}

Bytes encode_body(const T_Nack& message) {
  Writer writer;
  writer.u64(message.stream_id);
  writer.u64(message.got_seq);
  writer.u64(message.expected_seq);
  writer.u64(message.file_id);
  writer.u64(message.offset);
  writer.u64(message.expected_file_id);
  writer.u64(message.expected_offset);
  writer.u8(to_u8(message.reason));
  writer.string(message.detail);
  return writer.take();
}

T_Hello decode_hello(Reader& reader) {
  T_Hello message;
  message.node_id = reader.string();
  message.role = static_cast<EM_Role>(reader.u8());
  message.min_version = reader.u16();
  message.max_version = reader.u16();
  message.feature_flags = reader.u64();
  message.required_feature_flags = reader.u64();
  message.chunk_size = reader.u32();
  message.max_inflight_bytes = reader.u64();
  const auto compression_count = reader.u32();
  message.supported_compression.clear();
  for (std::uint32_t i = 0; i < compression_count; ++i) {
    message.supported_compression.push_back(static_cast<EM_Compression>(reader.u8()));
  }
  const auto checksum_count = reader.u32();
  message.supported_checksum.clear();
  for (std::uint32_t i = 0; i < checksum_count; ++i) {
    message.supported_checksum.push_back(static_cast<EM_ChecksumAlgo>(reader.u8()));
  }
  return message;
}

T_Manifest1 decode_manifest1(Reader& reader) {
  T_Manifest1 message;
  message.manifest_id = reader.u64();
  const auto stream_count = reader.u32();
  message.streams.reserve(stream_count);
  for (std::uint32_t stream_i = 0; stream_i < stream_count; ++stream_i) {
    T_Manifest1Stream stream;
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

T_Manifest2 decode_manifest2(Reader& reader) {
  T_Manifest2 message;
  message.manifest_id = reader.u64();
  const auto stream_count = reader.u32();
  message.streams.reserve(stream_count);
  for (std::uint32_t stream_i = 0; stream_i < stream_count; ++stream_i) {
    message.streams.push_back(read_manifest2_stream(reader));
  }
  return message;
}

T_Create decode_create(Reader& reader) {
  T_Create message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  message.kind = static_cast<EM_EntryKind>(reader.u8());
  message.name = reader.string();
  message.link_target = reader.string();
  message.final_size = reader.u64();
  message.prev_file_id = reader.u64();
  message.prev_final_size = reader.u64();
  message.prev_checksum = read_checksum(reader);
  return message;
}

T_Data decode_data(Reader& reader) {
  T_Data message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  message.offset = reader.u64();
  message.final_size = reader.u64();
  message.raw_len = reader.u32();
  const auto payload_len = reader.u32();
  message.compression = static_cast<EM_Compression>(reader.u8());
  message.checksum_algo = static_cast<EM_ChecksumAlgo>(reader.u8());
  message.checksum = reader.bytes();
  message.payload = reader.bytes();
  if (message.payload.size() != payload_len) {
    throw std::runtime_error("DATA payload length mismatch");
  }
  return message;
}

T_FileBegin decode_file_begin(Reader& reader) {
  T_FileBegin message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
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

T_Chunk decode_chunk(Reader& reader) {
  T_Chunk message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  message.chunk_index = reader.u64();
  message.offset = reader.u64();
  message.raw_len = reader.u32();
  const auto payload_len = reader.u32();
  message.compression = static_cast<EM_Compression>(reader.u8());
  message.checksum_algo = static_cast<EM_ChecksumAlgo>(reader.u8());
  message.checksum = reader.bytes();
  message.payload = reader.bytes();
  if (message.payload.size() != payload_len) {
    throw std::runtime_error("CHUNK payload length mismatch");
  }
  return message;
}

T_FileCommit decode_file_commit(Reader& reader) {
  T_FileCommit message;
  message.stream_id = reader.u64();
  message.seq = reader.u64();
  message.file_id = reader.u64();
  return message;
}

T_Heartbeat decode_heartbeat(Reader& reader) {
  T_Heartbeat message;
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
  const auto missing_count = reader.u32();
  message.missing_ranges.reserve(missing_count);
  for (std::uint32_t i = 0; i < missing_count; ++i) {
    message.missing_ranges.push_back(read_missing_chunk_range(reader));
  }
  return message;
}

T_Nack decode_nack(Reader& reader) {
  T_Nack message;
  message.stream_id = reader.u64();
  message.got_seq = reader.u64();
  message.expected_seq = reader.u64();
  message.file_id = reader.u64();
  message.offset = reader.u64();
  message.expected_file_id = reader.u64();
  message.expected_offset = reader.u64();
  message.reason = static_cast<EM_NackReason>(reader.u8());
  message.detail = reader.string();
  return message;
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

T_Frame encode_message(const T_Message& message) {
  T_Frame frame;
  frame.body = std::visit([](const auto& value) { return encode_body(value); }, message);
  frame.header.body_len = static_cast<std::uint32_t>(frame.body.size());
  frame.header.header_len = static_cast<std::uint16_t>(kHeaderLen);
  frame.header.version = T_MessageHeader::kVersion;
  frame.header.magic = T_MessageHeader::kMagic;
  frame.header.msg_type = std::visit(
      [](const auto& value) -> EM_MessageType {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, T_Hello>) return EM_MessageType::HELLO;
        if constexpr (std::is_same_v<T, T_Manifest1>) return EM_MessageType::MANIFEST1;
        if constexpr (std::is_same_v<T, T_Manifest2>) return EM_MessageType::MANIFEST2;
        if constexpr (std::is_same_v<T, T_Create>) return EM_MessageType::CREATE;
        if constexpr (std::is_same_v<T, T_Data>) return EM_MessageType::DATA;
        if constexpr (std::is_same_v<T, T_FileBegin>) return EM_MessageType::FILE_BEGIN;
        if constexpr (std::is_same_v<T, T_Chunk>) return EM_MessageType::CHUNK;
        if constexpr (std::is_same_v<T, T_FileCommit>) return EM_MessageType::FILE_COMMIT;
        if constexpr (std::is_same_v<T, T_Heartbeat>) return EM_MessageType::HEARTBEAT;
        if constexpr (std::is_same_v<T, T_Nack>) return EM_MessageType::NACK;
      },
      message);
  return frame;
}

T_Message decode_message(const T_Frame& frame) {
  if (frame.header.magic != T_MessageHeader::kMagic) {
    throw std::runtime_error("bad frame magic");
  }
  if (frame.header.version != T_MessageHeader::kVersion) {
    throw std::runtime_error("unsupported frame version");
  }
  Reader reader(frame.body);
  T_Message message;
  switch (frame.header.msg_type) {
    case EM_MessageType::HELLO:
      message = decode_hello(reader);
      break;
    case EM_MessageType::MANIFEST1:
      message = decode_manifest1(reader);
      break;
    case EM_MessageType::MANIFEST2:
      message = decode_manifest2(reader);
      break;
    case EM_MessageType::CREATE:
      message = decode_create(reader);
      break;
    case EM_MessageType::DATA:
      message = decode_data(reader);
      break;
    case EM_MessageType::FILE_BEGIN:
      message = decode_file_begin(reader);
      break;
    case EM_MessageType::CHUNK:
      message = decode_chunk(reader);
      break;
    case EM_MessageType::FILE_COMMIT:
      message = decode_file_commit(reader);
      break;
    case EM_MessageType::HEARTBEAT:
      message = decode_heartbeat(reader);
      break;
    case EM_MessageType::NACK:
      message = decode_nack(reader);
      break;
    default:
      throw std::runtime_error("unsupported message type");
  }
  reader.done();
  return message;
}

Bytes encode_frame(const T_Message& message) {
  const auto frame = encode_message(message);
  Writer writer;
  write_header_prefix(writer, frame.header);
  auto bytes = writer.take();
  bytes.insert(bytes.end(), frame.body.begin(), frame.body.end());
  return bytes;
}

T_Frame decode_frame(std::span<const std::byte> bytes) {
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
  T_Frame frame;
  frame.header = header;
  frame.body = Bytes(bytes.begin() + static_cast<std::ptrdiff_t>(header.header_len), bytes.end());
  return frame;
}

}  // namespace yisync
