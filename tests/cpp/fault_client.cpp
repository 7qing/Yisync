#include "core/yisync_protocol.hpp"
#include "network/yisync_network.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {

constexpr std::uint64_t kStreamId = 9001;
constexpr std::size_t kHeaderLen = yisync::MessageHeader::kHeaderLen;

std::uint64_t parse_u64(std::string_view value, const char* name) {
  std::uint64_t out = 0;
  const auto* first = value.data();
  const auto* last = value.data() + value.size();
  const auto result = std::from_chars(first, last, out);
  if (result.ec != std::errc{} || result.ptr != last) {
    throw std::runtime_error(std::string("invalid ") + name + ": " + std::string(value));
  }
  return out;
}

void send_all(int fd, const yisync::Bytes& bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const auto* data = reinterpret_cast<const char*>(bytes.data()) + sent;
    const auto remaining = bytes.size() - sent;
    const auto n = ::send(fd, data, remaining, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("send failed: ") + std::strerror(errno));
    }
    if (n == 0) {
      throw std::runtime_error("send returned zero");
    }
    sent += static_cast<std::size_t>(n);
  }
}

void recv_exact(int fd, std::vector<std::byte>& out, std::size_t len) {
  out.resize(len);
  std::size_t got = 0;
  while (got < len) {
    auto* data = reinterpret_cast<char*>(out.data()) + got;
    const auto n = ::recv(fd, data, len - got, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("recv failed: ") + std::strerror(errno));
    }
    if (n == 0) {
      throw std::runtime_error("peer closed while reading frame");
    }
    got += static_cast<std::size_t>(n);
  }
}

std::uint32_t read_le32(const std::vector<std::byte>& bytes, std::size_t offset) {
  return static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset])) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 3])) << 24);
}

yisync::Message recv_message(int fd) {
  std::vector<std::byte> header;
  recv_exact(fd, header, kHeaderLen);
  const auto body_len = read_le32(header, 8);
  std::vector<std::byte> body;
  recv_exact(fd, body, body_len);
  yisync::Bytes frame_bytes(header.begin(), header.end());
  frame_bytes.insert(frame_bytes.end(), body.begin(), body.end());
  return yisync::decode_message(yisync::decode_frame(frame_bytes));
}

int connect_tcp(std::string_view host, std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error(std::string("socket failed: ") + std::strerror(errno));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, std::string(host).c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::runtime_error("invalid IPv4 host");
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    const auto error = errno;
    ::close(fd);
    throw std::runtime_error(std::string("connect failed: ") + std::strerror(error));
  }
  return fd;
}

yisync::Bytes bytes(std::string_view text) {
  yisync::Bytes out;
  out.reserve(text.size());
  for (char ch : text) {
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
  }
  return out;
}

yisync::Data make_data(std::uint64_t final_size, yisync::Bytes payload) {
  return yisync::Data{
      .stream_id = kStreamId,
      .seq = 1,
      .file_id = 1,
      .offset = 0,
      .final_size = final_size,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

void expect_nack_reason(int fd, yisync::NackReason expected) {
  for (int i = 0; i < 16; ++i) {
    const auto message = recv_message(fd);
    if (const auto* nack = std::get_if<yisync::Nack>(&message)) {
      if (nack->reason != expected) {
        throw std::runtime_error("unexpected NACK reason");
      }
      return;
    }
  }
  throw std::runtime_error("did not receive NACK");
}

yisync::FileChecksum checksum_for(const yisync::Bytes& payload) {
  return yisync::FileChecksum{
      .algo = yisync::ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(payload.size()),
      .value = yisync::crc32c_bytes(payload),
  };
}

void send_hello_and_manifest(int fd) {
  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::network::make_hello(yisync::Role::Sender,
                                                "fault-client",
                                                yisync::kDefaultChunkSizeBytes,
                                                256 * 1024)}));
  const auto hello = recv_message(fd);
  if (!std::holds_alternative<yisync::Hello>(hello)) {
    throw std::runtime_error("receiver did not return Hello");
  }

  const auto payload = bytes("hello");
  const yisync::Manifest1 manifest{
      .manifest_id = 1,
      .streams = {
          yisync::Manifest1Stream{
              .stream_id = kStreamId,
              .root = "/tmp/yisync_fault_source",
              .entries = {
                  yisync::ManifestEntry{
                      .file_id = 1,
                      .seq = 1,
                      .kind = yisync::EntryKind::RegularFile,
                      .name = "fault.txt",
                      .size = 5,
                      .checksum = checksum_for(payload),
                  },
              },
          },
      },
  };
  send_all(fd, yisync::encode_frame(yisync::Message{manifest}));
  const auto manifest2 = recv_message(fd);
  if (!std::holds_alternative<yisync::Manifest2>(manifest2)) {
    throw std::runtime_error("receiver did not return Manifest2");
  }
}

void run_bad_checksum(int fd) {
  send_hello_and_manifest(fd);
  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::Create{
                       .stream_id = kStreamId,
                       .seq = 1,
                       .file_id = 1,
                       .kind = yisync::EntryKind::RegularFile,
                       .name = "fault.txt",
                       .final_size = 5,
                   }}));
  (void)recv_message(fd);

  auto data = make_data(5, bytes("hello"));
  data.checksum = yisync::Bytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}};
  send_all(fd, yisync::encode_frame(yisync::Message{data}));
  expect_nack_reason(fd, yisync::NackReason::BadChecksum);
}

void run_size_conflict(int fd) {
  send_hello_and_manifest(fd);
  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::Create{
                       .stream_id = kStreamId,
                       .seq = 1,
                       .file_id = 1,
                       .kind = yisync::EntryKind::RegularFile,
                       .name = "fault.txt",
                       .final_size = 5,
                   }}));
  (void)recv_message(fd);

  send_all(fd, yisync::encode_frame(yisync::Message{make_data(6, bytes("hello"))}));
  expect_nack_reason(fd, yisync::NackReason::SizeConflict);
}

void run_bad_commit(int fd) {
  send_hello_and_manifest(fd);
  yisync::Bytes payload(yisync::kDefaultChunkSizeBytes + 1);
  std::fill(payload.begin(), payload.end(), std::byte{'x'});
  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::FileBegin{
                       .stream_id = kStreamId,
                       .seq = 1,
                       .file_id = 1,
                       .name = "fault.txt",
                       .final_size = static_cast<std::uint64_t>(payload.size()),
                       .chunk_size = yisync::kDefaultChunkSizeBytes,
                       .chunk_count = 2,
                       .file_checksum = checksum_for(payload),
                   }}));
  (void)recv_message(fd);

  yisync::Bytes chunk0(payload.begin(),
                       payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes));
  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::Chunk{
                       .stream_id = kStreamId,
                       .seq = 1,
                       .file_id = 1,
                       .chunk_index = 0,
                       .offset = 0,
                       .raw_len = static_cast<std::uint32_t>(chunk0.size()),
                       .checksum_algo = yisync::ChecksumAlgo::Crc32c,
                       .checksum = yisync::crc32c_bytes(chunk0),
                       .payload = chunk0,
                   }}));
  (void)recv_message(fd);

  send_all(fd, yisync::encode_frame(yisync::Message{
                   yisync::FileCommit{.stream_id = kStreamId, .seq = 1, .file_id = 1}}));
  expect_nack_reason(fd, yisync::NackReason::BadCommit);
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 4) {
      throw std::runtime_error("usage: yisync_fault_client <host> <port> <bad-checksum|bad-commit|size-conflict>");
    }
    const auto port = static_cast<std::uint16_t>(parse_u64(argv[2], "port"));
    const std::string scenario = argv[3];
    const int fd = connect_tcp(argv[1], port);
    if (scenario == "bad-checksum") {
      run_bad_checksum(fd);
    } else if (scenario == "bad-commit") {
      run_bad_commit(fd);
    } else if (scenario == "size-conflict") {
      run_size_conflict(fd);
    } else {
      throw std::runtime_error("unknown scenario: " + scenario);
    }
    ::close(fd);
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "fault client failed: " << ex.what() << "\n";
    return 1;
  }
}
