#include "core/yisync_protocol.hpp"
#include "network/yisync_network.hpp"

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

namespace {

std::string message_name(const yisync::Message& message) {
  return std::visit(
      [](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, yisync::Hello>) return "Hello";
        if constexpr (std::is_same_v<T, yisync::Manifest1>) return "Manifest1";
        if constexpr (std::is_same_v<T, yisync::Manifest2>) return "Manifest2";
        if constexpr (std::is_same_v<T, yisync::Create>) return "Create";
        if constexpr (std::is_same_v<T, yisync::Data>) return "Data";
        if constexpr (std::is_same_v<T, yisync::FileBegin>) return "FileBegin";
        if constexpr (std::is_same_v<T, yisync::Chunk>) return "Chunk";
        if constexpr (std::is_same_v<T, yisync::FileCommit>) return "FileCommit";
        if constexpr (std::is_same_v<T, yisync::Heartbeat>) return "Heartbeat";
        if constexpr (std::is_same_v<T, yisync::Nack>) return "Nack";
      },
      message);
}

}  // namespace

TEST(ProtocolDecodeTest, RoundTripsEveryMessageType) {
  const yisync::Bytes payload{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  std::vector<yisync::Message> messages;
  messages.push_back(yisync::network::make_hello(yisync::Role::Sender,
                                                 "sender",
                                                 yisync::kDefaultChunkSizeBytes,
                                                 256 * 1024));
  messages.push_back(yisync::Manifest1{
      .manifest_id = 7,
      .streams = {
          yisync::Manifest1Stream{
              .stream_id = 9001,
              .root = "/tmp/source",
              .entries = {
                  yisync::ManifestEntry{
                      .file_id = 1,
                      .seq = 1,
                      .kind = yisync::EntryKind::RegularFile,
                      .name = "a.txt",
                      .size = 3,
                      .checksum = yisync::FileChecksum{
                          .algo = yisync::ChecksumAlgo::Crc32c,
                          .offset = 0,
                          .len = 3,
                          .value = yisync::crc32c_bytes(payload),
                      },
                  },
              },
          },
      },
  });
  messages.push_back(yisync::Manifest2{
      .manifest_id = 7,
      .streams = {
          yisync::Manifest2Stream{
              .stream_id = 9001,
              .action = yisync::Manifest2Action::CreateMissing,
              .start_file_id = 1,
              .start_offset = 0,
          },
      },
  });
  messages.push_back(yisync::Create{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 1,
      .kind = yisync::EntryKind::RegularFile,
      .name = "a.txt",
      .final_size = 3,
  });
  messages.push_back(yisync::Data{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 1,
      .offset = 0,
      .final_size = 3,
      .raw_len = 3,
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = payload,
  });
  messages.push_back(yisync::FileBegin{
      .stream_id = 9001,
      .seq = 2,
      .file_id = 2,
      .name = "big.bin",
      .final_size = yisync::kDefaultChunkSizeBytes + 1,
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = 2,
  });
  messages.push_back(yisync::Chunk{
      .stream_id = 9001,
      .seq = 2,
      .file_id = 2,
      .chunk_index = 0,
      .offset = 0,
      .raw_len = 3,
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = payload,
  });
  messages.push_back(yisync::FileCommit{.stream_id = 9001, .seq = 2, .file_id = 2});
  messages.push_back(yisync::Heartbeat{
      .stream_id = 9001,
      .next_seq = 3,
      .file_id = 2,
      .recv_window_bytes = 1024,
      .received_chunks = {yisync::ReceivedChunk{.seq = 2, .file_id = 2, .chunk_index = 0}},
      .missing_ranges = {yisync::MissingChunkRange{
          .seq = 2,
          .file_id = 2,
          .first_chunk_index = 1,
          .last_chunk_index = 1,
      }},
  });
  messages.push_back(yisync::Nack{
      .stream_id = 9001,
      .got_seq = 2,
      .expected_seq = 1,
      .file_id = 2,
      .reason = yisync::NackReason::BadSeq,
      .detail = "future seq",
  });

  for (const auto& message : messages) {
    const auto decoded = yisync::decode_message(yisync::decode_frame(yisync::encode_frame(message)));
    EXPECT_EQ(message_name(decoded), message_name(message));
  }
}

TEST(ProtocolDecodeTest, RejectsRandomMalformedFramesWithoutCrashing) {
  std::mt19937_64 rng(12345);
  for (int i = 0; i < 512; ++i) {
    const auto len = static_cast<std::size_t>(rng() % 256);
    yisync::Bytes bytes(len);
    for (auto& value : bytes) {
      value = static_cast<std::byte>(rng() & 0xff);
    }
    try {
      const auto frame = yisync::decode_frame(bytes);
      (void)yisync::decode_message(frame);
    } catch (const std::exception&) {
    }
  }
}

TEST(ProtocolDecodeTest, NegotiatesHelloCapabilities) {
  const auto sender = yisync::network::make_hello(yisync::Role::Sender,
                                                  "sender",
                                                  yisync::kDefaultChunkSizeBytes,
                                                  1024);
  const auto receiver = yisync::network::make_hello(yisync::Role::Receiver,
                                                    "receiver",
                                                    yisync::kDefaultChunkSizeBytes,
                                                    2048);

  const auto result = yisync::network::negotiate_hello(sender, receiver, yisync::Role::Receiver);
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.negotiated_version, yisync::kProtocolVersion);
  EXPECT_EQ(result.negotiated_features & yisync::kRequiredFeatureFlags,
            yisync::kRequiredFeatureFlags);
  EXPECT_EQ(result.recv_window_bytes, 2048U);
}

TEST(ProtocolDecodeTest, RejectsIncompatibleHello) {
  auto sender = yisync::network::make_hello(yisync::Role::Sender,
                                            "sender",
                                            yisync::kDefaultChunkSizeBytes,
                                            1024);
  auto receiver = yisync::network::make_hello(yisync::Role::Receiver,
                                              "receiver",
                                              yisync::kDefaultChunkSizeBytes * 2,
                                              2048);

  const auto result = yisync::network::negotiate_hello(sender, receiver, yisync::Role::Receiver);
  EXPECT_FALSE(result.ok);
  EXPECT_NE(result.error.find("chunk_size"), std::string::npos);
}
