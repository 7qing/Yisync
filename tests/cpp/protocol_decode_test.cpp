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

std::string message_name(const yisync::T_Message& message) {
  return std::visit(
      [](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, yisync::T_Hello>) return "T_Hello";
        if constexpr (std::is_same_v<T, yisync::T_Manifest1>) return "T_Manifest1";
        if constexpr (std::is_same_v<T, yisync::T_Manifest2>) return "T_Manifest2";
        if constexpr (std::is_same_v<T, yisync::T_Create>) return "T_Create";
        if constexpr (std::is_same_v<T, yisync::T_Data>) return "T_Data";
        if constexpr (std::is_same_v<T, yisync::T_FileBegin>) return "T_FileBegin";
        if constexpr (std::is_same_v<T, yisync::T_Chunk>) return "T_Chunk";
        if constexpr (std::is_same_v<T, yisync::T_FileCommit>) return "T_FileCommit";
        if constexpr (std::is_same_v<T, yisync::T_Heartbeat>) return "T_Heartbeat";
        if constexpr (std::is_same_v<T, yisync::T_Nack>) return "T_Nack";
      },
      message);
}

}  // namespace

TEST(ProtocolDecodeTest, RoundTripsEveryMessageType) {
  const yisync::Bytes payload{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  std::vector<yisync::T_Message> messages;
  messages.push_back(yisync::network::make_hello(yisync::EM_Role::SENDER,
                                                 "sender",
                                                 yisync::kDefaultChunkSizeBytes,
                                                 256 * 1024));
  messages.push_back(yisync::T_Manifest1{
      .manifest_id = 7,
      .streams = {
          yisync::T_Manifest1Stream{
              .stream_id = 9001,
              .root = "/tmp/source",
              .entries = {
                  yisync::T_ManifestEntry{
                      .file_id = 1,
                      .seq = 1,
                      .kind = yisync::EM_EntryKind::REGULAR_FILE,
                      .name = "a.txt",
                      .size = 3,
                      .checksum = yisync::T_FileChecksum{
                          .algo = yisync::EM_ChecksumAlgo::CRC32C,
                          .offset = 0,
                          .len = 3,
                          .value = yisync::crc32c_bytes(payload),
                      },
                  },
              },
          },
      },
  });
  messages.push_back(yisync::T_Manifest2{
      .manifest_id = 7,
      .streams = {
          yisync::T_Manifest2Stream{
              .stream_id = 9001,
              .action = yisync::EM_Manifest2Action::CREATE_MISSING,
              .start_file_id = 1,
              .start_offset = 0,
          },
      },
  });
  messages.push_back(yisync::T_Create{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 1,
      .kind = yisync::EM_EntryKind::REGULAR_FILE,
      .name = "a.txt",
      .final_size = 3,
  });
  messages.push_back(yisync::T_Data{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 1,
      .offset = 0,
      .final_size = 3,
      .raw_len = 3,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = payload,
  });
  messages.push_back(yisync::T_FileBegin{
      .stream_id = 9001,
      .seq = 2,
      .file_id = 2,
      .name = "big.bin",
      .final_size = yisync::kDefaultChunkSizeBytes + 1,
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = 2,
  });
  messages.push_back(yisync::T_Chunk{
      .stream_id = 9001,
      .seq = 2,
      .file_id = 2,
      .chunk_index = 0,
      .offset = 0,
      .raw_len = 3,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = payload,
  });
  messages.push_back(yisync::T_FileCommit{.stream_id = 9001, .seq = 2, .file_id = 2});
  messages.push_back(yisync::T_Heartbeat{
      .stream_id = 9001,
      .next_seq = 3,
      .file_id = 2,
      .recv_window_bytes = 1024,
      .received_chunks = {yisync::T_ReceivedChunk{.seq = 2, .file_id = 2, .chunk_index = 0}},
      .missing_ranges = {yisync::T_MissingChunkRange{
          .seq = 2,
          .file_id = 2,
          .first_chunk_index = 1,
          .last_chunk_index = 1,
      }},
  });
  messages.push_back(yisync::T_Nack{
      .stream_id = 9001,
      .got_seq = 2,
      .expected_seq = 1,
      .file_id = 2,
      .reason = yisync::EM_NackReason::BAD_SEQ,
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
  const auto sender = yisync::network::make_hello(yisync::EM_Role::SENDER,
                                                  "sender",
                                                  yisync::kDefaultChunkSizeBytes,
                                                  1024);
  const auto receiver = yisync::network::make_hello(yisync::EM_Role::RECEIVER,
                                                    "receiver",
                                                    yisync::kDefaultChunkSizeBytes,
                                                    2048);

  const auto result = yisync::network::negotiate_hello(sender, receiver, yisync::EM_Role::RECEIVER);
  ASSERT_TRUE(result.ok) << result.error;
  EXPECT_EQ(result.negotiated_version, yisync::kProtocolVersion);
  EXPECT_EQ(result.negotiated_features & yisync::kRequiredFeatureFlags,
            yisync::kRequiredFeatureFlags);
  EXPECT_EQ(result.recv_window_bytes, 2048U);
}

TEST(ProtocolDecodeTest, RejectsIncompatibleHello) {
  auto sender = yisync::network::make_hello(yisync::EM_Role::SENDER,
                                            "sender",
                                            yisync::kDefaultChunkSizeBytes,
                                            1024);
  auto receiver = yisync::network::make_hello(yisync::EM_Role::RECEIVER,
                                              "receiver",
                                              yisync::kDefaultChunkSizeBytes * 2,
                                              2048);

  const auto result = yisync::network::negotiate_hello(sender, receiver, yisync::EM_Role::RECEIVER);
  EXPECT_FALSE(result.ok);
  EXPECT_NE(result.error.find("chunk_size"), std::string::npos);
}
