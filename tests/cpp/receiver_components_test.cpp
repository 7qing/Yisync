#include "core/yisync_sync.hpp"
#include "receiver/yisync_disk_writer.hpp"
#include "receiver/yisync_receiver_coordinator.hpp"
#include "receiver/yisync_receiver_streams.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

namespace {

class TempDir {
 public:
  TempDir() {
    path_ = std::filesystem::temp_directory_path() /
            ("yisync_unit_" + std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  const std::filesystem::path& path() const noexcept { return path_; }

 private:
  std::filesystem::path path_;
};

yisync::Bytes bytes(std::string_view text) {
  yisync::Bytes out;
  out.reserve(text.size());
  for (const char ch : text) {
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
  }
  return out;
}

yisync::Data data_from_payload(std::uint64_t stream_id,
                               std::uint64_t file_id,
                               std::uint64_t seq,
                               std::uint64_t offset,
                               std::uint64_t final_size,
                               yisync::Bytes payload) {
  return yisync::Data{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .offset = offset,
      .final_size = final_size,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

yisync::Chunk chunk_from_payload(std::uint64_t stream_id,
                                 std::uint64_t file_id,
                                 std::uint64_t seq,
                                 std::uint64_t chunk_index,
                                 yisync::Bytes payload) {
  return yisync::Chunk{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .chunk_index = chunk_index,
      .offset = chunk_index * yisync::kDefaultChunkSizeBytes,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .checksum_algo = yisync::ChecksumAlgo::Crc32c,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

}  // namespace

TEST(DiskWriterTest, ReportsQueueFullAndTaskFailure) {
  yisync::SpscDiskWriter writer;
  std::atomic<bool> release{false};
  std::atomic<int> ran{0};

  ASSERT_TRUE(writer.enqueue([&] {
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ran.fetch_add(1, std::memory_order_acq_rel);
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  std::size_t accepted = 0;
  for (std::size_t i = 0; i < yisync::kDefaultDiskWriterQueueCapacity + 4; ++i) {
    if (writer.enqueue([&] {
          ran.fetch_add(1, std::memory_order_acq_rel);
        })) {
      ++accepted;
    }
  }
  EXPECT_GE(accepted, yisync::kDefaultDiskWriterQueueCapacity - 1);
  EXPECT_LE(accepted, yisync::kDefaultDiskWriterQueueCapacity);

  release.store(true, std::memory_order_release);
  writer.drain();
  EXPECT_FALSE(writer.failed());
  EXPECT_EQ(ran.load(std::memory_order_acquire),
            static_cast<int>(yisync::kDefaultDiskWriterQueueCapacity + 1));

  ASSERT_TRUE(writer.enqueue([] {
    throw std::runtime_error("writer boom");
  }));
  writer.drain();
  EXPECT_TRUE(writer.failed());
  EXPECT_NE(std::string(writer.error()).find("writer boom"), std::string::npos);
}

TEST(ReceiverCoordinatorTest, AppliesCreateDataAndReportsChecksumNack) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);

  const auto create_actions = coordinator.apply_create(1, yisync::Create{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 10,
      .kind = yisync::EntryKind::RegularFile,
      .name = "file.txt",
      .final_size = 5,
  });
  EXPECT_TRUE(create_actions.nacks.empty());
  ASSERT_FALSE(create_actions.heartbeats.empty());

  auto bad = data_from_payload(9001, 10, 1, 0, 5, bytes("hello"));
  bad.checksum = yisync::Bytes{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}};
  const auto bad_actions = coordinator.apply_data(1, bad);
  ASSERT_FALSE(bad_actions.nacks.empty());
  EXPECT_EQ(bad_actions.nacks.front().nack.reason, yisync::NackReason::BadChecksum);

  auto data = data_from_payload(9001, 10, 1, 0, 5, bytes("hello"));
  const auto data_actions = coordinator.apply_data(1, data);
  EXPECT_TRUE(data_actions.nacks.empty());
  writer.drain();
  const auto completed = coordinator.poll_completions();
  EXPECT_TRUE(completed.failed == false);

  std::ifstream input(tmp.path() / "file.txt", std::ios::binary);
  std::string content;
  std::getline(input, content);
  EXPECT_EQ(content, "hello");
}

TEST(ReceiverCoordinatorTest, ReportsSizeConflict) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);

  ASSERT_TRUE(coordinator.apply_create(1, yisync::Create{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 10,
      .kind = yisync::EntryKind::RegularFile,
      .name = "size.txt",
      .final_size = 5,
  }).nacks.empty());

  const auto actions = coordinator.apply_data(1, data_from_payload(9001, 10, 1, 0, 6, bytes("hello")));
  ASSERT_FALSE(actions.nacks.empty());
  EXPECT_EQ(actions.nacks.front().nack.reason, yisync::NackReason::SizeConflict);
}

TEST(ReceiverCoordinatorTest, CommitsChunkFileThroughWriter) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);

  yisync::Bytes payload(yisync::kDefaultChunkSizeBytes + 1);
  for (std::size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<std::byte>('A' + (i % 26));
  }
  const yisync::FileChecksum full_checksum{
      .algo = yisync::ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(payload.size()),
      .value = yisync::crc32c_bytes(payload),
  };
  const auto begin_actions = coordinator.apply_begin(1, yisync::FileBegin{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 20,
      .name = "big.bin",
      .final_size = static_cast<std::uint64_t>(payload.size()),
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = 2,
      .file_checksum = full_checksum,
  });
  EXPECT_TRUE(begin_actions.nacks.empty());

  yisync::Bytes chunk0(payload.begin(),
                       payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes));
  yisync::Bytes chunk1(payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes),
                       payload.end());
  EXPECT_TRUE(coordinator.apply_chunk(1, chunk_from_payload(9001, 20, 1, 1, chunk1)).nacks.empty());
  EXPECT_TRUE(coordinator.apply_chunk(1, chunk_from_payload(9001, 20, 1, 0, chunk0)).nacks.empty());

  const auto commit_actions = coordinator.apply_commit(1, yisync::FileCommit{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 20,
  });
  EXPECT_TRUE(commit_actions.nacks.empty());
  EXPECT_TRUE(commit_actions.schedule_commit_poll);
  writer.drain();
  const auto completed = coordinator.poll_completions();
  EXPECT_FALSE(completed.failed);

  const auto final_path = tmp.path() / "big.bin";
  ASSERT_TRUE(std::filesystem::exists(final_path));
  EXPECT_EQ(std::filesystem::file_size(final_path), payload.size());
}

TEST(ReceiverCoordinatorTest, ReportsBadCommitWhenChunkMissing) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);

  yisync::Bytes payload(yisync::kDefaultChunkSizeBytes + 1);
  std::fill(payload.begin(), payload.end(), std::byte{'x'});
  const yisync::FileChecksum full_checksum{
      .algo = yisync::ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(payload.size()),
      .value = yisync::crc32c_bytes(payload),
  };

  ASSERT_TRUE(coordinator.apply_begin(1, yisync::FileBegin{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 30,
      .name = "missing.bin",
      .final_size = static_cast<std::uint64_t>(payload.size()),
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = 2,
      .file_checksum = full_checksum,
  }).nacks.empty());

  yisync::Bytes chunk0(payload.begin(),
                       payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes));
  ASSERT_TRUE(coordinator.apply_chunk(1, chunk_from_payload(9001, 30, 1, 0, chunk0)).nacks.empty());

  const auto actions = coordinator.apply_commit(1, yisync::FileCommit{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 30,
  });
  ASSERT_FALSE(actions.nacks.empty());
  EXPECT_EQ(actions.nacks.front().nack.reason, yisync::NackReason::BadCommit);
}

TEST(ReceiverCoordinatorTest, ReportsChunkCommitChecksumFailureFromWriter) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);

  yisync::Bytes payload(yisync::kDefaultChunkSizeBytes + 1);
  std::fill(payload.begin(), payload.end(), std::byte{'q'});
  yisync::Bytes wrong_checksum_payload = payload;
  wrong_checksum_payload[0] = std::byte{'z'};
  const yisync::FileChecksum wrong_full_checksum{
      .algo = yisync::ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(payload.size()),
      .value = yisync::crc32c_bytes(wrong_checksum_payload),
  };

  ASSERT_TRUE(coordinator.apply_begin(1, yisync::FileBegin{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 40,
      .name = "bad-commit.bin",
      .final_size = static_cast<std::uint64_t>(payload.size()),
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = 2,
      .file_checksum = wrong_full_checksum,
  }).nacks.empty());

  yisync::Bytes chunk0(payload.begin(),
                       payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes));
  yisync::Bytes chunk1(payload.begin() + static_cast<std::ptrdiff_t>(yisync::kDefaultChunkSizeBytes),
                       payload.end());
  ASSERT_TRUE(coordinator.apply_chunk(1, chunk_from_payload(9001, 40, 1, 0, chunk0)).nacks.empty());
  ASSERT_TRUE(coordinator.apply_chunk(1, chunk_from_payload(9001, 40, 1, 1, chunk1)).nacks.empty());

  const auto commit_actions = coordinator.apply_commit(1, yisync::FileCommit{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 40,
  });
  ASSERT_TRUE(commit_actions.nacks.empty());
  ASSERT_TRUE(commit_actions.schedule_commit_poll);
  writer.drain();
  const auto completed = coordinator.poll_completions();
  EXPECT_TRUE(completed.failed);
  ASSERT_FALSE(completed.nacks.empty());
  EXPECT_EQ(completed.nacks.front().nack.reason, yisync::NackReason::ChecksumMismatch);
}

TEST(ReceiverCoordinatorTest, ReportsAppendFsyncFailure) {
  TempDir tmp;
  std::unordered_map<std::uint64_t, std::filesystem::path> roots;
  roots[9001] = tmp.path();
  yisync::ReceiverStreamMap streams(9001, tmp.path(), &roots);
  yisync::SpscDiskWriter writer;
  yisync::ReceiverCoordinator coordinator(streams, writer, 256 * 1024, 8);
  std::atomic<bool> release_writer{false};
  ASSERT_TRUE(writer.enqueue([&] {
    while (!release_writer.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  ASSERT_TRUE(coordinator.apply_create(1, yisync::Create{
      .stream_id = 9001,
      .seq = 1,
      .file_id = 50,
      .kind = yisync::EntryKind::RegularFile,
      .name = "append.txt",
      .final_size = 5,
  }).nacks.empty());

  const auto actions = coordinator.apply_data(1, data_from_payload(9001, 50, 1, 0, 5, bytes("hello")));
  EXPECT_TRUE(actions.nacks.empty());
  auto& context = streams.context_for(9001);
  ASSERT_TRUE(context.append_fsync_queued);
  std::filesystem::remove(tmp.path() / "append.txt");
  release_writer.store(true, std::memory_order_release);
  writer.drain();
  const auto completed = coordinator.poll_completions();
  EXPECT_TRUE(completed.failed);
  EXPECT_NE(completed.failure.find("append fsync failed"), std::string::npos);
}
