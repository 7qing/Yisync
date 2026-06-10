#pragma once

#include "core/yisync_protocol.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yisync {

struct ReceiverStreamState {
  bool active = false;
  std::uint64_t expected_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::uint64_t current_final_size = 0;
  std::uint64_t next_create_file_id = 0;
};

struct ChunkCommitTask {
  std::filesystem::path root;
  std::filesystem::path temp_root;
  std::filesystem::path temp_path;
  std::filesystem::path final_path;
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t final_size = 0;
  FileChecksum file_checksum;
};

struct ChunkCommitResult {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t final_size = 0;
  std::filesystem::path final_path;
};

class ReceiverStream {
 public:
  ReceiverStream(std::uint64_t stream_id, std::filesystem::path root);

  const ReceiverStreamState& state() const noexcept;
  std::filesystem::path current_path() const;
  void refresh_committed_from_disk();
  Heartbeat heartbeat(std::uint64_t recv_window_bytes, std::uint64_t durable_offset = 0) const;
  std::optional<Nack> apply(const Create& create);
  std::optional<Nack> apply(const Data& data);

 private:
  std::uint64_t stream_id_ = 0;
  std::filesystem::path root_;
  ReceiverStreamState state_;
  EntryKind current_kind_ = EntryKind::RegularFile;
  std::filesystem::path current_path_;

  void reset_session_from_disk();
  std::filesystem::path path_for_name(const std::string& name) const;
  std::filesystem::path path_for_file(std::uint64_t file_id) const;
  Nack nack(std::uint64_t stream_id,
            std::uint64_t got_seq,
            std::uint64_t file_id,
            std::uint64_t offset,
            NackReason reason,
            std::string detail) const;
};

class ChunkedReceiverStream {
 public:
  ChunkedReceiverStream(std::uint64_t stream_id, std::filesystem::path root);
  ~ChunkedReceiverStream();

  ChunkedReceiverStream(const ChunkedReceiverStream&) = delete;
  ChunkedReceiverStream& operator=(const ChunkedReceiverStream&) = delete;
  ChunkedReceiverStream(ChunkedReceiverStream&&) noexcept;
  ChunkedReceiverStream& operator=(ChunkedReceiverStream&&) noexcept;

  std::uint64_t expected_seq() const noexcept;
  void refresh_committed_from_disk();
  std::optional<Nack> prepare_commit(const FileCommit& commit, ChunkCommitTask& task);
  void abort_commit(std::uint64_t seq) noexcept;
  void finish_commit(const ChunkCommitResult& result);
  static ChunkCommitResult write_commit_task(const ChunkCommitTask& task);
  std::vector<MissingChunkRange> missing_ranges(std::uint64_t seq,
                                                std::uint64_t max_ranges) const;
  std::optional<Nack> apply(const FileBegin& begin);
  std::optional<Nack> apply(const Chunk& chunk);
  std::optional<Nack> apply(const FileCommit& commit);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yisync
