#pragma once

#include "core/yisync_protocol.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yisync {

struct T_ReceiverStreamState {
  bool active = false;
  std::uint64_t expected_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::uint64_t current_final_size = 0;
  std::uint64_t next_create_file_id = 0;
};

struct T_ChunkCommitTask {
  std::filesystem::path root;
  std::filesystem::path temp_root;
  std::filesystem::path temp_path;
  std::filesystem::path final_path;
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t final_size = 0;
  T_FileChecksum file_checksum;
};

struct T_ChunkCommitResult {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  std::uint64_t final_size = 0;
  std::filesystem::path final_path;
};

class T_ReceiverStream {
 public:
  T_ReceiverStream(std::uint64_t stream_id, std::filesystem::path root);

  const T_ReceiverStreamState& state() const noexcept;
  std::filesystem::path current_path() const;
  void refresh_committed_from_disk();
  T_Heartbeat heartbeat(std::uint64_t recv_window_bytes, std::uint64_t durable_offset = 0) const;
  std::optional<T_Nack> apply(const T_Create& create);
  std::optional<T_Nack> apply(const T_Data& data);

 private:
  std::uint64_t stream_id_ = 0;
  std::filesystem::path root_;
  T_ReceiverStreamState state_;
  EM_EntryKind current_kind_ = EM_EntryKind::REGULAR_FILE;
  std::filesystem::path current_path_;

  void reset_session_from_disk();
  std::filesystem::path path_for_name(const std::string& name) const;
  std::filesystem::path path_for_file(std::uint64_t file_id) const;
  T_Nack nack(std::uint64_t stream_id,
            std::uint64_t got_seq,
            std::uint64_t file_id,
            std::uint64_t offset,
            EM_NackReason reason,
            std::string detail) const;
};

class T_ChunkedReceiverStream {
 public:
  T_ChunkedReceiverStream(std::uint64_t stream_id, std::filesystem::path root);
  ~T_ChunkedReceiverStream();

  T_ChunkedReceiverStream(const T_ChunkedReceiverStream&) = delete;
  T_ChunkedReceiverStream& operator=(const T_ChunkedReceiverStream&) = delete;
  T_ChunkedReceiverStream(T_ChunkedReceiverStream&&) noexcept;
  T_ChunkedReceiverStream& operator=(T_ChunkedReceiverStream&&) noexcept;

  std::uint64_t expected_seq() const noexcept;
  void refresh_committed_from_disk();
  std::optional<T_Nack> prepare_commit(const T_FileCommit& commit, T_ChunkCommitTask& task);
  void abort_commit(std::uint64_t seq) noexcept;
  void finish_commit(const T_ChunkCommitResult& result);
  static T_ChunkCommitResult write_commit_task(const T_ChunkCommitTask& task);
  std::vector<T_MissingChunkRange> missing_ranges(std::uint64_t seq,
                                                std::uint64_t max_ranges) const;
  std::optional<T_Nack> apply(const T_FileBegin& begin);
  std::optional<T_Nack> apply(const T_Chunk& chunk);
  std::optional<T_Nack> apply(const T_FileCommit& commit);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace yisync
