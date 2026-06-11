#pragma once

#include "core/yisync_protocol.hpp"
#include "network/yisync_scheduler.hpp"
#include "receiver/yisync_receiver.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <unordered_map>

namespace yisync {

struct T_ChunkCommitCompletion {
  std::atomic<bool> completed{false};
  std::atomic<bool> failed{false};
  std::array<char, 160> error{};
};

struct T_ReceiverStreamContext {
  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::unique_ptr<T_ReceiverStream> append;
  std::unique_ptr<T_ChunkedReceiverStream> chunk;
  std::uint64_t append_durable_file_id = 0;
  std::filesystem::path append_durable_path;
  std::uint64_t append_durable_offset = 0;
  std::uint64_t append_durable_target = 0;
  std::uint64_t append_fsync_active_target = 0;
  LineId append_heartbeat_line_id = 0;
  bool append_fsync_queued = false;
  std::atomic<std::uint64_t> append_fsync_completed{0};
  std::atomic<bool> append_fsync_failed{false};
  std::array<char, 160> append_fsync_error{};
  bool chunk_commit_queued = false;
  LineId chunk_commit_line_id = 0;
  std::uint64_t chunk_commit_seq = 0;
  std::uint64_t chunk_commit_file_id = 0;
  std::uint64_t chunk_commit_final_size = 0;
  std::filesystem::path chunk_commit_final_path;
  std::shared_ptr<T_ChunkCommitCompletion> chunk_commit_completion;

  T_ReceiverStreamContext() = default;
  T_ReceiverStreamContext(const T_ReceiverStreamContext&) = delete;
  T_ReceiverStreamContext& operator=(const T_ReceiverStreamContext&) = delete;
  T_ReceiverStreamContext(T_ReceiverStreamContext&& other) noexcept;
  T_ReceiverStreamContext& operator=(T_ReceiverStreamContext&& other) noexcept;
};

class T_ReceiverStreamMap {
 public:
  using Map = std::unordered_map<std::uint64_t, T_ReceiverStreamContext>;
  using iterator = Map::iterator;
  using const_iterator = Map::const_iterator;

  T_ReceiverStreamMap(std::uint64_t default_stream_id,
                    std::filesystem::path default_root,
                    std::unordered_map<std::uint64_t, std::filesystem::path>* roots);

  std::filesystem::path stream_root_for(std::uint64_t stream_id) const;
  T_ReceiverStreamContext& context_for(std::uint64_t stream_id);
  T_ReceiverStream& append_receiver_for(std::uint64_t stream_id);
  T_ChunkedReceiverStream& chunk_receiver_for(std::uint64_t stream_id);
  bool append_durable_idle() const;
  bool has_pending_chunk_commit() const;

  iterator begin() noexcept;
  iterator end() noexcept;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;

 private:
  std::filesystem::path default_root_;
  std::uint64_t default_stream_id_ = 0;
  std::unordered_map<std::uint64_t, std::filesystem::path>* roots_ = nullptr;
  Map streams_;
};

}  // namespace yisync
