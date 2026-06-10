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

struct ChunkCommitCompletion {
  std::atomic<bool> completed{false};
  std::atomic<bool> failed{false};
  std::array<char, 160> error{};
};

struct ReceiverStreamContext {
  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::unique_ptr<ReceiverStream> append;
  std::unique_ptr<ChunkedReceiverStream> chunk;
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
  std::shared_ptr<ChunkCommitCompletion> chunk_commit_completion;

  ReceiverStreamContext() = default;
  ReceiverStreamContext(const ReceiverStreamContext&) = delete;
  ReceiverStreamContext& operator=(const ReceiverStreamContext&) = delete;
  ReceiverStreamContext(ReceiverStreamContext&& other) noexcept;
  ReceiverStreamContext& operator=(ReceiverStreamContext&& other) noexcept;
};

class ReceiverStreamMap {
 public:
  using Map = std::unordered_map<std::uint64_t, ReceiverStreamContext>;
  using iterator = Map::iterator;
  using const_iterator = Map::const_iterator;

  ReceiverStreamMap(std::uint64_t default_stream_id,
                    std::filesystem::path default_root,
                    std::unordered_map<std::uint64_t, std::filesystem::path>* roots);

  std::filesystem::path stream_root_for(std::uint64_t stream_id) const;
  ReceiverStreamContext& context_for(std::uint64_t stream_id);
  ReceiverStream& append_receiver_for(std::uint64_t stream_id);
  ChunkedReceiverStream& chunk_receiver_for(std::uint64_t stream_id);
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
