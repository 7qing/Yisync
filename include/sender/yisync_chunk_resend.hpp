#pragma once

#include "core/yisync_protocol.hpp"
#include "network/yisync_scheduler.hpp"

#include <cstdint>
#include <optional>
#include <vector>

namespace yisync {

struct T_ChunkResendState {
  std::uint64_t chunk_count = 0;
  std::vector<std::uint64_t> order;
  std::vector<bool> acked;
  std::vector<bool> sent;
  std::vector<LineId> line;
  std::vector<std::uint64_t> send_tick;
  std::vector<std::uint64_t> attempts;
  std::vector<bool> priority;
};

struct T_ChunkSendMark {
  bool marked = false;
  bool retransmit = false;
  std::uint64_t attempt = 0;
};

struct T_MissingHintApplied {
  std::uint64_t first_chunk_index = 0;
  std::uint64_t last_chunk_index = 0;
};

void initialize_chunk_resend_state(T_ChunkResendState& state,
                                   std::uint64_t chunk_count,
                                   const std::vector<std::uint64_t>& requested_order);
void reset_chunk_resend_state(T_ChunkResendState& state);
bool all_chunks_acked(const T_ChunkResendState& state);
bool chunk_index_valid(const T_ChunkResendState& state, std::uint64_t chunk_index) noexcept;
std::uint64_t chunk_attempts(const T_ChunkResendState& state, std::uint64_t chunk_index) noexcept;
std::optional<std::uint64_t> next_chunk_to_send(const T_ChunkResendState& state,
                                                std::uint64_t current_tick,
                                                std::uint64_t retransmit_ticks);
T_ChunkSendMark mark_chunk_sent(T_ChunkResendState& state,
                              std::uint64_t chunk_index,
                              LineId line_id,
                              std::uint64_t current_tick);
bool acknowledge_chunk(T_ChunkResendState& state, std::uint64_t chunk_index);
bool mark_chunk_lost(T_ChunkResendState& state, std::uint64_t chunk_index);
std::vector<T_MissingHintApplied> apply_missing_hints(T_ChunkResendState& state,
                                                    std::uint64_t seq,
                                                    std::uint64_t file_id,
                                                    const std::vector<T_MissingChunkRange>& ranges);

}  // namespace yisync
