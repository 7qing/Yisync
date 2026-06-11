#include "sender/yisync_chunk_resend.hpp"

#include <algorithm>
#include <cstddef>

namespace yisync {
namespace {

bool can_send_chunk(const T_ChunkResendState& state,
                    std::uint64_t chunk_index,
                    bool priority_only,
                    std::uint64_t current_tick,
                    std::uint64_t retransmit_ticks) {
  const auto index = static_cast<std::size_t>(chunk_index);
  if (index >= state.acked.size() || state.acked[index]) {
    return false;
  }
  if (priority_only && !state.priority[index]) {
    return false;
  }
  if (!state.sent[index]) {
    return true;
  }
  return state.priority[index] &&
         current_tick >= state.send_tick[index] &&
         current_tick - state.send_tick[index] >= retransmit_ticks;
}

}  // namespace

void initialize_chunk_resend_state(T_ChunkResendState& state,
                                   std::uint64_t chunk_count,
                                   const std::vector<std::uint64_t>& requested_order) {
  state.chunk_count = chunk_count;
  state.order.clear();
  state.order.reserve(static_cast<std::size_t>(state.chunk_count));
  for (const auto chunk_index : requested_order) {
    if (chunk_index < state.chunk_count) {
      state.order.push_back(chunk_index);
    }
  }
  if (state.order.size() != state.chunk_count) {
    state.order.clear();
    if (state.chunk_count > 0) {
      state.order.push_back(state.chunk_count - 1);
      for (std::uint64_t i = 0; i + 1 < state.chunk_count; ++i) {
        state.order.push_back(i);
      }
    }
  }
  state.acked.assign(static_cast<std::size_t>(state.chunk_count), false);
  state.sent.assign(static_cast<std::size_t>(state.chunk_count), false);
  state.line.assign(static_cast<std::size_t>(state.chunk_count), LineId{0});
  state.send_tick.assign(static_cast<std::size_t>(state.chunk_count), 0);
  state.attempts.assign(static_cast<std::size_t>(state.chunk_count), 0);
  state.priority.assign(static_cast<std::size_t>(state.chunk_count), false);
}

void reset_chunk_resend_state(T_ChunkResendState& state) {
  std::fill(state.acked.begin(), state.acked.end(), false);
  std::fill(state.sent.begin(), state.sent.end(), false);
  std::fill(state.line.begin(), state.line.end(), LineId{0});
  std::fill(state.send_tick.begin(), state.send_tick.end(), 0);
  std::fill(state.attempts.begin(), state.attempts.end(), 0);
  std::fill(state.priority.begin(), state.priority.end(), false);
}

bool all_chunks_acked(const T_ChunkResendState& state) {
  return std::all_of(state.acked.begin(), state.acked.end(), [](bool acked) {
    return acked;
  });
}

bool chunk_index_valid(const T_ChunkResendState& state, std::uint64_t chunk_index) noexcept {
  return chunk_index < state.chunk_count &&
         static_cast<std::size_t>(chunk_index) < state.acked.size();
}

std::uint64_t chunk_attempts(const T_ChunkResendState& state, std::uint64_t chunk_index) noexcept {
  if (!chunk_index_valid(state, chunk_index)) {
    return 0;
  }
  return state.attempts[static_cast<std::size_t>(chunk_index)];
}

std::optional<std::uint64_t> next_chunk_to_send(const T_ChunkResendState& state,
                                                std::uint64_t current_tick,
                                                std::uint64_t retransmit_ticks) {
  for (std::uint64_t index = 0; index < state.priority.size(); ++index) {
    if (can_send_chunk(state, index, true, current_tick, retransmit_ticks)) {
      return index;
    }
  }

  for (const auto chunk_index : state.order) {
    if (can_send_chunk(state, chunk_index, false, current_tick, retransmit_ticks)) {
      return chunk_index;
    }
  }
  return std::nullopt;
}

T_ChunkSendMark mark_chunk_sent(T_ChunkResendState& state,
                              std::uint64_t chunk_index,
                              LineId line_id,
                              std::uint64_t current_tick) {
  if (!chunk_index_valid(state, chunk_index)) {
    return {};
  }
  const auto index = static_cast<std::size_t>(chunk_index);
  const bool retransmit = state.attempts[index] > 0;
  state.sent[index] = true;
  state.line[index] = line_id;
  state.send_tick[index] = current_tick;
  state.attempts[index] += 1;
  state.priority[index] = false;
  return T_ChunkSendMark{
      .marked = true,
      .retransmit = retransmit,
      .attempt = state.attempts[index],
  };
}

bool acknowledge_chunk(T_ChunkResendState& state, std::uint64_t chunk_index) {
  if (!chunk_index_valid(state, chunk_index)) {
    return false;
  }
  const auto index = static_cast<std::size_t>(chunk_index);
  state.acked[index] = true;
  state.sent[index] = true;
  state.line[index] = 0;
  state.priority[index] = false;
  return true;
}

bool mark_chunk_lost(T_ChunkResendState& state, std::uint64_t chunk_index) {
  if (!chunk_index_valid(state, chunk_index)) {
    return false;
  }
  const auto index = static_cast<std::size_t>(chunk_index);
  if (!state.sent[index] || state.acked[index]) {
    return false;
  }
  state.sent[index] = false;
  state.line[index] = 0;
  state.priority[index] = true;
  return true;
}

std::vector<T_MissingHintApplied> apply_missing_hints(T_ChunkResendState& state,
                                                    std::uint64_t seq,
                                                    std::uint64_t file_id,
                                                    const std::vector<T_MissingChunkRange>& ranges) {
  std::vector<T_MissingHintApplied> applied;
  if (state.chunk_count == 0) {
    return applied;
  }
  for (const auto& range : ranges) {
    if (range.seq != seq || range.file_id != file_id) {
      continue;
    }
    const auto first = std::min<std::uint64_t>(range.first_chunk_index, state.chunk_count);
    const auto last = std::min<std::uint64_t>(range.last_chunk_index, state.chunk_count - 1);
    if (first > last) {
      continue;
    }
    bool changed = false;
    for (std::uint64_t chunk_index = first; chunk_index <= last; ++chunk_index) {
      const auto index = static_cast<std::size_t>(chunk_index);
      if (!state.acked[index]) {
        state.priority[index] = true;
        changed = true;
      }
    }
    if (changed) {
      applied.push_back(T_MissingHintApplied{
          .first_chunk_index = first,
          .last_chunk_index = last,
      });
    }
  }
  return applied;
}

}  // namespace yisync
