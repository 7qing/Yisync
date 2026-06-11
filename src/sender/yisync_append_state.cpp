#include "sender/yisync_append_state.hpp"

namespace yisync {

void reset_append_state(T_AppendSendState& state) noexcept {
  state = T_AppendSendState{};
}

void reset_append_inflight(T_AppendSendState& state) noexcept {
  state.create_sent = false;
  state.create_ready = false;
  state.data_sent = false;
  state.create_line_id = 0;
  state.data_line_id = 0;
  state.current_data_len = 0;
}

bool append_inflight(const T_AppendSendState& state) noexcept {
  return state.create_sent || state.data_sent;
}

void start_append_plan(T_AppendSendState& state,
                       const T_SyncStart& diff,
                       std::uint64_t task_seq,
                       std::uint64_t source_size) {
  state.needs_create = diff.start_action == EM_StartAction::CREATE_MISSING;
  state.offset = diff.start_offset;
  state.next_offset = diff.start_offset;
  state.data_needed = state.offset < source_size;
  state.seq = task_seq;
  state.done_next_seq = task_seq + 1;
  state.create_ready = !state.needs_create;
  state.create_sent = false;
  state.data_sent = false;
  state.create_line_id = 0;
  state.data_line_id = 0;
  state.current_data_len = 0;
}

void mark_append_create_sent(T_AppendSendState& state, LineId line_id) noexcept {
  state.create_sent = true;
  state.create_line_id = line_id;
}

void mark_append_data_sent(T_AppendSendState& state,
                           LineId line_id,
                           std::uint64_t data_len) noexcept {
  state.data_sent = true;
  state.data_line_id = line_id;
  state.current_data_len = data_len;
}

bool mark_append_create_ready_from_heartbeat(T_AppendSendState& state,
                                             const T_Heartbeat& heartbeat) noexcept {
  if (!state.needs_create || !state.create_sent || state.create_ready) {
    return false;
  }
  if (heartbeat.next_seq <= state.seq && heartbeat.offset != 0) {
    return false;
  }
  state.create_ready = true;
  return true;
}

bool mark_append_data_ready_from_heartbeat(T_AppendSendState& state,
                                           const T_Heartbeat& heartbeat) noexcept {
  if (!state.data_sent) {
    return false;
  }
  const auto target_offset = state.next_offset + state.current_data_len;
  if (heartbeat.next_seq <= state.seq &&
      heartbeat.offset < target_offset &&
      heartbeat.durable_offset < target_offset) {
    return false;
  }
  state.next_offset = target_offset;
  state.data_sent = false;
  state.data_line_id = 0;
  state.current_data_len = 0;
  return true;
}

bool append_complete_by_heartbeat(const T_AppendSendState& state,
                                  const T_Heartbeat& heartbeat,
                                  EM_EntryKind kind,
                                  std::uint64_t source_size) noexcept {
  if (state.data_sent || state.next_offset < source_size ||
      heartbeat.next_seq < state.done_next_seq) {
    return false;
  }
  return kind != EM_EntryKind::REGULAR_FILE || heartbeat.durable_offset >= source_size;
}

void mark_append_create_retransmitted(T_AppendSendState& state, LineId line_id) noexcept {
  state.create_sent = true;
  state.create_line_id = line_id;
}

void mark_append_data_retransmitted(T_AppendSendState& state,
                                    LineId line_id,
                                    std::uint64_t offset,
                                    std::uint64_t end_offset) noexcept {
  state.data_sent = true;
  state.data_line_id = line_id;
  if (end_offset > offset) {
    state.current_data_len = end_offset - offset;
  }
}

bool append_lost_matches(const T_AppendSendState& state, const T_LostSend& lost) noexcept {
  const bool lost_create = lost.kind == EM_SendKind::CREATE &&
                           state.create_sent &&
                           lost.seq == state.seq;
  const bool lost_data = lost.kind == EM_SendKind::DATA &&
                         state.data_sent &&
                         lost.seq == state.seq &&
                         lost.offset == state.next_offset;
  return lost_create || lost_data;
}

}  // namespace yisync
