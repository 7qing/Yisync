#pragma once

#include "core/yisync_sync.hpp"
#include "network/yisync_scheduler.hpp"

#include <cstdint>

namespace yisync {

struct T_AppendSendState {
  bool needs_create = false;
  bool data_needed = false;
  bool create_sent = false;
  bool create_ready = false;
  bool data_sent = false;
  LineId create_line_id = 0;
  LineId data_line_id = 0;
  std::uint64_t offset = 0;
  std::uint64_t next_offset = 0;
  std::uint64_t current_data_len = 0;
  std::uint64_t seq = 0;
  std::uint64_t done_next_seq = 0;
};

void reset_append_state(T_AppendSendState& state) noexcept;
void reset_append_inflight(T_AppendSendState& state) noexcept;
bool append_inflight(const T_AppendSendState& state) noexcept;
void start_append_plan(T_AppendSendState& state,
                       const T_SyncStart& diff,
                       std::uint64_t task_seq,
                       std::uint64_t source_size);
void mark_append_create_sent(T_AppendSendState& state, LineId line_id) noexcept;
void mark_append_data_sent(T_AppendSendState& state,
                           LineId line_id,
                           std::uint64_t data_len) noexcept;
bool mark_append_create_ready_from_heartbeat(T_AppendSendState& state,
                                             const T_Heartbeat& heartbeat) noexcept;
bool mark_append_data_ready_from_heartbeat(T_AppendSendState& state,
                                           const T_Heartbeat& heartbeat) noexcept;
bool append_complete_by_heartbeat(const T_AppendSendState& state,
                                  const T_Heartbeat& heartbeat,
                                  EM_EntryKind kind,
                                  std::uint64_t source_size) noexcept;
void mark_append_create_retransmitted(T_AppendSendState& state, LineId line_id) noexcept;
void mark_append_data_retransmitted(T_AppendSendState& state,
                                    LineId line_id,
                                    std::uint64_t offset,
                                    std::uint64_t end_offset) noexcept;
bool append_lost_matches(const T_AppendSendState& state, const T_LostSend& lost) noexcept;

}  // namespace yisync
