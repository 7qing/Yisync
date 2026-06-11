#pragma once

#include "receiver/yisync_disk_writer.hpp"
#include "receiver/yisync_receiver_streams.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace yisync {

struct T_ReceiverHeartbeatAction {
  LineId line_id = 0;
  T_Heartbeat heartbeat;
  bool flush = false;
  bool only_if_line_open = false;
};

struct T_ReceiverNackAction {
  LineId line_id = 0;
  T_Nack nack;
  bool only_if_line_open = false;
  std::string closed_log;
};

struct T_ReceiverLogAction {
  bool error = false;
  std::string text;
};

struct T_ReceiverActionBatch {
  std::vector<T_ReceiverHeartbeatAction> heartbeats;
  std::vector<T_ReceiverNackAction> nacks;
  std::vector<T_ReceiverLogAction> logs;
  bool schedule_quiet_stop = false;
  bool schedule_commit_poll = false;
  bool failed = false;
  std::string failure;
};

class T_ReceiverCoordinator {
 public:
  T_ReceiverCoordinator(T_ReceiverStreamMap& streams,
                      SpscDiskWriter& disk_writer,
                      std::uint64_t recv_window_bytes,
                      std::uint64_t max_missing_ranges_per_heartbeat);

  T_ReceiverActionBatch apply_create(LineId line_id, const T_Create& create);
  T_ReceiverActionBatch apply_data(LineId line_id, const T_Data& data);
  T_ReceiverActionBatch apply_begin(LineId line_id, const T_FileBegin& begin);
  T_ReceiverActionBatch apply_chunk(LineId line_id, const T_Chunk& chunk);
  T_ReceiverActionBatch apply_commit(LineId line_id, const T_FileCommit& commit);
  T_ReceiverActionBatch poll_completions();

  bool append_durable_idle() const;
  bool has_pending_chunk_commit() const;

 private:
  struct T_CommittedFileInfo {
    std::filesystem::path path;
    std::uint64_t size = 0;
  };

  T_ReceiverStreamMap& streams_;
  SpscDiskWriter& disk_writer_;
  std::uint64_t recv_window_bytes_ = 0;
  std::uint64_t max_missing_ranges_per_heartbeat_ = 0;

  T_CommittedFileInfo committed_file_info(std::uint64_t stream_id,
                                        std::uint64_t file_id) const;
  void reset_append_durable_context(T_ReceiverStreamContext& context,
                                    std::uint64_t file_id,
                                    std::filesystem::path path,
                                    std::uint64_t durable_offset);
  void maybe_enqueue_append_fsync(T_ReceiverStreamContext& context,
                                  T_ReceiverActionBatch& actions,
                                  std::uint64_t target_offset);
  void poll_append_fsync(T_ReceiverStreamContext& context,
                         T_ReceiverActionBatch& actions);
  void poll_chunk_commit(T_ReceiverStreamContext& context,
                         T_ReceiverActionBatch& actions);
  void clear_chunk_commit(T_ReceiverStreamContext& context);
};

}  // namespace yisync
