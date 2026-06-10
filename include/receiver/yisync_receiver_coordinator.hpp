#pragma once

#include "receiver/yisync_disk_writer.hpp"
#include "receiver/yisync_receiver_streams.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace yisync {

struct ReceiverHeartbeatAction {
  LineId line_id = 0;
  Heartbeat heartbeat;
  bool flush = false;
  bool only_if_line_open = false;
};

struct ReceiverNackAction {
  LineId line_id = 0;
  Nack nack;
  bool only_if_line_open = false;
  std::string closed_log;
};

struct ReceiverLogAction {
  bool error = false;
  std::string text;
};

struct ReceiverActionBatch {
  std::vector<ReceiverHeartbeatAction> heartbeats;
  std::vector<ReceiverNackAction> nacks;
  std::vector<ReceiverLogAction> logs;
  bool schedule_quiet_stop = false;
  bool schedule_commit_poll = false;
  bool failed = false;
  std::string failure;
};

class ReceiverCoordinator {
 public:
  ReceiverCoordinator(ReceiverStreamMap& streams,
                      SpscDiskWriter& disk_writer,
                      std::uint64_t recv_window_bytes,
                      std::uint64_t max_missing_ranges_per_heartbeat);

  ReceiverActionBatch apply_create(LineId line_id, const Create& create);
  ReceiverActionBatch apply_data(LineId line_id, const Data& data);
  ReceiverActionBatch apply_begin(LineId line_id, const FileBegin& begin);
  ReceiverActionBatch apply_chunk(LineId line_id, const Chunk& chunk);
  ReceiverActionBatch apply_commit(LineId line_id, const FileCommit& commit);
  ReceiverActionBatch poll_completions();

  bool append_durable_idle() const;
  bool has_pending_chunk_commit() const;

 private:
  struct CommittedFileInfo {
    std::filesystem::path path;
    std::uint64_t size = 0;
  };

  ReceiverStreamMap& streams_;
  SpscDiskWriter& disk_writer_;
  std::uint64_t recv_window_bytes_ = 0;
  std::uint64_t max_missing_ranges_per_heartbeat_ = 0;

  CommittedFileInfo committed_file_info(std::uint64_t stream_id,
                                        std::uint64_t file_id) const;
  void reset_append_durable_context(ReceiverStreamContext& context,
                                    std::uint64_t file_id,
                                    std::filesystem::path path,
                                    std::uint64_t durable_offset);
  void maybe_enqueue_append_fsync(ReceiverStreamContext& context,
                                  ReceiverActionBatch& actions,
                                  std::uint64_t target_offset);
  void poll_append_fsync(ReceiverStreamContext& context,
                         ReceiverActionBatch& actions);
  void poll_chunk_commit(ReceiverStreamContext& context,
                         ReceiverActionBatch& actions);
  void clear_chunk_commit(ReceiverStreamContext& context);
};

}  // namespace yisync
