#include "receiver/yisync_receiver_coordinator.hpp"

#include "core/yisync_sync.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <fcntl.h>
#include <unistd.h>

namespace yisync {

namespace {

std::uint64_t file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  return ec ? 0 : size;
}

void copy_error(std::string_view message, std::array<char, 160>& out) {
  const auto count = std::min<std::size_t>(message.size(), out.size() - 1);
  std::copy_n(message.begin(), count, out.begin());
  out[count] = '\0';
}

EM_NackReason chunk_commit_error_reason(std::string_view error) {
  if (error.find("checksum") != std::string_view::npos) {
    return EM_NackReason::CHECKSUM_MISMATCH;
  }
  if (error.find("already exists") != std::string_view::npos) {
    return EM_NackReason::FILE_EXISTS;
  }
  return EM_NackReason::IO_ERROR;
}

void add_log(T_ReceiverActionBatch& actions, std::string text) {
  actions.logs.push_back(T_ReceiverLogAction{.error = false, .text = std::move(text)});
}

void add_error(T_ReceiverActionBatch& actions, std::string text) {
  actions.logs.push_back(T_ReceiverLogAction{.error = true, .text = std::move(text)});
}

std::string path_string(const std::filesystem::path& path) {
  std::ostringstream out;
  out << path;
  return out.str();
}

}  // namespace

T_ReceiverCoordinator::T_ReceiverCoordinator(T_ReceiverStreamMap& streams,
                                         SpscDiskWriter& disk_writer,
                                         std::uint64_t recv_window_bytes,
                                         std::uint64_t max_missing_ranges_per_heartbeat)
    : streams_(streams),
      disk_writer_(disk_writer),
      recv_window_bytes_(recv_window_bytes),
      max_missing_ranges_per_heartbeat_(max_missing_ranges_per_heartbeat) {}

T_ReceiverActionBatch T_ReceiverCoordinator::apply_create(LineId line_id, const T_Create& create) {
  T_ReceiverActionBatch actions;
  auto& append_receiver = streams_.append_receiver_for(create.stream_id);
  const auto result = append_receiver.apply(create);
  if (result.has_value()) {
    actions.nacks.push_back(T_ReceiverNackAction{.line_id = line_id, .nack = *result});
    return actions;
  }
  auto& context = streams_.context_for(create.stream_id);
  reset_append_durable_context(context, create.file_id, append_receiver.current_path(), 0);
  if (context.chunk) {
    context.chunk->refresh_committed_from_disk();
  }
  actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
      .line_id = line_id,
      .heartbeat = append_receiver.heartbeat(recv_window_bytes_,
                                             append_receiver.state().current_offset),
      .flush = true,
  });
  add_log(actions,
          "RECEIVER CREATE line=" + std::to_string(line_id) +
              " stream=" + std::to_string(create.stream_id) +
              " file_id=" + std::to_string(create.file_id) +
              " seq=" + std::to_string(create.seq));
  actions.schedule_quiet_stop = true;
  return actions;
}

T_ReceiverActionBatch T_ReceiverCoordinator::apply_data(LineId line_id, const T_Data& data) {
  T_ReceiverActionBatch actions;
  auto& append_receiver = streams_.append_receiver_for(data.stream_id);
  const auto result = append_receiver.apply(data);
  if (result.has_value()) {
    actions.nacks.push_back(T_ReceiverNackAction{.line_id = line_id, .nack = *result});
    return actions;
  }
  auto& context = streams_.context_for(data.stream_id);
  context.append_heartbeat_line_id = line_id;
  reset_append_durable_context(context, data.file_id, append_receiver.current_path(), data.offset);
  maybe_enqueue_append_fsync(context, actions, append_receiver.state().current_offset);
  actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
      .line_id = line_id,
      .heartbeat = append_receiver.heartbeat(recv_window_bytes_, context.append_durable_offset),
  });
  if (context.chunk) {
    context.chunk->refresh_committed_from_disk();
  }
  add_log(actions,
          "RECEIVER DATA line=" + std::to_string(line_id) +
              " stream=" + std::to_string(data.stream_id) +
              " file_id=" + std::to_string(data.file_id) +
              " seq=" + std::to_string(data.seq) +
              " offset=" + std::to_string(data.offset) +
              " len=" + std::to_string(data.raw_len));
  actions.schedule_quiet_stop = true;
  return actions;
}

T_ReceiverActionBatch T_ReceiverCoordinator::apply_begin(LineId line_id, const T_FileBegin& begin) {
  T_ReceiverActionBatch actions;
  auto& receiver = streams_.chunk_receiver_for(begin.stream_id);
  const auto result = receiver.apply(begin);
  if (result.has_value()) {
    actions.nacks.push_back(T_ReceiverNackAction{.line_id = line_id, .nack = *result});
    return actions;
  }
  actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
      .line_id = line_id,
      .heartbeat = T_Heartbeat{
          .stream_id = begin.stream_id,
          .next_seq = receiver.expected_seq(),
          .file_id = begin.file_id,
          .offset = 0,
          .durable_offset = 0,
          .recv_window_bytes = recv_window_bytes_,
      },
      .flush = true,
  });
  add_log(actions,
          "RECEIVER FILE_BEGIN line=" + std::to_string(line_id) +
              " stream=" + std::to_string(begin.stream_id) +
              " file_id=" + std::to_string(begin.file_id) +
              " chunks=" + std::to_string(begin.chunk_count) +
              " size=" + std::to_string(begin.final_size));
  return actions;
}

T_ReceiverActionBatch T_ReceiverCoordinator::apply_chunk(LineId line_id, const T_Chunk& chunk) {
  T_ReceiverActionBatch actions;
  auto& receiver = streams_.chunk_receiver_for(chunk.stream_id);
  const auto result = receiver.apply(chunk);
  if (result.has_value()) {
    actions.nacks.push_back(T_ReceiverNackAction{.line_id = line_id, .nack = *result});
    return actions;
  }
  actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
      .line_id = line_id,
      .heartbeat = T_Heartbeat{
          .stream_id = chunk.stream_id,
          .next_seq = receiver.expected_seq(),
          .file_id = chunk.file_id,
          .offset = 0,
          .durable_offset = 0,
          .recv_window_bytes = recv_window_bytes_,
          .received_chunks = {
              T_ReceivedChunk{
                  .seq = chunk.seq,
                  .file_id = chunk.file_id,
                  .chunk_index = chunk.chunk_index,
              },
          },
          .missing_ranges = receiver.missing_ranges(chunk.seq,
                                                    max_missing_ranges_per_heartbeat_),
      },
  });
  add_log(actions,
          "RECEIVER CHUNK line=" + std::to_string(line_id) +
              " stream=" + std::to_string(chunk.stream_id) +
              " file_id=" + std::to_string(chunk.file_id) +
              " index=" + std::to_string(chunk.chunk_index) +
              " offset=" + std::to_string(chunk.offset) +
              " len=" + std::to_string(chunk.raw_len));
  return actions;
}

T_ReceiverActionBatch T_ReceiverCoordinator::apply_commit(LineId line_id, const T_FileCommit& commit) {
  T_ReceiverActionBatch actions;
  auto& context = streams_.context_for(commit.stream_id);
  if (context.chunk_commit_queued) {
    if (context.chunk_commit_seq == commit.seq &&
        context.chunk_commit_file_id == commit.file_id) {
      add_log(actions,
              "RECEIVER FILE_COMMIT duplicate while queued line=" + std::to_string(line_id) +
                  " stream=" + std::to_string(commit.stream_id) +
                  " file_id=" + std::to_string(commit.file_id));
      return actions;
    }
    actions.nacks.push_back(T_ReceiverNackAction{
        .line_id = line_id,
        .nack = T_Nack{
            .stream_id = commit.stream_id,
            .got_seq = commit.seq,
            .expected_seq = context.chunk ? context.chunk->expected_seq() : commit.seq,
            .file_id = commit.file_id,
            .offset = 0,
            .expected_file_id = context.chunk_commit_file_id,
            .expected_offset = 0,
            .reason = EM_NackReason::BAD_COMMIT,
            .detail = "another commit is already pending",
        },
    });
    return actions;
  }

  auto& receiver = streams_.chunk_receiver_for(commit.stream_id);
  T_ChunkCommitTask task;
  const auto result = receiver.prepare_commit(commit, task);
  if (result.has_value()) {
    actions.nacks.push_back(T_ReceiverNackAction{.line_id = line_id, .nack = *result});
    return actions;
  }
  if (task.seq == 0) {
    if (commit.seq < receiver.expected_seq()) {
      const auto info = committed_file_info(commit.stream_id, commit.file_id);
      actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
          .line_id = line_id,
          .heartbeat = T_Heartbeat{
              .stream_id = commit.stream_id,
              .next_seq = receiver.expected_seq(),
              .file_id = commit.file_id,
              .offset = info.size,
              .durable_offset = info.size,
              .recv_window_bytes = recv_window_bytes_,
          },
          .flush = true,
      });
      add_log(actions,
              "RECEIVER FILE_COMMIT duplicate after complete line=" + std::to_string(line_id) +
                  " stream=" + std::to_string(commit.stream_id) +
                  " file_id=" + std::to_string(commit.file_id) +
                  " final_path=" + path_string(info.path) +
                  " expected_seq=" + std::to_string(receiver.expected_seq()));
    }
    return actions;
  }

  context.chunk_commit_queued = true;
  context.chunk_commit_line_id = line_id;
  context.chunk_commit_seq = task.seq;
  context.chunk_commit_file_id = task.file_id;
  context.chunk_commit_final_size = task.final_size;
  context.chunk_commit_final_path = task.final_path;
  auto completion = std::make_shared<T_ChunkCommitCompletion>();
  context.chunk_commit_completion = completion;

  const auto queued = disk_writer_.enqueue([task = std::move(task), completion] {
    try {
      (void)T_ChunkedReceiverStream::write_commit_task(task);
      completion->completed.store(true, std::memory_order_release);
    } catch (const std::exception& ex) {
      copy_error(ex.what(), completion->error);
      completion->failed.store(true, std::memory_order_release);
    }
  });
  if (!queued) {
    receiver.abort_commit(commit.seq);
    clear_chunk_commit(context);
    actions.nacks.push_back(T_ReceiverNackAction{
        .line_id = line_id,
        .nack = T_Nack{
            .stream_id = commit.stream_id,
            .got_seq = commit.seq,
            .expected_seq = receiver.expected_seq(),
            .file_id = commit.file_id,
            .offset = 0,
            .expected_file_id = commit.file_id,
            .expected_offset = 0,
            .reason = EM_NackReason::IO_ERROR,
            .detail = "disk writer queue full while enqueueing commit",
        },
    });
    return actions;
  }

  add_log(actions,
          "RECEIVER FILE_COMMIT queued line=" + std::to_string(line_id) +
              " stream=" + std::to_string(commit.stream_id) +
              " file_id=" + std::to_string(commit.file_id) +
              " final_path=" + path_string(context.chunk_commit_final_path) +
              " expected_seq=" + std::to_string(receiver.expected_seq()));
  actions.schedule_commit_poll = true;
  return actions;
}

T_ReceiverActionBatch T_ReceiverCoordinator::poll_completions() {
  T_ReceiverActionBatch actions;
  for (auto& [stream_id, context] : streams_) {
    (void)stream_id;
    poll_append_fsync(context, actions);
    if (actions.failed) {
      return actions;
    }
    poll_chunk_commit(context, actions);
    if (actions.failed) {
      return actions;
    }
  }
  return actions;
}

bool T_ReceiverCoordinator::append_durable_idle() const {
  return streams_.append_durable_idle();
}

bool T_ReceiverCoordinator::has_pending_chunk_commit() const {
  return streams_.has_pending_chunk_commit();
}

T_ReceiverCoordinator::T_CommittedFileInfo T_ReceiverCoordinator::committed_file_info(
    std::uint64_t stream_id,
    std::uint64_t file_id) const {
  const auto root = streams_.stream_root_for(stream_id);
  const auto manifest = scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
  auto path = root / file_name_for_id(file_id);
  auto size = file_size_or_zero(path);
  const auto entry_it = std::find_if(manifest.entries.begin(), manifest.entries.end(), [&](const auto& entry) {
    return entry.file_id == file_id;
  });
  if (entry_it != manifest.entries.end()) {
    path = root / entry_it->name;
    size = entry_it->size;
  }
  return T_CommittedFileInfo{.path = std::move(path), .size = size};
}

void T_ReceiverCoordinator::reset_append_durable_context(T_ReceiverStreamContext& context,
                                                       std::uint64_t file_id,
                                                       std::filesystem::path path,
                                                       std::uint64_t durable_offset) {
  if (context.append_durable_file_id == file_id) {
    if (!path.empty()) {
      context.append_durable_path = std::move(path);
    }
    return;
  }
  context.append_durable_file_id = file_id;
  context.append_durable_path = std::move(path);
  context.append_durable_offset = durable_offset;
  context.append_durable_target = durable_offset;
  context.append_fsync_active_target = 0;
  context.append_fsync_queued = false;
  context.append_fsync_completed.store(durable_offset, std::memory_order_release);
  context.append_fsync_failed.store(false, std::memory_order_release);
  std::fill(context.append_fsync_error.begin(), context.append_fsync_error.end(), '\0');
}

void T_ReceiverCoordinator::maybe_enqueue_append_fsync(T_ReceiverStreamContext& context,
                                                     T_ReceiverActionBatch& actions,
                                                     std::uint64_t target_offset) {
  if (target_offset <= context.append_durable_offset) {
    return;
  }
  context.append_durable_target = std::max(context.append_durable_target, target_offset);
  if (context.append_fsync_queued) {
    return;
  }

  auto* completed = &context.append_fsync_completed;
  auto* failed = &context.append_fsync_failed;
  auto* error = &context.append_fsync_error;
  const auto path = context.append_durable_path;
  if (path.empty()) {
    add_error(actions,
              "RECEIVER append fsync missing path stream=" + std::to_string(context.stream_id) +
                  " target_offset=" + std::to_string(target_offset));
    return;
  }
  const auto durable_target = context.append_durable_target;
  std::fill(error->begin(), error->end(), '\0');
  failed->store(false, std::memory_order_release);

  const auto queued = disk_writer_.enqueue([path, durable_target, completed, failed, error] {
    try {
      const int fd = ::open(path.c_str(), O_RDONLY);
      if (fd < 0) {
        throw std::runtime_error("failed to open append file for fsync: " + path.string());
      }
      if (::fsync(fd) != 0) {
        const auto error_number = errno;
        ::close(fd);
        throw std::runtime_error("failed to fsync append file: " +
                                 std::string(std::strerror(error_number)));
      }
      ::close(fd);
      completed->store(durable_target, std::memory_order_release);
    } catch (const std::exception& ex) {
      copy_error(ex.what(), *error);
      failed->store(true, std::memory_order_release);
    }
  });
  if (queued) {
    context.append_fsync_queued = true;
    context.append_fsync_active_target = durable_target;
  } else {
    add_error(actions,
              "RECEIVER append fsync writer queue full stream=" + std::to_string(context.stream_id) +
                  " target_offset=" + std::to_string(target_offset));
  }
}

void T_ReceiverCoordinator::poll_append_fsync(T_ReceiverStreamContext& context,
                                            T_ReceiverActionBatch& actions) {
  if (context.append_fsync_failed.load(std::memory_order_acquire)) {
    actions.failed = true;
    actions.failure = "RECEIVER append fsync failed stream=" + std::to_string(context.stream_id) +
                      " error=" + context.append_fsync_error.data();
    return;
  }

  const auto completed = context.append_fsync_completed.load(std::memory_order_acquire);
  if (completed > context.append_durable_offset) {
    context.append_durable_offset = completed;
    if (context.append && context.append_heartbeat_line_id != 0) {
      actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
          .line_id = context.append_heartbeat_line_id,
          .heartbeat = context.append->heartbeat(recv_window_bytes_,
                                                 context.append_durable_offset),
      });
    }
  }
  if (context.append_fsync_queued && completed >= context.append_fsync_active_target) {
    context.append_fsync_queued = false;
    context.append_fsync_active_target = 0;
  }
  if (!context.append_fsync_queued && context.append_durable_target > context.append_durable_offset) {
    maybe_enqueue_append_fsync(context, actions, context.append_durable_target);
  }
}

void T_ReceiverCoordinator::poll_chunk_commit(T_ReceiverStreamContext& context,
                                            T_ReceiverActionBatch& actions) {
  if (!context.chunk_commit_queued) {
    return;
  }
  auto completion = context.chunk_commit_completion;
  if (!completion) {
    actions.failed = true;
    actions.failure = "RECEIVER chunk commit missing completion stream=" +
                      std::to_string(context.stream_id);
    return;
  }
  if (completion->failed.load(std::memory_order_acquire)) {
    add_error(actions,
              "RECEIVER chunk commit failed stream=" + std::to_string(context.stream_id) +
                  " file_id=" + std::to_string(context.chunk_commit_file_id) +
                  " error=" + completion->error.data());
    if (context.chunk) {
      context.chunk->abort_commit(context.chunk_commit_seq);
    }
    actions.nacks.push_back(T_ReceiverNackAction{
        .line_id = context.chunk_commit_line_id,
        .nack = T_Nack{
            .stream_id = context.stream_id,
            .got_seq = context.chunk_commit_seq,
            .expected_seq = context.chunk ? context.chunk->expected_seq() : context.chunk_commit_seq,
            .file_id = context.chunk_commit_file_id,
            .offset = 0,
            .expected_file_id = context.chunk_commit_file_id,
            .expected_offset = 0,
            .reason = chunk_commit_error_reason(completion->error.data()),
            .detail = completion->error.data(),
        },
        .only_if_line_open = true,
        .closed_log = "RECEIVER chunk commit failed after line closed stream=" +
                      std::to_string(context.stream_id) +
                      " file_id=" + std::to_string(context.chunk_commit_file_id),
    });
    clear_chunk_commit(context);
    actions.failed = true;
    actions.failure = "receiver chunk commit failed";
    return;
  }
  if (!completion->completed.load(std::memory_order_acquire)) {
    return;
  }
  if (!context.chunk) {
    actions.failed = true;
    actions.failure = "RECEIVER chunk commit completed without receiver stream=" +
                      std::to_string(context.stream_id);
    return;
  }

  T_ChunkCommitResult result{
      .stream_id = context.stream_id,
      .seq = context.chunk_commit_seq,
      .file_id = context.chunk_commit_file_id,
      .final_size = context.chunk_commit_final_size,
      .final_path = context.chunk_commit_final_path,
  };
  try {
    context.chunk->finish_commit(result);
  } catch (const std::exception& ex) {
    actions.failed = true;
    actions.failure = "RECEIVER chunk commit finish failed stream=" +
                      std::to_string(context.stream_id) + " error=" + ex.what();
    return;
  }

  if (context.append) {
    context.append->refresh_committed_from_disk();
  }
  actions.heartbeats.push_back(T_ReceiverHeartbeatAction{
      .line_id = context.chunk_commit_line_id,
      .heartbeat = T_Heartbeat{
          .stream_id = context.stream_id,
          .next_seq = context.chunk->expected_seq(),
          .file_id = context.chunk_commit_file_id,
          .offset = context.chunk_commit_final_size,
          .durable_offset = context.chunk_commit_final_size,
          .recv_window_bytes = recv_window_bytes_,
      },
      .flush = true,
      .only_if_line_open = true,
  });
  add_log(actions,
          "RECEIVER FILE_COMMIT complete line=" + std::to_string(context.chunk_commit_line_id) +
              " stream=" + std::to_string(context.stream_id) +
              " file_id=" + std::to_string(context.chunk_commit_file_id) +
              " final_path=" + path_string(context.chunk_commit_final_path) +
              " expected_seq=" + std::to_string(context.chunk->expected_seq()));
  clear_chunk_commit(context);
  actions.schedule_quiet_stop = true;
}

void T_ReceiverCoordinator::clear_chunk_commit(T_ReceiverStreamContext& context) {
  context.chunk_commit_queued = false;
  context.chunk_commit_line_id = 0;
  context.chunk_commit_seq = 0;
  context.chunk_commit_file_id = 0;
  context.chunk_commit_final_size = 0;
  context.chunk_commit_final_path.clear();
  context.chunk_commit_completion.reset();
}

}  // namespace yisync
