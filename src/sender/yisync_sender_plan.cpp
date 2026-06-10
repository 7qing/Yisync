#include "sender/yisync_sender_plan.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace yisync {

namespace {

FileChecksum full_crc32c_checksum_for_bytes(const Bytes& bytes) {
  return FileChecksum{
      .algo = ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(bytes.size()),
      .value = crc32c_bytes(bytes),
  };
}

}  // namespace

void initialize_file_task(FileSendTask& task,
                          bool chunk_mode,
                          std::uint64_t chunk_size,
                          const std::vector<std::uint64_t>& chunk_order) {
  task.chunk_mode = chunk_mode;
  task.preferred_chunk_mode = chunk_mode;
  task.chunk_count = chunk_mode ? chunk_count_for_size(task.source_size, chunk_size) : 0;
  initialize_chunk_resend_state(task.chunk_resend, task.chunk_count, chunk_order);
}

FileSendTask make_real_file_task(std::uint64_t stream_id,
                                 const SourceFile& file,
                                 SourceDirectory& directory,
                                 std::shared_ptr<const ISourceReader> reader,
                                 std::uint64_t chunk_size,
                                 const std::vector<std::uint64_t>& chunk_order) {
  FileSendTask task;
  task.stream_id = stream_id;
  task.seq = file.manifest.seq == 0 ? file.file_id : file.manifest.seq;
  task.file_id = file.file_id;
  task.kind = file.manifest.kind;
  task.name = file.manifest.name;
  task.link_target = file.manifest.link_target;
  task.source_size = file.manifest.kind == EntryKind::RegularFile ? file.manifest.size : 0;
  task.source_checksum = file.manifest.kind == EntryKind::RegularFile
                             ? directory.full_checksum(file)
                             : FileChecksum{};
  task.range_checksum = file.manifest.checksum;
  task.reader = std::move(reader);
  initialize_file_task(task,
                       file.manifest.kind == EntryKind::RegularFile &&
                           should_use_chunk_mode(task.source_size),
                       chunk_size,
                       chunk_order);
  return task;
}

FileSendTask make_simulated_file_task(std::uint64_t stream_id,
                                      std::uint64_t seq,
                                      std::uint64_t file_id,
                                      Bytes data,
                                      std::shared_ptr<const ISourceReader> reader,
                                      std::uint64_t chunk_size,
                                      const std::vector<std::uint64_t>& chunk_order) {
  const auto checksum = full_crc32c_checksum_for_bytes(data);
  FileSendTask task;
  task.stream_id = stream_id;
  task.seq = seq;
  task.file_id = file_id;
  task.kind = EntryKind::RegularFile;
  task.name = file_name_for_id(file_id);
  task.source_size = static_cast<std::uint64_t>(data.size());
  task.source_checksum = checksum;
  task.range_checksum = checksum;
  task.reader = std::move(reader);
  initialize_file_task(task, should_use_chunk_mode(task.source_size), chunk_size, chunk_order);
  return task;
}

Manifest1 manifest1_from_streams(std::uint64_t manifest_id,
                                 const std::vector<StreamSendState>& streams) {
  Manifest1 manifest;
  manifest.manifest_id = manifest_id;
  manifest.streams.reserve(streams.size());
  for (const auto& stream : streams) {
    manifest.streams.push_back(stream.source_manifest);
  }
  return manifest;
}

std::optional<Manifest2ApplyResult> apply_manifest2_to_stream(const Manifest2& manifest2,
                                                              StreamSendState& stream) {
  if (stream.manifest_applied || stream.complete) {
    return std::nullopt;
  }

  const auto plan_it = std::find_if(manifest2.streams.begin(),
                                    manifest2.streams.end(),
                                    [&](const auto& candidate) {
                                      return candidate.stream_id == stream.stream_id;
                                    });
  if (plan_it == manifest2.streams.end()) {
    throw std::runtime_error("Manifest2 missing stream=" + std::to_string(stream.stream_id));
  }
  if (plan_it->action == Manifest2Action::InSync) {
    stream.current_task = stream.tasks.size();
    stream.manifest_applied = true;
    stream.complete = true;
    stream.has_pending_changes = false;
    return Manifest2ApplyResult{.in_sync = true};
  }

  auto it = std::find_if(stream.tasks.begin(), stream.tasks.end(), [&](const auto& task) {
    return task.file_id == plan_it->start_file_id;
  });
  if (it == stream.tasks.end()) {
    throw std::runtime_error("Manifest2 start file not found stream=" +
                             std::to_string(stream.stream_id) +
                             " file_id=" + std::to_string(plan_it->start_file_id));
  }
  stream.current_task = static_cast<std::size_t>(std::distance(stream.tasks.begin(), it));
  stream.manifest_applied = true;

  auto* task = active_task(stream);
  if (task == nullptr) {
    stream.complete = true;
    return Manifest2ApplyResult{.in_sync = true};
  }

  SyncStart start{
      .stream_id = stream.stream_id,
      .start_file_id = plan_it->start_file_id,
      .start_offset = plan_it->start_offset,
      .start_action = plan_it->action == Manifest2Action::ResumeExisting
                          ? StartAction::ResumeExisting
                          : StartAction::CreateMissing,
  };
  if (start.start_action == StartAction::ResumeExisting) {
    task->chunk_mode = false;
  }
  return Manifest2ApplyResult{
      .in_sync = false,
      .should_start_append = start.start_action == StartAction::ResumeExisting || !task->chunk_mode,
      .should_start_chunk = start.start_action != StartAction::ResumeExisting && task->chunk_mode,
      .start = start,
  };
}

FileSendTask* active_task(StreamSendState& stream) noexcept {
  if (stream.current_task >= stream.tasks.size()) {
    return nullptr;
  }
  return &stream.tasks[stream.current_task];
}

const FileSendTask* active_task(const StreamSendState& stream) noexcept {
  if (stream.current_task >= stream.tasks.size()) {
    return nullptr;
  }
  return &stream.tasks[stream.current_task];
}

bool all_chunks_acked(const FileSendTask& task) {
  return all_chunks_acked(task.chunk_resend);
}

std::optional<std::uint64_t> next_unsent_chunk_index(const FileSendTask& task,
                                                     std::uint64_t current_tick,
                                                     std::uint64_t retransmit_ticks) {
  return next_chunk_to_send(task.chunk_resend, current_tick, retransmit_ticks);
}

Bytes read_task_range(const FileSendTask& task, std::uint64_t offset, std::uint64_t len) {
  if (!task.reader) {
    throw std::runtime_error("send task has no source reader");
  }
  return task.reader->read_range(task.file_id, offset, len);
}

Bytes read_chunk_payload(const FileSendTask& task,
                         std::uint64_t chunk_index,
                         std::uint64_t chunk_size) {
  const auto offset = chunk_index * chunk_size;
  const auto len = std::min<std::uint64_t>(chunk_size, task.source_size - offset);
  return read_task_range(task, offset, len);
}

}  // namespace yisync
