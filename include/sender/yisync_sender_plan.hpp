#pragma once

#include "core/yisync_protocol.hpp"
#include "core/yisync_sync.hpp"
#include "network/yisync_scheduler.hpp"
#include "sender/yisync_append_state.hpp"
#include "sender/yisync_chunk_resend.hpp"
#include "sender/yisync_source.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yisync {

struct FileSendTask {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  EntryKind kind = EntryKind::RegularFile;
  std::string name;
  std::string link_target;
  std::uint64_t source_size = 0;
  FileChecksum source_checksum;
  FileChecksum range_checksum;
  std::shared_ptr<const ISourceReader> reader;

  bool chunk_mode = false;
  bool preferred_chunk_mode = false;
  std::uint64_t chunk_count = 0;
  ChunkResendState chunk_resend;

  bool begin_sent = false;
  bool begin_ready = false;
  bool commit_sent = false;
  LineId begin_line_id = 0;
  LineId commit_line_id = 0;

  AppendSendState append;
};

struct StreamSendState {
  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::string entry_name_regex;
  std::shared_ptr<SourceDirectory> source_directory;
  Manifest1Stream source_manifest;
  std::vector<FileSendTask> tasks;
  std::size_t current_task = 0;
  bool manifest_applied = false;
  bool complete = false;
  bool has_pending_changes = false;
};

struct Manifest2ApplyResult {
  bool in_sync = false;
  bool should_start_append = false;
  bool should_start_chunk = false;
  SyncStart start;
};

void initialize_file_task(FileSendTask& task,
                          bool chunk_mode,
                          std::uint64_t chunk_size,
                          const std::vector<std::uint64_t>& chunk_order);
FileSendTask make_real_file_task(std::uint64_t stream_id,
                                 const SourceFile& file,
                                 SourceDirectory& directory,
                                 std::shared_ptr<const ISourceReader> reader,
                                 std::uint64_t chunk_size,
                                 const std::vector<std::uint64_t>& chunk_order);
FileSendTask make_simulated_file_task(std::uint64_t stream_id,
                                      std::uint64_t seq,
                                      std::uint64_t file_id,
                                      Bytes data,
                                      std::shared_ptr<const ISourceReader> reader,
                                      std::uint64_t chunk_size,
                                      const std::vector<std::uint64_t>& chunk_order);
Manifest1 manifest1_from_streams(std::uint64_t manifest_id,
                                 const std::vector<StreamSendState>& streams);
std::optional<Manifest2ApplyResult> apply_manifest2_to_stream(const Manifest2& manifest2,
                                                              StreamSendState& stream);

FileSendTask* active_task(StreamSendState& stream) noexcept;
const FileSendTask* active_task(const StreamSendState& stream) noexcept;
bool all_chunks_acked(const FileSendTask& task);
std::optional<std::uint64_t> next_unsent_chunk_index(const FileSendTask& task,
                                                     std::uint64_t current_tick,
                                                     std::uint64_t retransmit_ticks);
Bytes read_task_range(const FileSendTask& task, std::uint64_t offset, std::uint64_t len);
Bytes read_chunk_payload(const FileSendTask& task,
                         std::uint64_t chunk_index,
                         std::uint64_t chunk_size);

}  // namespace yisync
