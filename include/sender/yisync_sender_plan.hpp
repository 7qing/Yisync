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

struct T_FileSendTask {
  std::uint64_t stream_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t file_id = 0;
  EM_EntryKind kind = EM_EntryKind::REGULAR_FILE;
  std::string name;
  std::string link_target;
  std::uint64_t source_size = 0;
  T_FileChecksum source_checksum;
  T_FileChecksum range_checksum;
  std::shared_ptr<const T_ISourceReader> reader;

  bool chunk_mode = false;
  bool preferred_chunk_mode = false;
  std::uint64_t chunk_count = 0;
  T_ChunkResendState chunk_resend;

  bool begin_sent = false;
  bool begin_ready = false;
  bool commit_sent = false;
  LineId begin_line_id = 0;
  LineId commit_line_id = 0;

  T_AppendSendState append;
};

struct T_StreamSendState {
  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::string entry_name_regex;
  std::shared_ptr<T_SourceDirectory> source_directory;
  T_Manifest1Stream source_manifest;
  std::vector<T_FileSendTask> tasks;
  std::size_t current_task = 0;
  bool manifest_applied = false;
  bool complete = false;
  bool has_pending_changes = false;
};

struct T_Manifest2ApplyResult {
  bool in_sync = false;
  bool should_start_append = false;
  bool should_start_chunk = false;
  T_SyncStart start;
};

void initialize_file_task(T_FileSendTask& task,
                          bool chunk_mode,
                          std::uint64_t chunk_size,
                          const std::vector<std::uint64_t>& chunk_order);
T_FileSendTask make_real_file_task(std::uint64_t stream_id,
                                 const T_SourceFile& file,
                                 T_SourceDirectory& directory,
                                 std::shared_ptr<const T_ISourceReader> reader,
                                 std::uint64_t chunk_size,
                                 const std::vector<std::uint64_t>& chunk_order);
T_FileSendTask make_simulated_file_task(std::uint64_t stream_id,
                                      std::uint64_t seq,
                                      std::uint64_t file_id,
                                      Bytes data,
                                      std::shared_ptr<const T_ISourceReader> reader,
                                      std::uint64_t chunk_size,
                                      const std::vector<std::uint64_t>& chunk_order);
T_Manifest1 manifest1_from_streams(std::uint64_t manifest_id,
                                 const std::vector<T_StreamSendState>& streams);
std::optional<T_Manifest2ApplyResult> apply_manifest2_to_stream(const T_Manifest2& manifest2,
                                                              T_StreamSendState& stream);

T_FileSendTask* active_task(T_StreamSendState& stream) noexcept;
const T_FileSendTask* active_task(const T_StreamSendState& stream) noexcept;
bool all_chunks_acked(const T_FileSendTask& task);
std::optional<std::uint64_t> next_unsent_chunk_index(const T_FileSendTask& task,
                                                     std::uint64_t current_tick,
                                                     std::uint64_t retransmit_ticks);
Bytes read_task_range(const T_FileSendTask& task, std::uint64_t offset, std::uint64_t len);
Bytes read_chunk_payload(const T_FileSendTask& task,
                         std::uint64_t chunk_index,
                         std::uint64_t chunk_size);

}  // namespace yisync
