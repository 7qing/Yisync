#pragma once

#include "core/yisync_protocol.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yisync {

inline constexpr std::uint64_t kChunkModeThresholdBytes = 64 * 1024;

enum class EM_StartAction : std::uint8_t {
  RESUME_EXISTING = 1,
  CREATE_MISSING = 2,
};

struct T_SyncStart {
  std::uint64_t stream_id = 0;
  std::uint64_t start_file_id = 0;
  std::uint64_t start_offset = 0;
  EM_StartAction start_action = EM_StartAction::RESUME_EXISTING;
};

bool checksum_matches(const T_FileChecksum& checksum,
                      const std::filesystem::path& file_path);
T_FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len);

std::string file_name_for_id(std::uint64_t file_id);
std::optional<std::uint64_t> parse_file_id(std::string_view name);

T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len);
T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs);
T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs,
                                      std::string_view entry_name_regex);
std::optional<T_SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<T_ManifestEntry>& sender,
                                     const std::vector<T_ManifestEntry>& receiver);
T_Manifest2Stream make_manifest2_stream(std::uint64_t stream_id,
                                      const std::optional<T_SyncStart>& start);

bool should_use_chunk_mode(std::uint64_t file_size) noexcept;
std::uint64_t chunk_count_for_size(std::uint64_t file_size,
                                   std::uint64_t chunk_size = kDefaultChunkSizeBytes);

}  // namespace yisync
