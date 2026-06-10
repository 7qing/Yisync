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

enum class StartAction : std::uint8_t {
  ResumeExisting = 1,
  CreateMissing = 2,
};

struct SyncStart {
  std::uint64_t stream_id = 0;
  std::uint64_t start_file_id = 0;
  std::uint64_t start_offset = 0;
  StartAction start_action = StartAction::ResumeExisting;
};

bool checksum_matches(const FileChecksum& checksum,
                      const std::filesystem::path& file_path);
FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len);

std::string file_name_for_id(std::uint64_t file_id);
std::optional<std::uint64_t> parse_file_id(std::string_view name);

Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len);
Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs);
Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs,
                                      std::string_view entry_name_regex);
std::optional<SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<ManifestEntry>& sender,
                                     const std::vector<ManifestEntry>& receiver);
Manifest2Stream make_manifest2_stream(std::uint64_t stream_id,
                                      const std::optional<SyncStart>& start);

bool should_use_chunk_mode(std::uint64_t file_size) noexcept;
std::uint64_t chunk_count_for_size(std::uint64_t file_size,
                                   std::uint64_t chunk_size = kDefaultChunkSizeBytes);

}  // namespace yisync
