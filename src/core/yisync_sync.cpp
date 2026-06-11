#include "core/yisync_sync.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <charconv>
#include <fstream>
#include <regex>
#include <stdexcept>
#include <system_error>
#include <unordered_set>
#include <utility>

namespace yisync {
namespace {

constexpr std::size_t kChecksumBufferBytes = 4 * 1024 * 1024;

std::uint64_t file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return 0;
  }
  return size;
}

bool bytes_equal(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

Bytes crc32c_value_bytes(std::uint32_t value) {
  return Bytes{
      static_cast<std::byte>(value & 0xff),
      static_cast<std::byte>((value >> 8) & 0xff),
      static_cast<std::byte>((value >> 16) & 0xff),
      static_cast<std::byte>((value >> 24) & 0xff),
  };
}

Bytes crc32c_file_range(const std::filesystem::path& path,
                        std::uint64_t offset,
                        std::uint64_t len) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("failed to open file for checksum: " + path.string());
  }
  input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  if (!input) {
    throw std::runtime_error("failed to seek file for checksum: " + path.string());
  }

  std::vector<char> buffer(kChecksumBufferBytes);
  std::uint64_t remaining = len;
  std::uint32_t crc = 0;
  while (remaining > 0) {
    const auto wanted = std::min<std::uint64_t>(remaining, buffer.size());
    input.read(buffer.data(), static_cast<std::streamsize>(wanted));
    const auto read = input.gcount();
    if (read <= 0) {
      throw std::runtime_error("failed to read requested checksum range: " + path.string());
    }
    crc = crc32c::Extend(crc,
                         reinterpret_cast<const std::uint8_t*>(buffer.data()),
                         static_cast<std::size_t>(read));
    remaining -= static_cast<std::uint64_t>(read);
  }
  return crc32c_value_bytes(crc);
}

bool checksums_equal(const T_FileChecksum& lhs, const T_FileChecksum& rhs) {
  return lhs.algo == rhs.algo &&
         lhs.offset == rhs.offset &&
         lhs.len == rhs.len &&
         bytes_equal(lhs.value, rhs.value);
}

std::string generic_relative_name(const std::filesystem::path& root,
                                  const std::filesystem::path& path) {
  return path.lexically_relative(root).generic_string();
}

bool is_yisync_internal_path(std::string_view relative_name) {
  return relative_name == ".yisync_tmp" || relative_name.starts_with(".yisync_tmp/");
}

std::uint64_t stable_path_id(std::string_view relative_name) {
  std::uint64_t hash = 1469598103934665603ULL;
  for (const auto ch : relative_name) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= 1099511628211ULL;
  }
  return (std::uint64_t{1} << 63) | (hash & ~(std::uint64_t{1} << 63));
}

bool top_level_dir_excluded(const std::filesystem::path& path,
                            const std::unordered_set<std::string>& excluded) {
  if (excluded.empty()) {
    return false;
  }
  const auto name = path.filename().string();
  return excluded.find(name) != excluded.end();
}

bool manifest_entries_same_identity(const T_ManifestEntry& lhs, const T_ManifestEntry& rhs) {
  return lhs.file_id == rhs.file_id &&
         lhs.kind == rhs.kind &&
         lhs.name == rhs.name;
}

bool manifest_entries_same_content(const T_ManifestEntry& sender, const T_ManifestEntry& receiver) {
  if (sender.kind != receiver.kind) {
    return false;
  }
  if (sender.kind == EM_EntryKind::DIRECTORY) {
    return true;
  }
  if (sender.kind == EM_EntryKind::SYMLINK) {
    return sender.link_target == receiver.link_target;
  }
  return sender.size == receiver.size && checksums_equal(sender.checksum, receiver.checksum);
}

}  // namespace

bool checksum_matches(const T_FileChecksum& checksum, const std::filesystem::path& file_path) {
  if (checksum.algo == EM_ChecksumAlgo::NONE) {
    return checksum.len == 0;
  }
  if (checksum.algo != EM_ChecksumAlgo::CRC32C) {
    throw std::runtime_error("only CRC32C file checksums are implemented in this prototype");
  }
  const auto size = file_size_or_zero(file_path);
  if (checksum.offset > size || checksum.len > size || checksum.offset + checksum.len > size) {
    return false;
  }
  return bytes_equal(crc32c_file_range(file_path, checksum.offset, checksum.len), checksum.value);
}

T_FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len) {
  const auto size = file_size_or_zero(file_path);
  T_FileChecksum checksum;
  checksum.algo = size == 0 ? EM_ChecksumAlgo::NONE : EM_ChecksumAlgo::CRC32C;
  checksum.len = std::min(size, max_len);
  checksum.offset = size - checksum.len;
  if (checksum.len > 0) {
    checksum.value = crc32c_file_range(file_path, checksum.offset, checksum.len);
  }
  return checksum;
}

std::string file_name_for_id(std::uint64_t file_id) {
  return std::to_string(file_id) + ".file";
}

std::optional<std::uint64_t> parse_file_id(std::string_view name) {
  constexpr std::string_view suffix = ".file";
  if (name.size() <= suffix.size() || name.substr(name.size() - suffix.size()) != suffix) {
    return std::nullopt;
  }
  const auto number = name.substr(0, name.size() - suffix.size());
  std::uint64_t file_id = 0;
  const auto* begin = number.data();
  const auto* end = number.data() + number.size();
  const auto result = std::from_chars(begin, end, file_id);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return file_id;
}

T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len) {
  return scan_manifest_stream(stream_id, root, checksum_range_len, {});
}

T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs) {
  return scan_manifest_stream(stream_id, root, checksum_range_len, excluded_top_level_dirs, {});
}

T_Manifest1Stream scan_manifest_stream(std::uint64_t stream_id,
                                      const std::filesystem::path& root,
                                      std::uint64_t checksum_range_len,
                                      const std::vector<std::string>& excluded_top_level_dirs,
                                      std::string_view entry_name_regex) {
  T_Manifest1Stream stream;
  stream.stream_id = stream_id;
  stream.root = root.string();

  std::error_code ec;
  if (!std::filesystem::exists(root, ec)) {
    return stream;
  }

  std::unordered_set<std::string> excluded_dirs(excluded_top_level_dirs.begin(),
                                               excluded_top_level_dirs.end());
  std::optional<std::regex> entry_filter;
  if (!entry_name_regex.empty()) {
    try {
      entry_filter.emplace(std::string(entry_name_regex), std::regex::extended);
    } catch (const std::regex_error& ex) {
      throw std::runtime_error("bad entry_name_regex for " + root.string() + ": " + ex.what());
    }
  }
  std::vector<T_ManifestEntry> entries;
  const auto options = std::filesystem::directory_options::skip_permission_denied;
  for (std::filesystem::recursive_directory_iterator it(root, options, ec), end;
       it != end;
       it.increment(ec)) {
    if (ec) {
      ec.clear();
      continue;
    }

    const auto relative_name = generic_relative_name(root, it->path());
    if (is_yisync_internal_path(relative_name)) {
      if (it->is_directory(ec)) {
        it.disable_recursion_pending();
      }
      ec.clear();
      continue;
    }

    if (it.depth() == 0 && top_level_dir_excluded(it->path(), excluded_dirs)) {
      it.disable_recursion_pending();
      continue;
    }

    const auto status = it->symlink_status(ec);
    if (ec) {
      ec.clear();
      continue;
    }

    T_ManifestEntry manifest_entry;
    manifest_entry.name = relative_name;
    if (std::filesystem::is_symlink(status)) {
      if (entry_filter.has_value() && !std::regex_search(relative_name, *entry_filter)) {
        it.disable_recursion_pending();
        continue;
      }
      it.disable_recursion_pending();
      manifest_entry.kind = EM_EntryKind::SYMLINK;
      manifest_entry.link_target = std::filesystem::read_symlink(it->path(), ec).generic_string();
      if (ec) {
        ec.clear();
        continue;
      }
      manifest_entry.size = 0;
      manifest_entry.checksum = T_FileChecksum{};
    } else if (std::filesystem::is_directory(status)) {
      manifest_entry.kind = EM_EntryKind::DIRECTORY;
      manifest_entry.size = 0;
      manifest_entry.checksum = T_FileChecksum{};
    } else if (std::filesystem::is_regular_file(status)) {
      if (entry_filter.has_value() && !std::regex_search(relative_name, *entry_filter)) {
        continue;
      }
      manifest_entry.kind = EM_EntryKind::REGULAR_FILE;
      manifest_entry.size = it->file_size(ec);
      if (ec) {
        ec.clear();
        continue;
      }
      manifest_entry.checksum = make_crc32c_range_checksum(it->path(), checksum_range_len);
    } else {
      continue;
    }
    const auto parsed_id = parse_file_id(std::filesystem::path(relative_name).filename().string());
    const auto top_level_regular_numeric =
        manifest_entry.kind == EM_EntryKind::REGULAR_FILE &&
        std::filesystem::path(relative_name).parent_path().empty() &&
        parsed_id.has_value();
    manifest_entry.file_id = top_level_regular_numeric ? *parsed_id : stable_path_id(relative_name);
    entries.push_back(std::move(manifest_entry));
  }

  std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
    const auto lhs_legacy_numeric = lhs.kind == EM_EntryKind::REGULAR_FILE &&
                                    lhs.file_id < (std::uint64_t{1} << 63) &&
                                    std::filesystem::path(lhs.name).parent_path().empty();
    const auto rhs_legacy_numeric = rhs.kind == EM_EntryKind::REGULAR_FILE &&
                                    rhs.file_id < (std::uint64_t{1} << 63) &&
                                    std::filesystem::path(rhs.name).parent_path().empty();
    if (lhs_legacy_numeric && rhs_legacy_numeric) {
      return lhs.file_id < rhs.file_id;
    }
    if (lhs_legacy_numeric != rhs_legacy_numeric) {
      return lhs_legacy_numeric;
    }
    return lhs.name < rhs.name;
  });
  for (std::uint64_t i = 0; i < entries.size(); ++i) {
    entries[static_cast<std::size_t>(i)].seq = i + 1;
  }
  std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.seq != rhs.seq) {
      return lhs.seq < rhs.seq;
    }
    return lhs.file_id < rhs.file_id;
  });
  stream.entries = std::move(entries);
  return stream;
}

std::optional<T_SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<T_ManifestEntry>& sender,
                                     const std::vector<T_ManifestEntry>& receiver) {
  std::size_t sender_i = 0;
  std::size_t receiver_i = 0;

  while (sender_i < sender.size()) {
    const auto& sender_entry = sender[sender_i];
    if (receiver_i >= receiver.size()) {
      return T_SyncStart{stream_id, sender_entry.file_id, 0, EM_StartAction::CREATE_MISSING};
    }

    const auto& receiver_entry = receiver[receiver_i];
    if (!manifest_entries_same_identity(sender_entry, receiver_entry)) {
      throw std::runtime_error("receiver entry identity conflicts with sender for " + sender_entry.name);
    }
    if (sender_entry.kind == EM_EntryKind::DIRECTORY || sender_entry.kind == EM_EntryKind::SYMLINK) {
      if (!manifest_entries_same_content(sender_entry, receiver_entry)) {
        throw std::runtime_error("receiver entry content conflicts with sender for " + sender_entry.name);
      }
      ++sender_i;
      ++receiver_i;
      continue;
    }

    if (receiver_entry.size > sender_entry.size) {
      throw std::runtime_error("receiver file is larger than sender for " + sender_entry.name);
    }
    if (receiver_entry.size < sender_entry.size) {
      return T_SyncStart{stream_id, sender_entry.file_id, receiver_entry.size, EM_StartAction::RESUME_EXISTING};
    }
    if (!bytes_equal(sender_entry.checksum.value, receiver_entry.checksum.value) ||
        sender_entry.checksum.algo != receiver_entry.checksum.algo ||
        sender_entry.checksum.offset != receiver_entry.checksum.offset ||
        sender_entry.checksum.len != receiver_entry.checksum.len) {
      throw std::runtime_error("checksum mismatch for " + sender_entry.name);
    }

    ++sender_i;
    ++receiver_i;
  }
  return std::nullopt;
}

T_Manifest2Stream make_manifest2_stream(std::uint64_t stream_id,
                                       const std::optional<T_SyncStart>& start) {
  if (!start.has_value()) {
    return T_Manifest2Stream{
        .stream_id = stream_id,
        .action = EM_Manifest2Action::IN_SYNC,
        .start_file_id = 0,
        .start_offset = 0,
    };
  }
  return T_Manifest2Stream{
      .stream_id = stream_id,
      .action = start->start_action == EM_StartAction::RESUME_EXISTING
                    ? EM_Manifest2Action::RESUME_EXISTING
                    : EM_Manifest2Action::CREATE_MISSING,
      .start_file_id = start->start_file_id,
      .start_offset = start->start_offset,
  };
}

bool should_use_chunk_mode(std::uint64_t file_size) noexcept {
  return file_size > kChunkModeThresholdBytes;
}

std::uint64_t chunk_count_for_size(std::uint64_t file_size, std::uint64_t chunk_size) {
  if (chunk_size == 0) {
    throw std::invalid_argument("chunk_size must be non-zero");
  }
  return file_size == 0 ? 0 : ((file_size + chunk_size - 1) / chunk_size);
}

}  // namespace yisync
