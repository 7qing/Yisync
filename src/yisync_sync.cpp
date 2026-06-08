#include "yisync_sync.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <charconv>
#include <fstream>
#include <sstream>
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

std::vector<std::uint64_t> parse_received_chunks(std::string_view text) {
  std::vector<std::uint64_t> chunks;
  std::istringstream input(std::string{text});
  std::string token;
  while (std::getline(input, token, ',')) {
    if (token.empty()) {
      continue;
    }
    std::uint64_t value = 0;
    const auto* begin = token.data();
    const auto* end = token.data() + token.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec == std::errc{} && result.ptr == end) {
      chunks.push_back(value);
    }
  }
  std::sort(chunks.begin(), chunks.end());
  chunks.erase(std::unique(chunks.begin(), chunks.end()), chunks.end());
  return chunks;
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

bool checksums_equal(const FileChecksum& lhs, const FileChecksum& rhs) {
  return lhs.algo == rhs.algo &&
         lhs.offset == rhs.offset &&
         lhs.len == rhs.len &&
         bytes_equal(lhs.value, rhs.value);
}

bool checksum_scopes_equal(const FileChecksum& lhs, const FileChecksum& rhs) {
  return lhs.algo == rhs.algo &&
         lhs.offset == rhs.offset &&
         lhs.len == rhs.len;
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
  return excluded.contains(name);
}

bool manifest_entries_same_identity(const ManifestEntry& lhs, const ManifestEntry& rhs) {
  return lhs.file_id == rhs.file_id &&
         lhs.kind == rhs.kind &&
         lhs.name == rhs.name;
}

bool manifest_entries_same_content(const ManifestEntry& sender, const ManifestEntry& receiver) {
  if (sender.kind != receiver.kind) {
    return false;
  }
  if (sender.kind == EntryKind::Directory) {
    return true;
  }
  if (sender.kind == EntryKind::Symlink) {
    return sender.link_target == receiver.link_target;
  }
  return sender.size == receiver.size && checksums_equal(sender.checksum, receiver.checksum);
}

}  // namespace

bool checksum_matches(const FileChecksum& checksum, const std::filesystem::path& file_path) {
  if (checksum.algo == ChecksumAlgo::None) {
    return checksum.len == 0;
  }
  if (checksum.algo != ChecksumAlgo::Crc32c) {
    throw std::runtime_error("only CRC32C file checksums are implemented in this prototype");
  }
  const auto size = file_size_or_zero(file_path);
  if (checksum.offset > size || checksum.len > size || checksum.offset + checksum.len > size) {
    return false;
  }
  return bytes_equal(crc32c_file_range(file_path, checksum.offset, checksum.len), checksum.value);
}

FileChecksum make_crc32c_range_checksum(const std::filesystem::path& file_path,
                                        std::uint64_t max_len) {
  const auto size = file_size_or_zero(file_path);
  FileChecksum checksum;
  checksum.algo = size == 0 ? ChecksumAlgo::None : ChecksumAlgo::Crc32c;
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

ManifestStream scan_manifest_stream(std::uint64_t stream_id,
                                    const std::filesystem::path& root,
                                    std::uint64_t checksum_range_len) {
  return scan_manifest_stream(stream_id, root, checksum_range_len, {});
}

ManifestStream scan_manifest_stream(std::uint64_t stream_id,
                                    const std::filesystem::path& root,
                                    std::uint64_t checksum_range_len,
                                    const std::vector<std::string>& excluded_top_level_dirs) {
  ManifestStream stream;
  stream.stream_id = stream_id;
  stream.root = root.string();

  std::error_code ec;
  if (!std::filesystem::exists(root, ec)) {
    return stream;
  }

  std::unordered_set<std::string> excluded_dirs(excluded_top_level_dirs.begin(),
                                               excluded_top_level_dirs.end());
  std::vector<ManifestEntry> entries;
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

    ManifestEntry manifest_entry;
    manifest_entry.name = relative_name;
    if (std::filesystem::is_symlink(status)) {
      it.disable_recursion_pending();
      manifest_entry.kind = EntryKind::Symlink;
      manifest_entry.link_target = std::filesystem::read_symlink(it->path(), ec).generic_string();
      if (ec) {
        ec.clear();
        continue;
      }
      manifest_entry.size = 0;
      manifest_entry.checksum = FileChecksum{};
    } else if (std::filesystem::is_directory(status)) {
      manifest_entry.kind = EntryKind::Directory;
      manifest_entry.size = 0;
      manifest_entry.checksum = FileChecksum{};
    } else if (std::filesystem::is_regular_file(status)) {
      manifest_entry.kind = EntryKind::RegularFile;
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
        manifest_entry.kind == EntryKind::RegularFile &&
        std::filesystem::path(relative_name).parent_path().empty() &&
        parsed_id.has_value();
    manifest_entry.file_id = top_level_regular_numeric ? *parsed_id : stable_path_id(relative_name);
    entries.push_back(std::move(manifest_entry));
  }

  std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
    const auto lhs_legacy_numeric = lhs.kind == EntryKind::RegularFile &&
                                    lhs.file_id < (std::uint64_t{1} << 63) &&
                                    std::filesystem::path(lhs.name).parent_path().empty();
    const auto rhs_legacy_numeric = rhs.kind == EntryKind::RegularFile &&
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
    entries[static_cast<std::size_t>(i)].order_seq = i + 1;
  }
  std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.order_seq != rhs.order_seq) {
      return lhs.order_seq < rhs.order_seq;
    }
    return lhs.file_id < rhs.file_id;
  });
  stream.entries = std::move(entries);
  stream.incomplete_chunks = scan_incomplete_chunk_files(root);
  return stream;
}

std::vector<IncompleteChunkFile> scan_incomplete_chunk_files(const std::filesystem::path& root) {
  std::vector<IncompleteChunkFile> files;
  const auto temp_root = root / ".yisync_tmp";
  std::error_code ec;
  if (!std::filesystem::exists(temp_root, ec)) {
    return files;
  }

  for (const auto& entry : std::filesystem::directory_iterator(temp_root)) {
    if (!entry.is_regular_file() || entry.path().extension() != ".meta") {
      continue;
    }
    std::ifstream input(entry.path());
    if (!input) {
      continue;
    }

    IncompleteChunkFile file;
    std::string line;
    while (std::getline(input, line)) {
      const auto pos = line.find('=');
      if (pos == std::string::npos) {
        continue;
      }
      const auto key = std::string_view(line).substr(0, pos);
      const auto value = std::string_view(line).substr(pos + 1);
      const auto parse_u64 = [](std::string_view text) -> std::uint64_t {
        std::uint64_t result = 0;
        std::from_chars(text.data(), text.data() + text.size(), result);
        return result;
      };

      if (key == "order_seq") {
        file.order_seq = parse_u64(value);
      } else if (key == "file_id") {
        file.file_id = parse_u64(value);
      } else if (key == "name") {
        file.name = std::string(value);
      } else if (key == "final_size") {
        file.final_size = parse_u64(value);
      } else if (key == "chunk_size") {
        file.chunk_size = parse_u64(value);
      } else if (key == "chunk_count") {
        file.chunk_count = parse_u64(value);
      } else if (key == "file_checksum_algo") {
        file.file_checksum.algo = static_cast<ChecksumAlgo>(parse_u64(value));
      } else if (key == "file_checksum_offset") {
        file.file_checksum.offset = parse_u64(value);
      } else if (key == "file_checksum_len") {
        file.file_checksum.len = parse_u64(value);
      } else if (key == "file_checksum_crc32c") {
        const auto crc = static_cast<std::uint32_t>(parse_u64(value));
        file.file_checksum.value = Bytes{
            static_cast<std::byte>(crc & 0xff),
            static_cast<std::byte>((crc >> 8) & 0xff),
            static_cast<std::byte>((crc >> 16) & 0xff),
            static_cast<std::byte>((crc >> 24) & 0xff),
        };
      } else if (key == "prev_file_id") {
        file.prev_file_id = parse_u64(value);
      } else if (key == "prev_final_size") {
        file.prev_final_size = parse_u64(value);
      } else if (key == "prev_checksum_algo") {
        file.prev_checksum.algo = static_cast<ChecksumAlgo>(parse_u64(value));
      } else if (key == "prev_checksum_offset") {
        file.prev_checksum.offset = parse_u64(value);
      } else if (key == "prev_checksum_len") {
        file.prev_checksum.len = parse_u64(value);
      } else if (key == "prev_checksum_crc32c") {
        const auto crc = static_cast<std::uint32_t>(parse_u64(value));
        file.prev_checksum.value = Bytes{
            static_cast<std::byte>(crc & 0xff),
            static_cast<std::byte>((crc >> 8) & 0xff),
            static_cast<std::byte>((crc >> 16) & 0xff),
            static_cast<std::byte>((crc >> 24) & 0xff),
        };
      } else if (key == "received_chunks") {
        file.received_chunks = parse_received_chunks(value);
      }
    }

    const auto temp_path = temp_root / (std::to_string(file.order_seq) + "_" + file_name_for_id(file.file_id) + ".tmp");
    if (file.order_seq != 0 && file.file_id != 0 && file.chunk_count != 0 &&
        std::filesystem::exists(temp_path)) {
      file.received_chunks.erase(std::remove_if(file.received_chunks.begin(),
                                                file.received_chunks.end(),
                                                [&](std::uint64_t chunk_index) {
                                                  return chunk_index >= file.chunk_count;
                                                }),
                                  file.received_chunks.end());
      files.push_back(std::move(file));
    }
  }

  std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.order_seq != rhs.order_seq) {
      return lhs.order_seq < rhs.order_seq;
    }
    return lhs.file_id < rhs.file_id;
  });
  return files;
}

std::optional<SyncStart> diff_stream(std::uint64_t stream_id,
                                     const std::vector<ManifestEntry>& sender,
                                     const std::vector<ManifestEntry>& receiver) {
  std::size_t sender_i = 0;
  std::size_t receiver_i = 0;

  while (sender_i < sender.size()) {
    const auto& sender_entry = sender[sender_i];
    if (receiver_i >= receiver.size()) {
      return SyncStart{stream_id, sender_entry.file_id, 0, StartAction::CreateMissing};
    }

    const auto& receiver_entry = receiver[receiver_i];
    if (!manifest_entries_same_identity(sender_entry, receiver_entry)) {
      throw std::runtime_error("receiver entry identity conflicts with sender for " + sender_entry.name);
    }
    if (sender_entry.kind == EntryKind::Directory || sender_entry.kind == EntryKind::Symlink) {
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
      return SyncStart{stream_id, sender_entry.file_id, receiver_entry.size, StartAction::ResumeExisting};
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

ChunkResumePlan plan_chunk_resume_from_manifest(const Manifest& receiver_manifest,
                                                std::uint64_t stream_id,
                                                std::uint64_t order_seq,
                                                std::uint64_t file_id,
                                                std::uint64_t final_size,
                                                std::uint64_t chunk_size,
                                                std::uint64_t chunk_count,
                                                const FileChecksum& file_checksum) {
  ChunkResumePlan plan;
  const ManifestStream* stream = nullptr;
  for (const auto& candidate : receiver_manifest.streams) {
    if (candidate.stream_id == stream_id) {
      stream = &candidate;
      break;
    }
  }
  if (stream == nullptr) {
    return plan;
  }

  for (const auto& entry : stream->entries) {
    if (entry.file_id != file_id) {
      continue;
    }
    if (entry.size != final_size) {
      throw std::runtime_error("receiver complete file size conflicts with sender file");
    }
    if (checksum_scopes_equal(entry.checksum, file_checksum) &&
        !checksums_equal(entry.checksum, file_checksum)) {
      throw std::runtime_error("receiver complete file checksum conflicts with sender file");
    }
    plan.complete = true;
    return plan;
  }

  for (const auto& incomplete : stream->incomplete_chunks) {
    if (incomplete.order_seq != order_seq || incomplete.file_id != file_id) {
      continue;
    }
    if (incomplete.final_size != final_size) {
      throw std::runtime_error("receiver incomplete file size conflicts with sender file");
    }
    if (incomplete.chunk_size != chunk_size) {
      throw std::runtime_error("receiver incomplete chunk_size conflicts with sender file");
    }
    if (incomplete.chunk_count != chunk_count) {
      throw std::runtime_error("receiver incomplete chunk_count conflicts with sender file");
    }
    if (!checksums_equal(incomplete.file_checksum, file_checksum)) {
      throw std::runtime_error("receiver incomplete checksum conflicts with sender file");
    }
    plan.resume_incomplete = true;
    plan.checkpointed_chunks = incomplete.received_chunks;
    plan.checkpointed_chunks.erase(std::remove_if(plan.checkpointed_chunks.begin(),
                                                  plan.checkpointed_chunks.end(),
                                                  [&](std::uint64_t chunk_index) {
                                                    return chunk_index >= chunk_count;
                                                  }),
                                    plan.checkpointed_chunks.end());
    std::sort(plan.checkpointed_chunks.begin(), plan.checkpointed_chunks.end());
    plan.checkpointed_chunks.erase(std::unique(plan.checkpointed_chunks.begin(), plan.checkpointed_chunks.end()),
                                   plan.checkpointed_chunks.end());
    return plan;
  }

  return plan;
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
