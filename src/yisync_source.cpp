#include "yisync_source.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace yisync {
namespace {

constexpr std::size_t kChecksumBufferBytes = 4 * 1024 * 1024;

Bytes crc32c_value_bytes(std::uint32_t value) {
  return Bytes{
      static_cast<std::byte>(value & 0xff),
      static_cast<std::byte>((value >> 8) & 0xff),
      static_cast<std::byte>((value >> 16) & 0xff),
      static_cast<std::byte>((value >> 24) & 0xff),
  };
}

struct WatchSnapshot {
  std::uint64_t size = 0;
  std::filesystem::file_time_type mtime{};
};

class PollingSourceWatcher final : public ISourceWatcher {
 public:
  explicit PollingSourceWatcher(std::filesystem::path root)
      : root_(std::move(root)) {
    snapshot_ = scan();
  }

  std::vector<WatchEvent> poll() override {
    const auto next = scan();
    std::vector<WatchEvent> events;
    for (const auto& [path, current] : next) {
      const auto it = snapshot_.find(path);
      if (it == snapshot_.end()) {
        events.push_back(WatchEvent{WatchEventKind::Created, path});
        continue;
      }
      if (current.size > it->second.size) {
        events.push_back(WatchEvent{WatchEventKind::Appended, path});
      } else if (current.size != it->second.size || current.mtime != it->second.mtime) {
        events.push_back(WatchEvent{WatchEventKind::Modified, path});
      }
    }
    for (const auto& [path, previous] : snapshot_) {
      (void)previous;
      if (!next.contains(path)) {
        events.push_back(WatchEvent{WatchEventKind::Removed, path});
      }
    }
    snapshot_ = next;
    return events;
  }

 private:
  std::unordered_map<std::filesystem::path, WatchSnapshot> scan() const {
    std::unordered_map<std::filesystem::path, WatchSnapshot> result;
    std::error_code ec;
    if (!std::filesystem::exists(root_, ec)) {
      return result;
    }
    const auto options = std::filesystem::directory_options::skip_permission_denied;
    for (std::filesystem::recursive_directory_iterator it(root_, options, ec), end;
         it != end;
         it.increment(ec)) {
      if (ec) {
        ec.clear();
        continue;
      }
      const auto status = it->symlink_status(ec);
      if (ec) {
        ec.clear();
        continue;
      }
      if (std::filesystem::is_symlink(status)) {
        it.disable_recursion_pending();
        continue;
      }
      if (!std::filesystem::is_regular_file(status)) {
        continue;
      }
      result.emplace(it->path(),
                     WatchSnapshot{
                         .size = it->file_size(),
                         .mtime = it->last_write_time(),
                     });
    }
    return result;
  }

  std::filesystem::path root_;
  std::unordered_map<std::filesystem::path, WatchSnapshot> snapshot_;
};

}  // namespace

SourceDirectory::SourceDirectory(std::uint64_t stream_id,
                                 std::filesystem::path root,
                                 std::uint64_t checksum_range_len)
    : stream_id_(stream_id),
      root_(std::move(root)),
      checksum_range_len_(checksum_range_len) {}

const std::filesystem::path& SourceDirectory::root() const noexcept {
  return root_;
}

ManifestStream SourceDirectory::scan_manifest() const {
  return scan_manifest_stream(stream_id_, root_, checksum_range_len_);
}

std::vector<SourceFile> SourceDirectory::files() const {
  const auto manifest = scan_manifest();
  std::vector<SourceFile> result;
  result.reserve(manifest.entries.size());
  for (const auto& entry : manifest.entries) {
    SourceFile file;
    file.file_id = entry.file_id;
    file.path = root_ / entry.name;
    file.manifest = entry;
    result.push_back(std::move(file));
  }
  return result;
}

SourceFile SourceDirectory::file(std::uint64_t file_id) const {
  const auto manifest = scan_manifest();
  const auto it = std::find_if(manifest.entries.begin(), manifest.entries.end(), [file_id](const auto& entry) {
    return entry.file_id == file_id;
  });
  if (it == manifest.entries.end()) {
    throw std::runtime_error("source manifest entry does not exist for file_id: " + std::to_string(file_id));
  }
  if (it->kind != EntryKind::RegularFile) {
    throw std::runtime_error("source manifest entry is not a regular file: " + it->name);
  }
  const auto path = root_ / it->name;
  if (!std::filesystem::is_regular_file(path)) {
    throw std::runtime_error("source file does not exist: " + path.string());
  }
  SourceFile file;
  file.file_id = file_id;
  file.path = path;
  file.manifest = *it;
  return file;
}

Bytes SourceDirectory::read_range(std::uint64_t file_id, std::uint64_t offset, std::uint64_t len) const {
  const auto source_file = file(file_id);
  if (offset > source_file.manifest.size || len > source_file.manifest.size ||
      offset + len > source_file.manifest.size) {
    throw std::runtime_error("source read range is outside file");
  }

  std::ifstream input(source_file.path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("failed to open source file: " + source_file.path.string());
  }
  input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  Bytes bytes(static_cast<std::size_t>(len));
  if (len > 0) {
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (static_cast<std::uint64_t>(input.gcount()) != len) {
      throw std::runtime_error("failed to read source file range: " + source_file.path.string());
    }
  }
  return bytes;
}

FileChecksum SourceDirectory::full_checksum(std::uint64_t file_id) const {
  return full_checksum(file(file_id));
}

FileChecksum SourceDirectory::full_checksum(const SourceFile& file) const {
  FileChecksum checksum;
  checksum.algo = file.manifest.size == 0 ? ChecksumAlgo::None : ChecksumAlgo::Crc32c;
  checksum.offset = 0;
  checksum.len = file.manifest.size;
  if (file.manifest.size > 0) {
    std::ifstream input(file.path, std::ios::binary);
    if (!input) {
      throw std::runtime_error("failed to open source file for full checksum: " + file.path.string());
    }
    std::vector<char> buffer(kChecksumBufferBytes);
    std::uint32_t crc = 0;
    while (input) {
      input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      const auto read = input.gcount();
      if (read > 0) {
        crc = crc32c::Extend(crc,
                             reinterpret_cast<const std::uint8_t*>(buffer.data()),
                             static_cast<std::size_t>(read));
      }
    }
    if (!input.eof()) {
      throw std::runtime_error("failed to read source file for full checksum: " + file.path.string());
    }
    checksum.value = crc32c_value_bytes(crc);
  }
  return checksum;
}

std::unique_ptr<ISourceWatcher> make_source_watcher(const std::filesystem::path& root,
                                                    WatchBackend backend) {
  switch (backend) {
    case WatchBackend::Auto:
    case WatchBackend::Polling:
      return std::make_unique<PollingSourceWatcher>(root);
    case WatchBackend::Inotify:
    case WatchBackend::Fsevents:
      return std::make_unique<PollingSourceWatcher>(root);
  }
  return std::make_unique<PollingSourceWatcher>(root);
}

}  // namespace yisync
