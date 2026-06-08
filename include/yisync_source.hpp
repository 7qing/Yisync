#pragma once

#include "yisync_protocol.hpp"
#include "yisync_sync.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

namespace yisync {

struct SourceFile {
  std::uint64_t file_id = 0;
  std::filesystem::path path;
  ManifestEntry manifest;
};

class SourceDirectory {
 public:
  SourceDirectory(std::uint64_t stream_id,
                  std::filesystem::path root,
                  std::uint64_t checksum_range_len);

  const std::filesystem::path& root() const noexcept;
  ManifestStream scan_manifest() const;
  std::vector<SourceFile> files() const;
  SourceFile file(std::uint64_t file_id) const;
  Bytes read_range(std::uint64_t file_id, std::uint64_t offset, std::uint64_t len) const;
  FileChecksum full_checksum(std::uint64_t file_id) const;
  FileChecksum full_checksum(const SourceFile& file) const;

 private:
  std::uint64_t stream_id_ = 0;
  std::filesystem::path root_;
  std::uint64_t checksum_range_len_ = 0;
};

enum class WatchEventKind : std::uint8_t {
  Created = 1,
  Appended = 2,
  Modified = 3,
  Removed = 4,
};

struct WatchEvent {
  WatchEventKind kind = WatchEventKind::Modified;
  std::filesystem::path path;
};

class ISourceWatcher {
 public:
  virtual ~ISourceWatcher() = default;
  virtual std::vector<WatchEvent> poll() = 0;
};

enum class WatchBackend : std::uint8_t {
  Auto = 0,
  Polling = 1,
  Inotify = 2,
  Fsevents = 3,
};

std::unique_ptr<ISourceWatcher> make_source_watcher(const std::filesystem::path& root,
                                                    WatchBackend backend = WatchBackend::Auto);

}  // namespace yisync
