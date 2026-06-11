#pragma once

#include "core/yisync_protocol.hpp"
#include "core/yisync_sync.hpp"

#include <cstdint>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yisync {

struct T_SourceFile {
  std::uint64_t file_id = 0;
  std::filesystem::path path;
  T_ManifestEntry manifest;
};

class T_ISourceReader {
 public:
  virtual ~T_ISourceReader() = default;
  virtual Bytes read_range(std::uint64_t file_id, std::uint64_t offset, std::uint64_t len) const = 0;
};

class T_SourceDirectory : public T_ISourceReader {
 public:
  T_SourceDirectory(std::uint64_t stream_id,
                  std::filesystem::path root,
                  std::uint64_t checksum_range_len,
                  std::string entry_name_regex = {});

  const std::filesystem::path& root() const noexcept;
  T_Manifest1Stream scan_manifest() const;
  std::vector<T_SourceFile> files() const;
  T_SourceFile file(std::uint64_t file_id) const;
  Bytes read_range(std::uint64_t file_id, std::uint64_t offset, std::uint64_t len) const override;
  T_FileChecksum full_checksum(std::uint64_t file_id) const;
  T_FileChecksum full_checksum(const T_SourceFile& file) const;

 private:
  std::uint64_t stream_id_ = 0;
  std::filesystem::path root_;
  std::uint64_t checksum_range_len_ = 0;
  std::string entry_name_regex_;
};

class T_SimulatedSourceReader final : public T_ISourceReader {
 public:
  explicit T_SimulatedSourceReader(Bytes data);

  const Bytes& data() const noexcept;
  Bytes read_range(std::uint64_t file_id, std::uint64_t offset, std::uint64_t len) const override;

 private:
  Bytes data_;
};

enum class EM_WatchEventKind : std::uint8_t {
  CREATED = 1,
  APPENDED = 2,
  MODIFIED = 3,
  REMOVED = 4,
  WATCH_OVERFLOW = 5,
};

struct T_WatchEvent {
  EM_WatchEventKind kind = EM_WatchEventKind::MODIFIED;
  std::filesystem::path path;
};

class T_ISourceWatcher {
 public:
  virtual ~T_ISourceWatcher() = default;
  virtual std::vector<T_WatchEvent> poll() = 0;
};

enum class EM_WatchBackend : std::uint8_t {
  AUTO = 0,
  POLLING = 1,
  INOTIFY = 2,
  FSEVENTS = 3,
};

std::unique_ptr<T_ISourceWatcher> make_source_watcher(const std::filesystem::path& root,
                                                    EM_WatchBackend backend = EM_WatchBackend::AUTO);
std::string_view watch_backend_name(EM_WatchBackend backend) noexcept;

}  // namespace yisync
