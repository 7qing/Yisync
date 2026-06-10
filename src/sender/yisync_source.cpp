#include "sender/yisync_source.hpp"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/inotify.h>
#include <unistd.h>
#endif

#if defined(__APPLE__)
#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>
#endif

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

WatchSnapshot snapshot_for_regular_file(const std::filesystem::path& path) {
  std::error_code ec;
  return WatchSnapshot{
      .size = std::filesystem::file_size(path, ec),
      .mtime = std::filesystem::last_write_time(path, ec),
  };
}

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
      if (next.find(path) == next.end()) {
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

#if defined(__linux__)
class InotifySourceWatcher final : public ISourceWatcher {
 public:
  explicit InotifySourceWatcher(std::filesystem::path root)
      : root_(std::move(root)),
        fd_(::inotify_init1(IN_NONBLOCK | IN_CLOEXEC)) {
    if (fd_ < 0) {
      throw std::runtime_error("failed to create inotify fd: " + std::string(std::strerror(errno)));
    }
    add_recursive(root_);
  }

  ~InotifySourceWatcher() override {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  InotifySourceWatcher(const InotifySourceWatcher&) = delete;
  InotifySourceWatcher& operator=(const InotifySourceWatcher&) = delete;

  std::vector<WatchEvent> poll() override {
    std::vector<WatchEvent> events;
    std::array<char, 64 * 1024> buffer{};
    while (true) {
      const auto read = ::read(fd_, buffer.data(), buffer.size());
      if (read < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;
        }
        events.push_back(WatchEvent{WatchEventKind::Overflow, root_});
        break;
      }
      if (read == 0) {
        break;
      }

      std::size_t offset = 0;
      while (offset < static_cast<std::size_t>(read)) {
        const auto* event = reinterpret_cast<const inotify_event*>(buffer.data() + offset);
        handle_event(*event, events);
        offset += sizeof(inotify_event) + event->len;
      }
    }
    return events;
  }

 private:
  void add_recursive(const std::filesystem::path& path) {
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec || !std::filesystem::is_directory(status)) {
      return;
    }
    add_watch(path);
    const auto options = std::filesystem::directory_options::skip_permission_denied;
    for (std::filesystem::recursive_directory_iterator it(path, options, ec), end;
         it != end;
         it.increment(ec)) {
      if (ec) {
        ec.clear();
        continue;
      }
      const auto child_status = it->symlink_status(ec);
      if (ec) {
        ec.clear();
        continue;
      }
      if (std::filesystem::is_symlink(child_status)) {
        it.disable_recursion_pending();
        continue;
      }
      if (std::filesystem::is_directory(child_status)) {
        add_watch(it->path());
        continue;
      }
      if (std::filesystem::is_regular_file(child_status)) {
        snapshot_[it->path()] = snapshot_for_regular_file(it->path());
      }
    }
  }

  void add_watch(const std::filesystem::path& path) {
    const int wd = ::inotify_add_watch(fd_,
                                       path.c_str(),
                                       IN_CREATE | IN_CLOSE_WRITE | IN_MODIFY | IN_MOVED_TO |
                                           IN_MOVED_FROM | IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF);
    if (wd >= 0) {
      watches_[wd] = path;
    }
  }

  void handle_event(const inotify_event& event, std::vector<WatchEvent>& events) {
    if ((event.mask & IN_Q_OVERFLOW) != 0) {
      add_recursive(root_);
      events.push_back(WatchEvent{WatchEventKind::Overflow, root_});
      return;
    }

    const auto watch_it = watches_.find(event.wd);
    if (watch_it == watches_.end()) {
      return;
    }
    auto path = watch_it->second;
    if (event.len > 0 && event.name[0] != '\0') {
      path /= event.name;
    }

    if ((event.mask & (IN_DELETE | IN_MOVED_FROM | IN_DELETE_SELF | IN_MOVE_SELF)) != 0) {
      snapshot_.erase(path);
      events.push_back(WatchEvent{WatchEventKind::Removed, std::move(path)});
      return;
    }

    if ((event.mask & IN_ISDIR) != 0) {
      if ((event.mask & (IN_CREATE | IN_MOVED_TO)) != 0) {
        add_recursive(path);
        events.push_back(WatchEvent{WatchEventKind::Created, std::move(path)});
      }
      return;
    }

    if ((event.mask & (IN_CREATE | IN_MOVED_TO | IN_CLOSE_WRITE | IN_MODIFY)) == 0) {
      return;
    }

    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec) {
      return;
    }
    if (!std::filesystem::is_regular_file(status) && !std::filesystem::is_symlink(status)) {
      return;
    }

    if (std::filesystem::is_symlink(status)) {
      events.push_back(WatchEvent{WatchEventKind::Modified, std::move(path)});
      return;
    }

    const auto current = snapshot_for_regular_file(path);
    const auto previous = snapshot_.find(path);
    WatchEventKind kind = WatchEventKind::Modified;
    if (previous == snapshot_.end()) {
      kind = WatchEventKind::Created;
    } else if (current.size > previous->second.size) {
      kind = WatchEventKind::Appended;
    }
    snapshot_[path] = current;
    events.push_back(WatchEvent{kind, std::move(path)});
  }

  std::filesystem::path root_;
  int fd_ = -1;
  std::unordered_map<int, std::filesystem::path> watches_;
  std::unordered_map<std::filesystem::path, WatchSnapshot> snapshot_;
};
#endif

#if defined(__APPLE__)
class FseventsSourceWatcher final : public ISourceWatcher {
 public:
  explicit FseventsSourceWatcher(std::filesystem::path root)
      : root_(std::move(root)) {
    const auto root_text = root_.string();
    CFStringRef path = CFStringCreateWithCString(nullptr, root_text.c_str(), kCFStringEncodingUTF8);
    if (path == nullptr) {
      throw std::runtime_error("failed to create FSEvents path");
    }
    const void* values[] = {path};
    CFArrayRef paths = CFArrayCreate(nullptr, values, 1, &kCFTypeArrayCallBacks);
    CFRelease(path);
    if (paths == nullptr) {
      throw std::runtime_error("failed to create FSEvents path array");
    }

    FSEventStreamContext context{};
    context.info = this;
    stream_ = FSEventStreamCreate(nullptr,
                                  &FseventsSourceWatcher::callback,
                                  &context,
                                  paths,
                                  kFSEventStreamEventIdSinceNow,
                                  0.2,
                                  kFSEventStreamCreateFlagFileEvents |
                                      kFSEventStreamCreateFlagNoDefer);
    CFRelease(paths);
    if (stream_ == nullptr) {
      throw std::runtime_error("failed to create FSEvents stream");
    }

    queue_ = dispatch_queue_create("yisync.source.fsevents", DISPATCH_QUEUE_SERIAL);
    FSEventStreamSetDispatchQueue(stream_, queue_);
    if (!FSEventStreamStart(stream_)) {
      FSEventStreamInvalidate(stream_);
      FSEventStreamRelease(stream_);
      stream_ = nullptr;
      throw std::runtime_error("failed to start FSEvents stream");
    }
  }

  ~FseventsSourceWatcher() override {
    if (stream_ != nullptr) {
      FSEventStreamStop(stream_);
      FSEventStreamInvalidate(stream_);
      FSEventStreamRelease(stream_);
      stream_ = nullptr;
    }
#if !OS_OBJECT_USE_OBJC
    if (queue_ != nullptr) {
      dispatch_release(queue_);
    }
#endif
  }

  FseventsSourceWatcher(const FseventsSourceWatcher&) = delete;
  FseventsSourceWatcher& operator=(const FseventsSourceWatcher&) = delete;

  std::vector<WatchEvent> poll() override {
    if (overflow_.exchange(false, std::memory_order_acq_rel)) {
      dirty_.store(false, std::memory_order_release);
      return {WatchEvent{WatchEventKind::Overflow, root_}};
    }
    if (dirty_.exchange(false, std::memory_order_acq_rel)) {
      return {WatchEvent{WatchEventKind::Modified, root_}};
    }
    return {};
  }

 private:
  static void callback(ConstFSEventStreamRef,
                       void* info,
                       std::size_t count,
                       void*,
                       const FSEventStreamEventFlags flags[],
                       const FSEventStreamEventId[]) {
    auto* self = static_cast<FseventsSourceWatcher*>(info);
    bool overflow = false;
    for (std::size_t i = 0; i < count; ++i) {
      if ((flags[i] & (kFSEventStreamEventFlagMustScanSubDirs |
                       kFSEventStreamEventFlagUserDropped |
                       kFSEventStreamEventFlagKernelDropped)) != 0) {
        overflow = true;
      }
    }
    if (overflow) {
      self->overflow_.store(true, std::memory_order_release);
    }
    self->dirty_.store(true, std::memory_order_release);
  }

  std::filesystem::path root_;
  FSEventStreamRef stream_ = nullptr;
  dispatch_queue_t queue_ = nullptr;
  std::atomic<bool> dirty_{false};
  std::atomic<bool> overflow_{false};
};
#endif

}  // namespace

SourceDirectory::SourceDirectory(std::uint64_t stream_id,
                                 std::filesystem::path root,
                                 std::uint64_t checksum_range_len,
                                 std::string entry_name_regex)
    : stream_id_(stream_id),
      root_(std::move(root)),
      checksum_range_len_(checksum_range_len),
      entry_name_regex_(std::move(entry_name_regex)) {}

SimulatedSourceReader::SimulatedSourceReader(Bytes data)
    : data_(std::move(data)) {}

const Bytes& SimulatedSourceReader::data() const noexcept {
  return data_;
}

Bytes SimulatedSourceReader::read_range(std::uint64_t file_id,
                                        std::uint64_t offset,
                                        std::uint64_t len) const {
  (void)file_id;
  if (offset > data_.size() || len > data_.size() || offset + len > data_.size()) {
    throw std::runtime_error("simulated source read range is outside data");
  }
  return Bytes(data_.begin() + static_cast<std::ptrdiff_t>(offset),
               data_.begin() + static_cast<std::ptrdiff_t>(offset + len));
}

const std::filesystem::path& SourceDirectory::root() const noexcept {
  return root_;
}

Manifest1Stream SourceDirectory::scan_manifest() const {
  return scan_manifest_stream(stream_id_, root_, checksum_range_len_, {}, entry_name_regex_);
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
#if defined(__linux__)
      try {
        return std::make_unique<InotifySourceWatcher>(root);
      } catch (const std::exception&) {
        return std::make_unique<PollingSourceWatcher>(root);
      }
#elif defined(__APPLE__)
      try {
        return std::make_unique<FseventsSourceWatcher>(root);
      } catch (const std::exception&) {
        return std::make_unique<PollingSourceWatcher>(root);
      }
#else
      return std::make_unique<PollingSourceWatcher>(root);
#endif
    case WatchBackend::Polling:
      return std::make_unique<PollingSourceWatcher>(root);
    case WatchBackend::Inotify:
#if defined(__linux__)
      try {
        return std::make_unique<InotifySourceWatcher>(root);
      } catch (const std::exception&) {
        return std::make_unique<PollingSourceWatcher>(root);
      }
#else
      return std::make_unique<PollingSourceWatcher>(root);
#endif
    case WatchBackend::Fsevents:
#if defined(__APPLE__)
      try {
        return std::make_unique<FseventsSourceWatcher>(root);
      } catch (const std::exception&) {
        return std::make_unique<PollingSourceWatcher>(root);
      }
#else
      return std::make_unique<PollingSourceWatcher>(root);
#endif
  }
  return std::make_unique<PollingSourceWatcher>(root);
}

std::string_view watch_backend_name(WatchBackend backend) noexcept {
  switch (backend) {
    case WatchBackend::Auto:
      return "auto";
    case WatchBackend::Polling:
      return "polling";
    case WatchBackend::Inotify:
      return "inotify";
    case WatchBackend::Fsevents:
      return "fsevents";
  }
  return "unknown";
}

}  // namespace yisync
