#include "yisync_receiver.hpp"

#include "yisync_sync.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <fcntl.h>
#include <unistd.h>

namespace yisync {
namespace {

std::uint64_t file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return 0;
  }
  return size;
}

Bytes raw_data_for_data_message(const Data& data) {
  if (data.compression != Compression::None) {
    throw std::runtime_error("compression is not implemented in this prototype");
  }
  if (data.payload.size() != data.raw_len) {
    throw std::runtime_error("raw_len does not match uncompressed payload length");
  }
  return data.payload;
}

Bytes raw_data_for_chunk_message(const Chunk& chunk) {
  if (chunk.compression != Compression::None) {
    throw std::runtime_error("compression is not implemented in this prototype");
  }
  if (chunk.payload.size() != chunk.raw_len) {
    throw std::runtime_error("raw_len does not match uncompressed chunk payload length");
  }
  return chunk.payload;
}

bool bytes_equal(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

void fsync_path(const std::filesystem::path& path, const char* what) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    throw std::runtime_error(std::string("failed to open ") + what + " for fsync");
  }
  if (::fsync(fd) != 0) {
    const auto error = errno;
    ::close(fd);
    throw std::runtime_error(std::string("failed to fsync ") + what + ": " + std::strerror(error));
  }
  ::close(fd);
}

void fsync_directory(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    throw std::runtime_error("failed to open directory for fsync");
  }
  if (::fsync(fd) != 0) {
    const auto error = errno;
    ::close(fd);
    throw std::runtime_error(std::string("failed to fsync directory: ") + std::strerror(error));
  }
  ::close(fd);
}

std::uint32_t checksum_crc32c_value(const FileChecksum& checksum) {
  if (checksum.value.size() != 4) {
    return 0;
  }
  return static_cast<std::uint32_t>(static_cast<std::uint8_t>(checksum.value[0])) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(checksum.value[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(checksum.value[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(checksum.value[3])) << 24);
}

bool is_safe_relative_name(const std::string& name) {
  if (name.empty()) {
    return false;
  }
  const std::filesystem::path path(name);
  if (path.is_absolute()) {
    return false;
  }
  for (const auto& part : path) {
    if (part.empty() || part == "." || part == "..") {
      return false;
    }
  }
  return true;
}

std::filesystem::path path_from_manifest_or_legacy(std::uint64_t stream_id,
                                                   const std::filesystem::path& root,
                                                   std::uint64_t file_id) {
  const auto manifest = scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
  const auto it = std::find_if(manifest.entries.begin(), manifest.entries.end(), [file_id](const auto& entry) {
    return entry.file_id == file_id;
  });
  if (it != manifest.entries.end() && is_safe_relative_name(it->name)) {
    return root / it->name;
  }
  return root / file_name_for_id(file_id);
}

}  // namespace

ReceiverStream::ReceiverStream(std::uint64_t stream_id, std::filesystem::path root)
    : stream_id_(stream_id), root_(std::move(root)) {
  std::filesystem::create_directories(root_);
  reset_session_from_disk();
}

const ReceiverStreamState& ReceiverStream::state() const noexcept {
  return state_;
}

std::filesystem::path ReceiverStream::current_path() const {
  return current_path_;
}

void ReceiverStream::refresh_committed_from_disk() {
  const auto expected_seq = state_.expected_seq;
  state_.active = true;
  state_.expected_seq = expected_seq;

  const auto manifest = scan_manifest_stream(stream_id_, root_, 4 * 1024 * 1024);
  if (manifest.entries.empty()) {
    state_.current_file_id = 0;
    state_.current_offset = 0;
    state_.next_create_file_id = 1;
    current_kind_ = EntryKind::RegularFile;
    current_path_.clear();
    return;
  }

  const auto& latest = manifest.entries.back();
  state_.current_file_id = latest.file_id;
  state_.current_offset = latest.size;
  state_.next_create_file_id = latest.file_id + 1;
  current_kind_ = latest.kind;
  current_path_ = path_for_name(latest.name);
}

Heartbeat ReceiverStream::heartbeat(std::uint64_t recv_window_bytes, std::uint64_t durable_offset) const {
  return Heartbeat{stream_id_, state_.expected_seq, state_.current_file_id, state_.current_offset, durable_offset, recv_window_bytes};
}

std::optional<Nack> ReceiverStream::apply(const Create& create) {
  if (!state_.active || create.stream_id != stream_id_) {
    return nack(create.stream_id, create.seq, create.file_id, 0, NackReason::BadSession, "inactive or wrong stream");
  }
  if (create.seq < state_.expected_seq) {
    return std::nullopt;
  }
  if (create.seq > state_.expected_seq) {
    return nack(create.stream_id, create.seq, create.file_id, 0, NackReason::BadSeq, "future seq");
  }
  if (create.kind != EntryKind::RegularFile &&
      create.kind != EntryKind::Directory &&
      create.kind != EntryKind::Symlink) {
    return nack(create.stream_id,
                create.seq,
                create.file_id,
                0,
                NackReason::BadCreate,
                "unsupported create entry kind");
  }
  if (!is_safe_relative_name(create.name)) {
    return nack(create.stream_id,
                create.seq,
                create.file_id,
                0,
                NackReason::BadCreate,
                "unsafe target path");
  }

  if (create.prev_file_id != 0) {
    const auto prev_path = path_for_file(create.prev_file_id);
    if (!std::filesystem::exists(prev_path)) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::PrevFileIncomplete,
                  "previous file does not exist");
    }
    const auto prev_size = file_size_or_zero(prev_path);
    if (prev_size != create.prev_final_size) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::PrevFileIncomplete,
                  "previous file size mismatch");
    }
    if (!checksum_matches(create.prev_checksum, prev_path)) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::ChecksumMismatch,
                  "previous file checksum mismatch");
    }
  }

  const auto path = path_for_name(create.name);
  if (std::filesystem::exists(path)) {
    const auto existing_size = file_size_or_zero(path);
    return nack(create.stream_id,
                create.seq,
                create.file_id,
                existing_size,
                NackReason::FileExists,
                "target file already exists");
  }

  std::error_code ec;
  std::filesystem::create_directories(path.parent_path(), ec);
  if (ec) {
    return nack(create.stream_id,
                create.seq,
                create.file_id,
                0,
                NackReason::IoError,
                "failed to create parent directories: " + ec.message());
  }

  if (create.kind == EntryKind::Directory) {
    std::filesystem::create_directory(path, ec);
    if (ec) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::IoError,
                  "failed to create directory: " + ec.message());
    }
  } else if (create.kind == EntryKind::Symlink) {
    std::filesystem::create_symlink(std::filesystem::path(create.link_target), path, ec);
    if (ec) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::IoError,
                  "failed to create symlink: " + ec.message());
    }
  } else {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
      return nack(create.stream_id,
                  create.seq,
                  create.file_id,
                  0,
                  NackReason::IoError,
                  "failed to create target file");
    }
  }

  state_.expected_seq += 1;
  state_.current_file_id = create.file_id;
  state_.current_offset = 0;
  state_.next_create_file_id = create.file_id + 1;
  current_kind_ = create.kind;
  current_path_ = path;
  return std::nullopt;
}

std::optional<Nack> ReceiverStream::apply(const Data& data) {
  if (!state_.active || data.stream_id != stream_id_) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadSession, "inactive or wrong stream");
  }
  if (data.seq < state_.expected_seq) {
    return std::nullopt;
  }
  if (data.seq > state_.expected_seq) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadSeq, "future seq");
  }
  if (data.file_id != state_.current_file_id) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadFileOrder, "wrong file id");
  }
  if (current_kind_ != EntryKind::RegularFile) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadFileOrder, "DATA target is not a regular file");
  }

  const auto path = current_path_.empty() ? path_for_file(data.file_id) : current_path_;
  if (!std::filesystem::exists(path)) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadOffset, "target file missing");
  }
  const auto size = file_size_or_zero(path);
  if (data.offset != size) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadOffset, "offset mismatch");
  }
  if (data.raw_len == 0) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::DecodeError, "empty DATA");
  }

  Bytes raw;
  try {
    raw = raw_data_for_data_message(data);
  } catch (const std::exception& ex) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::DecodeError, ex.what());
  }

  if (data.checksum_algo != ChecksumAlgo::Crc32c) {
    return nack(data.stream_id,
                data.seq,
                data.file_id,
                data.offset,
                NackReason::BadChecksum,
                "only CRC32C DATA checksum is implemented");
  }
  if (!bytes_equal(crc32c_bytes(raw), data.checksum)) {
    return nack(data.stream_id,
                data.seq,
                data.file_id,
                data.offset,
                NackReason::BadChecksum,
                "DATA checksum mismatch");
  }

  std::ofstream output(path, std::ios::binary | std::ios::app);
  if (!output) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::IoError, "failed to open target file");
  }
  output.write(reinterpret_cast<const char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
  if (!output) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::IoError, "failed to append target file");
  }

  state_.expected_seq += 1;
  state_.current_offset = data.offset + data.raw_len;
  return std::nullopt;
}

void ReceiverStream::reset_session_from_disk() {
  state_ = {};
  state_.active = true;
  state_.expected_seq = 1;

  const auto manifest = scan_manifest_stream(stream_id_, root_, 4 * 1024 * 1024);
  if (manifest.entries.empty()) {
    state_.current_file_id = 0;
    state_.current_offset = 0;
    state_.next_create_file_id = 1;
    current_kind_ = EntryKind::RegularFile;
    current_path_.clear();
    return;
  }

  const auto& latest = manifest.entries.back();
  state_.current_file_id = latest.file_id;
  state_.current_offset = latest.size;
  state_.next_create_file_id = latest.file_id + 1;
  current_kind_ = latest.kind;
  current_path_ = path_for_name(latest.name);
}

std::filesystem::path ReceiverStream::path_for_name(const std::string& name) const {
  if (!is_safe_relative_name(name)) {
    throw std::runtime_error("unsafe receiver path: " + name);
  }
  return root_ / std::filesystem::path(name);
}

std::filesystem::path ReceiverStream::path_for_file(std::uint64_t file_id) const {
  return path_from_manifest_or_legacy(stream_id_, root_, file_id);
}

Nack ReceiverStream::nack(std::uint64_t stream_id,
                          std::uint64_t got_seq,
                          std::uint64_t file_id,
                          std::uint64_t offset,
                          NackReason reason,
                          std::string detail) const {
  return Nack{stream_id,
              got_seq,
              state_.expected_seq,
              file_id,
              offset,
              state_.current_file_id,
              state_.current_offset,
              reason,
              std::move(detail)};
}

struct ChunkedReceiverStream::Impl {
  struct ActiveFile {
    std::uint64_t order_seq = 0;
    std::uint64_t file_id = 0;
    std::string name;
    std::uint64_t final_size = 0;
    std::uint64_t chunk_size = kDefaultChunkSizeBytes;
    std::uint64_t chunk_count = 0;
    FileChecksum file_checksum;
    std::uint64_t prev_file_id = 0;
    std::uint64_t prev_final_size = 0;
    FileChecksum prev_checksum;
    std::filesystem::path temp_path;
    std::filesystem::path final_path;
    std::vector<bool> received;
    std::vector<bool> checkpointed;
    std::uint64_t received_count = 0;
    std::uint64_t checkpointed_count = 0;
    std::uint64_t pending_checkpoint_bytes = 0;
    bool commit_pending = false;
  };

  Impl(std::uint64_t stream_id_value, std::filesystem::path root_value)
      : stream_id(stream_id_value), root(std::move(root_value)), temp_root(root / ".yisync_tmp") {
    std::filesystem::create_directories(root);
    std::filesystem::create_directories(temp_root);
    restore_active_from_disk();
  }

  std::filesystem::path final_path_for(std::uint64_t file_id, const std::string& name) const {
    if (name.empty()) {
      return root / file_name_for_id(file_id);
    }
    if (!is_safe_relative_name(name)) {
      throw std::runtime_error("unsafe chunk final path: " + name);
    }
    return root / std::filesystem::path(name);
  }

  std::filesystem::path final_path_for(const FileBegin& begin) const {
    return final_path_for(begin.file_id, begin.name);
  }

  std::filesystem::path temp_path_for(std::uint64_t order_seq, std::uint64_t file_id) const {
    return temp_root / (std::to_string(order_seq) + "_" + file_name_for_id(file_id) + ".tmp");
  }

  std::filesystem::path temp_path_for(const FileBegin& begin) const {
    return temp_path_for(begin.order_seq, begin.file_id);
  }

  std::filesystem::path meta_path_for(const ActiveFile& active) const {
    return std::filesystem::path(active.temp_path.string() + ".meta");
  }

  ChunkCheckpointTask make_checkpoint_task(const ActiveFile& active) const {
    return ChunkCheckpointTask{
        .temp_path = active.temp_path,
        .meta_path = meta_path_for(active),
        .temp_root = temp_root,
        .order_seq = active.order_seq,
        .file_id = active.file_id,
        .name = active.name,
        .final_size = active.final_size,
        .chunk_size = active.chunk_size,
        .chunk_count = active.chunk_count,
        .file_checksum = active.file_checksum,
        .prev_file_id = active.prev_file_id,
        .prev_final_size = active.prev_final_size,
        .prev_checksum = active.prev_checksum,
        .checkpointed = active.checkpointed,
    };
  }

  ChunkCommitTask make_commit_task(const ActiveFile& active) const {
    auto checkpoint = make_checkpoint_task(active);
    checkpoint.checkpointed = active.received;
    return ChunkCommitTask{
        .root = root,
        .temp_root = temp_root,
        .temp_path = active.temp_path,
        .final_path = active.final_path,
        .meta_path = meta_path_for(active),
        .checkpoint = std::move(checkpoint),
        .stream_id = stream_id,
        .order_seq = active.order_seq,
        .file_id = active.file_id,
        .final_size = active.final_size,
        .file_checksum = active.file_checksum,
    };
  }

  void restore_active_from_disk() {
    const auto manifest = scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
    if (!manifest.entries.empty()) {
      const auto& latest = manifest.entries.back();
      current_file_id = latest.file_id;
      current_offset = latest.size;
      expected_order_seq = latest.order_seq + 1;
    }

    for (const auto& file : manifest.incomplete_chunks) {
      if (file.chunk_count == 0 || file.chunk_size == 0) {
        continue;
      }
      auto temp_path = temp_path_for(file.order_seq, file.file_id);
      if (!std::filesystem::exists(temp_path)) {
        continue;
      }

      ActiveFile active_file;
      active_file.order_seq = file.order_seq;
      active_file.file_id = file.file_id;
      active_file.name = file.name;
      active_file.final_size = file.final_size;
      active_file.chunk_size = file.chunk_size;
      active_file.chunk_count = file.chunk_count;
      active_file.file_checksum = file.file_checksum;
      active_file.prev_file_id = file.prev_file_id;
      active_file.prev_final_size = file.prev_final_size;
      active_file.prev_checksum = file.prev_checksum;
      active_file.temp_path = std::move(temp_path);
      try {
        active_file.final_path = final_path_for(file.file_id, file.name);
      } catch (const std::exception&) {
        continue;
      }
      active_file.received.assign(static_cast<std::size_t>(file.chunk_count), false);
      active_file.checkpointed.assign(static_cast<std::size_t>(file.chunk_count), false);
      for (const auto chunk_index : file.received_chunks) {
        if (chunk_index >= file.chunk_count) {
          continue;
        }
        const auto index = static_cast<std::size_t>(chunk_index);
        if (!active_file.received[index]) {
          active_file.received[index] = true;
          active_file.checkpointed[index] = true;
          active_file.received_count += 1;
          active_file.checkpointed_count += 1;
        }
      }
      active.emplace(active_file.order_seq, std::move(active_file));
    }

    if (!active.empty()) {
      auto min_order_seq = active.begin()->first;
      for (const auto& [order_seq, unused] : active) {
        (void)unused;
        min_order_seq = std::min(min_order_seq, order_seq);
      }
      expected_order_seq = std::min(expected_order_seq, min_order_seq);
    }
  }

  std::uint64_t pending_checkpoint_bytes_total() const noexcept {
    std::uint64_t total = 0;
    for (const auto& [order_seq, file] : active) {
      (void)order_seq;
      total += file.pending_checkpoint_bytes;
    }
    return total;
  }

  void refresh_committed_from_disk() {
    const auto manifest = scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
    current_file_id = 0;
    current_offset = 0;
    if (!manifest.entries.empty()) {
      const auto& latest = manifest.entries.back();
      current_file_id = latest.file_id;
      current_offset = latest.size;
      expected_order_seq = std::max(expected_order_seq, latest.order_seq + 1);
    }
  }

  ChunkCheckpointBatch checkpoint_all() {
    ChunkCheckpointBatch batch;
    for (auto& [order_seq, file] : active) {
      (void)order_seq;
      if (file.commit_pending) {
        continue;
      }
      std::uint64_t new_chunks = 0;
      for (std::uint64_t index = 0; index < file.chunk_count; ++index) {
        const auto slot = static_cast<std::size_t>(index);
        if (!file.received[slot] || file.checkpointed[slot]) {
          continue;
        }
        file.checkpointed[slot] = true;
        file.checkpointed_count += 1;
        new_chunks += 1;
      }
      if (new_chunks == 0 && file.pending_checkpoint_bytes == 0) {
        continue;
      }
      batch.tasks.push_back(make_checkpoint_task(file));
      batch.result.files += 1;
      batch.result.chunks += new_chunks;
      batch.result.bytes += file.pending_checkpoint_bytes;
      file.pending_checkpoint_bytes = 0;
    }
    return batch;
  }

  ChunkCommitTask prepare_commit_task(ActiveFile& file) {
    file.commit_pending = true;
    return make_commit_task(file);
  }

  Nack nack(const FileBegin& begin, NackReason reason, std::string detail) const {
    return Nack{begin.stream_id,
                begin.order_seq,
                expected_order_seq,
                begin.file_id,
                0,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const Chunk& chunk, NackReason reason, std::string detail) const {
    return Nack{chunk.stream_id,
                chunk.order_seq,
                expected_order_seq,
                chunk.file_id,
                chunk.offset,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const FileCommit& commit, NackReason reason, std::string detail) const {
    return Nack{commit.stream_id,
                commit.order_seq,
                expected_order_seq,
                commit.file_id,
                0,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  std::uint64_t stream_id = 0;
  std::filesystem::path root;
  std::filesystem::path temp_root;
  std::uint64_t expected_order_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::unordered_map<std::uint64_t, ActiveFile> active;
};

ChunkedReceiverStream::ChunkedReceiverStream(std::uint64_t stream_id, std::filesystem::path root)
    : impl_(std::make_unique<Impl>(stream_id, std::move(root))) {}

ChunkedReceiverStream::~ChunkedReceiverStream() = default;

ChunkedReceiverStream::ChunkedReceiverStream(ChunkedReceiverStream&&) noexcept = default;

ChunkedReceiverStream& ChunkedReceiverStream::operator=(ChunkedReceiverStream&&) noexcept = default;

std::uint64_t ChunkedReceiverStream::expected_order_seq() const noexcept {
  return impl_->expected_order_seq;
}

void ChunkedReceiverStream::refresh_committed_from_disk() {
  impl_->refresh_committed_from_disk();
}

std::uint64_t ChunkedReceiverStream::pending_checkpoint_bytes() const noexcept {
  return impl_->pending_checkpoint_bytes_total();
}

ChunkCheckpointBatch ChunkedReceiverStream::checkpoint() {
  return impl_->checkpoint_all();
}

void ChunkedReceiverStream::write_checkpoint_task(const ChunkCheckpointTask& task) {
  fsync_path(task.temp_path, "chunk temp file");

  const auto staging_path = std::filesystem::path(task.meta_path.string() + ".writing");
  std::ofstream output(staging_path, std::ios::trunc);
  if (!output) {
    throw std::runtime_error("failed to write chunk metadata");
  }

  output << "order_seq=" << task.order_seq << "\n";
  output << "file_id=" << task.file_id << "\n";
  output << "name=" << task.name << "\n";
  output << "final_size=" << task.final_size << "\n";
  output << "chunk_size=" << task.chunk_size << "\n";
  output << "chunk_count=" << task.chunk_count << "\n";
  output << "file_checksum_algo=" << static_cast<int>(task.file_checksum.algo) << "\n";
  output << "file_checksum_offset=" << task.file_checksum.offset << "\n";
  output << "file_checksum_len=" << task.file_checksum.len << "\n";
  if (!task.file_checksum.value.empty()) {
    output << "file_checksum_crc32c=" << checksum_crc32c_value(task.file_checksum) << "\n";
  }
  output << "prev_file_id=" << task.prev_file_id << "\n";
  output << "prev_final_size=" << task.prev_final_size << "\n";
  output << "prev_checksum_algo=" << static_cast<int>(task.prev_checksum.algo) << "\n";
  output << "prev_checksum_offset=" << task.prev_checksum.offset << "\n";
  output << "prev_checksum_len=" << task.prev_checksum.len << "\n";
  if (!task.prev_checksum.value.empty()) {
    output << "prev_checksum_crc32c=" << checksum_crc32c_value(task.prev_checksum) << "\n";
  }
  output << "received_chunks=";
  bool first = true;
  for (std::uint64_t index = 0; index < task.checkpointed.size(); ++index) {
    if (!task.checkpointed[static_cast<std::size_t>(index)]) {
      continue;
    }
    if (!first) {
      output << ",";
    }
    first = false;
    output << index;
  }
  output << "\n";
  output.close();
  if (!output) {
    throw std::runtime_error("failed to flush chunk metadata");
  }
  fsync_path(staging_path, "chunk metadata staging file");

  std::error_code ec;
  std::filesystem::rename(staging_path, task.meta_path, ec);
  if (ec) {
    throw std::runtime_error("failed to publish chunk metadata: " + ec.message());
  }
  fsync_directory(task.temp_root);
}

std::optional<Nack> ChunkedReceiverStream::prepare_commit(const FileCommit& commit,
                                                          ChunkCommitTask& task) {
  if (commit.stream_id != impl_->stream_id) {
    return impl_->nack(commit, NackReason::BadSession, "wrong stream");
  }
  if (commit.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (commit.order_seq > impl_->expected_order_seq) {
    return impl_->nack(commit, NackReason::BadSeq, "future order_seq");
  }

  auto it = impl_->active.find(commit.order_seq);
  if (it == impl_->active.end()) {
    return impl_->nack(commit, NackReason::BadCommit, "commit received before FILE_BEGIN");
  }
  auto& active = it->second;
  if (commit.file_id != active.file_id) {
    return impl_->nack(commit, NackReason::BadFileOrder, "commit file_id mismatch");
  }
  if (active.commit_pending) {
    return impl_->nack(commit, NackReason::BadCommit, "commit already pending");
  }
  if (active.received_count != active.chunk_count) {
    return impl_->nack(commit, NackReason::BadCommit, "not all chunks have been received");
  }
  if (std::filesystem::exists(active.final_path)) {
    return impl_->nack(commit, NackReason::FileExists, "final file already exists");
  }

  task = impl_->prepare_commit_task(active);
  return std::nullopt;
}

void ChunkedReceiverStream::abort_commit(std::uint64_t order_seq) noexcept {
  if (!impl_) {
    return;
  }
  auto it = impl_->active.find(order_seq);
  if (it == impl_->active.end()) {
    return;
  }
  it->second.commit_pending = false;
}

void ChunkedReceiverStream::finish_commit(const ChunkCommitResult& result) {
  if (result.stream_id != impl_->stream_id) {
    throw std::runtime_error("commit result stream mismatch");
  }
  auto it = impl_->active.find(result.order_seq);
  if (it == impl_->active.end()) {
    throw std::runtime_error("commit result for inactive file");
  }
  const auto& active = it->second;
  if (result.file_id != active.file_id) {
    throw std::runtime_error("commit result file mismatch");
  }
  impl_->current_file_id = active.file_id;
  impl_->current_offset = active.final_size;
  impl_->expected_order_seq = std::max(impl_->expected_order_seq, active.order_seq + 1);
  impl_->active.erase(it);
}

ChunkCommitResult ChunkedReceiverStream::write_commit_task(const ChunkCommitTask& task) {
  if (!checksum_matches(task.file_checksum, task.temp_path)) {
    throw std::runtime_error("final file checksum mismatch");
  }
  ChunkedReceiverStream::write_checkpoint_task(task.checkpoint);

  std::error_code ec;
  if (std::filesystem::exists(task.final_path, ec)) {
    throw std::runtime_error("final file already exists");
  }
  ec.clear();
  std::filesystem::create_directories(task.final_path.parent_path(), ec);
  if (ec) {
    throw std::runtime_error("failed to create chunk final parent: " + ec.message());
  }
  std::filesystem::rename(task.temp_path, task.final_path, ec);
  if (ec) {
    throw std::runtime_error("failed to commit chunk temp file: " + ec.message());
  }

  fsync_path(task.final_path, "committed file");
  fsync_directory(task.final_path.parent_path());
  std::filesystem::remove(task.meta_path, ec);
  if (ec) {
    throw std::runtime_error("failed to remove chunk metadata: " + ec.message());
  }
  fsync_directory(task.temp_root);

  return ChunkCommitResult{
      .stream_id = task.stream_id,
      .order_seq = task.order_seq,
      .file_id = task.file_id,
      .final_size = task.final_size,
      .final_path = task.final_path,
  };
}

std::vector<MissingChunkRange> ChunkedReceiverStream::missing_ranges(std::uint64_t order_seq,
                                                                     std::uint64_t max_ranges) const {
  std::vector<MissingChunkRange> ranges;
  if (max_ranges == 0) {
    return ranges;
  }

  const auto it = impl_->active.find(order_seq);
  if (it == impl_->active.end()) {
    return ranges;
  }
  const auto& active = it->second;

  std::uint64_t highest_received = 0;
  bool has_received = false;
  for (std::uint64_t index = 0; index < active.chunk_count; ++index) {
    if (active.received[static_cast<std::size_t>(index)]) {
      highest_received = index;
      has_received = true;
    }
  }
  if (!has_received) {
    return ranges;
  }

  std::uint64_t index = 0;
  while (index < highest_received && ranges.size() < max_ranges) {
    if (active.received[static_cast<std::size_t>(index)]) {
      ++index;
      continue;
    }

    const auto first = index;
    while (index < highest_received && !active.received[static_cast<std::size_t>(index)]) {
      ++index;
    }
    ranges.push_back(MissingChunkRange{
        .order_seq = active.order_seq,
        .file_id = active.file_id,
        .first_chunk_index = first,
        .last_chunk_index = index - 1,
    });
  }
  return ranges;
}

std::optional<Nack> ChunkedReceiverStream::apply(const FileBegin& begin) {
  if (begin.stream_id != impl_->stream_id) {
    return impl_->nack(begin, NackReason::BadSession, "wrong stream");
  }
  if (begin.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (begin.order_seq > impl_->expected_order_seq) {
    return impl_->nack(begin, NackReason::BadSeq, "future order_seq");
  }
  if (!should_use_chunk_mode(begin.final_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "file size does not require chunk mode");
  }
  if (begin.chunk_size != kDefaultChunkSizeBytes) {
    return impl_->nack(begin, NackReason::BadChunk, "unsupported chunk_size");
  }
  if (begin.chunk_count != chunk_count_for_size(begin.final_size, begin.chunk_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "chunk_count does not match final_size");
  }
  if (impl_->active.contains(begin.order_seq)) {
    return std::nullopt;
  }

  if (begin.prev_file_id != 0) {
    const auto prev_path = path_from_manifest_or_legacy(begin.stream_id, impl_->root, begin.prev_file_id);
    if (!std::filesystem::exists(prev_path)) {
      return impl_->nack(begin, NackReason::PrevFileIncomplete, "previous file does not exist");
    }
    const auto prev_size = file_size_or_zero(prev_path);
    if (prev_size != begin.prev_final_size) {
      return impl_->nack(begin, NackReason::PrevFileIncomplete, "previous file size mismatch");
    }
    if (!checksum_matches(begin.prev_checksum, prev_path)) {
      return impl_->nack(begin, NackReason::ChecksumMismatch, "previous file checksum mismatch");
    }
  }

  std::filesystem::path final_path;
  try {
    final_path = impl_->final_path_for(begin);
  } catch (const std::exception& ex) {
    return impl_->nack(begin, NackReason::BadCreate, ex.what());
  }
  if (std::filesystem::exists(final_path)) {
    return impl_->nack(begin, NackReason::FileExists, "final file already exists");
  }

  auto temp_path = impl_->temp_path_for(begin);
  {
    std::ofstream temp(temp_path, std::ios::binary | std::ios::trunc);
    if (!temp) {
      return impl_->nack(begin, NackReason::IoError, "failed to create chunk temp file");
    }
  }

  Impl::ActiveFile active;
  active.order_seq = begin.order_seq;
  active.file_id = begin.file_id;
  active.name = begin.name;
  active.final_size = begin.final_size;
  active.chunk_size = begin.chunk_size;
  active.chunk_count = begin.chunk_count;
  active.file_checksum = begin.file_checksum;
  active.prev_file_id = begin.prev_file_id;
  active.prev_final_size = begin.prev_final_size;
  active.prev_checksum = begin.prev_checksum;
  active.temp_path = std::move(temp_path);
  active.final_path = std::move(final_path);
  active.received.assign(static_cast<std::size_t>(begin.chunk_count), false);
  active.checkpointed.assign(static_cast<std::size_t>(begin.chunk_count), false);
  try {
    ChunkedReceiverStream::write_checkpoint_task(impl_->make_checkpoint_task(active));
  } catch (const std::exception& ex) {
    return impl_->nack(begin, NackReason::IoError, ex.what());
  }
  impl_->active.emplace(begin.order_seq, std::move(active));
  return std::nullopt;
}

std::optional<Nack> ChunkedReceiverStream::apply(const Chunk& chunk) {
  if (chunk.stream_id != impl_->stream_id) {
    return impl_->nack(chunk, NackReason::BadSession, "wrong stream");
  }
  if (chunk.order_seq < impl_->expected_order_seq) {
    return std::nullopt;
  }
  if (chunk.order_seq > impl_->expected_order_seq) {
    return impl_->nack(chunk, NackReason::BadSeq, "future order_seq");
  }

  auto it = impl_->active.find(chunk.order_seq);
  if (it == impl_->active.end()) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk received before FILE_BEGIN");
  }

  auto& active = it->second;
  if (chunk.file_id != active.file_id) {
    return impl_->nack(chunk, NackReason::BadFileOrder, "chunk file_id mismatch");
  }
  if (chunk.chunk_index >= active.chunk_count) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk_index out of range");
  }

  const auto expected_offset = chunk.chunk_index * active.chunk_size;
  if (chunk.offset != expected_offset) {
    return impl_->nack(chunk, NackReason::BadOffset, "chunk offset mismatch");
  }
  const auto expected_len = static_cast<std::uint32_t>(
      std::min<std::uint64_t>(active.chunk_size, active.final_size - chunk.offset));
  if (chunk.raw_len != expected_len || chunk.offset + chunk.raw_len > active.final_size) {
    return impl_->nack(chunk, NackReason::BadChunk, "chunk raw_len mismatch");
  }
  if (active.received[static_cast<std::size_t>(chunk.chunk_index)]) {
    return std::nullopt;
  }

  Bytes raw;
  try {
    raw = raw_data_for_chunk_message(chunk);
  } catch (const std::exception& ex) {
    return impl_->nack(chunk, NackReason::DecodeError, ex.what());
  }
  if (chunk.checksum_algo != ChecksumAlgo::Crc32c) {
    return impl_->nack(chunk, NackReason::BadChecksum, "only CRC32C CHUNK checksum is implemented");
  }
  if (!bytes_equal(crc32c_bytes(raw), chunk.checksum)) {
    return impl_->nack(chunk, NackReason::BadChecksum, "CHUNK checksum mismatch");
  }

  std::fstream temp(active.temp_path, std::ios::binary | std::ios::in | std::ios::out);
  if (!temp) {
    return impl_->nack(chunk, NackReason::IoError, "failed to open chunk temp file");
  }
  temp.seekp(static_cast<std::streamoff>(chunk.offset), std::ios::beg);
  temp.write(reinterpret_cast<const char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
  if (!temp) {
    return impl_->nack(chunk, NackReason::IoError, "failed to write chunk temp file");
  }

  active.received[static_cast<std::size_t>(chunk.chunk_index)] = true;
  active.received_count += 1;
  active.pending_checkpoint_bytes += chunk.raw_len;
  return std::nullopt;
}

std::optional<Nack> ChunkedReceiverStream::apply(const FileCommit& commit) {
  ChunkCommitTask task;
  auto result = prepare_commit(commit, task);
  if (result.has_value() || task.order_seq == 0) {
    return result;
  }
  try {
    finish_commit(ChunkedReceiverStream::write_commit_task(task));
  } catch (const std::exception& ex) {
    abort_commit(commit.order_seq);
    return impl_->nack(commit, NackReason::IoError, ex.what());
  }
  return std::nullopt;
}

}  // namespace yisync
