#include "receiver/yisync_receiver.hpp"

#include "core/yisync_sync.hpp"

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
    state_.current_final_size = 0;
    state_.next_create_file_id = 1;
    current_kind_ = EntryKind::RegularFile;
    current_path_.clear();
    return;
  }

  const auto& latest = manifest.entries.back();
  state_.current_file_id = latest.file_id;
  state_.current_offset = latest.size;
  state_.current_final_size = latest.size;
  state_.expected_seq = std::max(state_.expected_seq, latest.seq + 1);
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

  state_.current_file_id = create.file_id;
  state_.current_offset = 0;
  state_.current_final_size = create.final_size;
  state_.next_create_file_id = create.file_id + 1;
  current_kind_ = create.kind;
  current_path_ = path;
  if (create.kind != EntryKind::RegularFile || create.final_size == 0) {
    state_.expected_seq = std::max(state_.expected_seq, create.seq + 1);
  }
  return std::nullopt;
}

std::optional<Nack> ReceiverStream::apply(const Data& data) {
  if (!state_.active || data.stream_id != stream_id_) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadSession, "inactive or wrong stream");
  }
  if (data.seq < state_.expected_seq) {
    const bool append_resume_for_current_file =
        data.file_id == state_.current_file_id &&
        data.seq + 1 == state_.expected_seq &&
        data.offset == state_.current_offset &&
        state_.current_offset < data.final_size;
    if (!append_resume_for_current_file) {
      return std::nullopt;
    }
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
  const bool restoring_final_size_from_resume =
      data.seq + 1 == state_.expected_seq &&
      data.file_id == state_.current_file_id &&
      data.offset == state_.current_offset &&
      state_.current_final_size == state_.current_offset &&
      data.final_size >= state_.current_offset;
  if (state_.current_final_size != 0 &&
      data.final_size != state_.current_final_size &&
      !restoring_final_size_from_resume) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::SizeConflict, "final_size mismatch");
  }
  if (data.offset + data.raw_len > data.final_size) {
    return nack(data.stream_id, data.seq, data.file_id, data.offset, NackReason::BadOffset, "DATA exceeds final_size");
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

  state_.current_offset = data.offset + data.raw_len;
  state_.current_final_size = data.final_size;
  if (state_.current_offset >= state_.current_final_size) {
    state_.expected_seq = std::max(state_.expected_seq, data.seq + 1);
  }
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
    state_.current_final_size = 0;
    state_.next_create_file_id = 1;
    current_kind_ = EntryKind::RegularFile;
    current_path_.clear();
    return;
  }

  const auto& latest = manifest.entries.back();
  state_.current_file_id = latest.file_id;
  state_.current_offset = latest.size;
  state_.current_final_size = latest.size;
  state_.expected_seq = latest.seq + 1;
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
    std::uint64_t seq = 0;
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
    std::uint64_t received_count = 0;
    bool commit_pending = false;
  };

  Impl(std::uint64_t stream_id_value, std::filesystem::path root_value)
      : stream_id(stream_id_value), root(std::move(root_value)), temp_root(root / ".yisync_tmp") {
    std::filesystem::create_directories(root);
    std::error_code ec;
    std::filesystem::remove_all(temp_root, ec);
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

  std::filesystem::path temp_path_for(std::uint64_t seq, std::uint64_t file_id) const {
    return temp_root / (std::to_string(seq) + "_" + file_name_for_id(file_id) + ".tmp");
  }

  std::filesystem::path temp_path_for(const FileBegin& begin) const {
    return temp_path_for(begin.seq, begin.file_id);
  }

  ChunkCommitTask make_commit_task(const ActiveFile& active) const {
    return ChunkCommitTask{
        .root = root,
        .temp_root = temp_root,
        .temp_path = active.temp_path,
        .final_path = active.final_path,
        .stream_id = stream_id,
        .seq = active.seq,
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
      expected_seq = latest.seq + 1;
    }
  }

  void refresh_committed_from_disk() {
    const auto manifest = scan_manifest_stream(stream_id, root, 4 * 1024 * 1024);
    current_file_id = 0;
    current_offset = 0;
    if (!manifest.entries.empty()) {
      const auto& latest = manifest.entries.back();
      current_file_id = latest.file_id;
      current_offset = latest.size;
      expected_seq = std::max(expected_seq, latest.seq + 1);
    }
  }

  ChunkCommitTask prepare_commit_task(ActiveFile& file) {
    file.commit_pending = true;
    return make_commit_task(file);
  }

  Nack nack(const FileBegin& begin, NackReason reason, std::string detail) const {
    return Nack{begin.stream_id,
                begin.seq,
                expected_seq,
                begin.file_id,
                0,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const Chunk& chunk, NackReason reason, std::string detail) const {
    return Nack{chunk.stream_id,
                chunk.seq,
                expected_seq,
                chunk.file_id,
                chunk.offset,
                current_file_id,
                current_offset,
                reason,
                std::move(detail)};
  }

  Nack nack(const FileCommit& commit, NackReason reason, std::string detail) const {
    return Nack{commit.stream_id,
                commit.seq,
                expected_seq,
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
  std::uint64_t expected_seq = 1;
  std::uint64_t current_file_id = 0;
  std::uint64_t current_offset = 0;
  std::unordered_map<std::uint64_t, ActiveFile> active;
};

ChunkedReceiverStream::ChunkedReceiverStream(std::uint64_t stream_id, std::filesystem::path root)
    : impl_(std::make_unique<Impl>(stream_id, std::move(root))) {}

ChunkedReceiverStream::~ChunkedReceiverStream() = default;

ChunkedReceiverStream::ChunkedReceiverStream(ChunkedReceiverStream&&) noexcept = default;

ChunkedReceiverStream& ChunkedReceiverStream::operator=(ChunkedReceiverStream&&) noexcept = default;

std::uint64_t ChunkedReceiverStream::expected_seq() const noexcept {
  return impl_->expected_seq;
}

void ChunkedReceiverStream::refresh_committed_from_disk() {
  impl_->refresh_committed_from_disk();
}

std::optional<Nack> ChunkedReceiverStream::prepare_commit(const FileCommit& commit,
                                                          ChunkCommitTask& task) {
  if (commit.stream_id != impl_->stream_id) {
    return impl_->nack(commit, NackReason::BadSession, "wrong stream");
  }
  if (commit.seq < impl_->expected_seq) {
    return std::nullopt;
  }
  if (commit.seq > impl_->expected_seq) {
    return impl_->nack(commit, NackReason::BadSeq, "future seq");
  }

  auto it = impl_->active.find(commit.seq);
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

void ChunkedReceiverStream::abort_commit(std::uint64_t seq) noexcept {
  if (!impl_) {
    return;
  }
  auto it = impl_->active.find(seq);
  if (it == impl_->active.end()) {
    return;
  }
  it->second.commit_pending = false;
}

void ChunkedReceiverStream::finish_commit(const ChunkCommitResult& result) {
  if (result.stream_id != impl_->stream_id) {
    throw std::runtime_error("commit result stream mismatch");
  }
  auto it = impl_->active.find(result.seq);
  if (it == impl_->active.end()) {
    throw std::runtime_error("commit result for inactive file");
  }
  const auto& active = it->second;
  if (result.file_id != active.file_id) {
    throw std::runtime_error("commit result file mismatch");
  }
  impl_->current_file_id = active.file_id;
  impl_->current_offset = active.final_size;
  impl_->expected_seq = std::max(impl_->expected_seq, active.seq + 1);
  impl_->active.erase(it);
}

ChunkCommitResult ChunkedReceiverStream::write_commit_task(const ChunkCommitTask& task) {
  if (!checksum_matches(task.file_checksum, task.temp_path)) {
    throw std::runtime_error("final file checksum mismatch");
  }
  fsync_path(task.temp_path, "chunk temp file");

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
  fsync_directory(task.temp_root);

  return ChunkCommitResult{
      .stream_id = task.stream_id,
      .seq = task.seq,
      .file_id = task.file_id,
      .final_size = task.final_size,
      .final_path = task.final_path,
  };
}

std::vector<MissingChunkRange> ChunkedReceiverStream::missing_ranges(std::uint64_t seq,
                                                                     std::uint64_t max_ranges) const {
  std::vector<MissingChunkRange> ranges;
  if (max_ranges == 0) {
    return ranges;
  }

  const auto it = impl_->active.find(seq);
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
        .seq = active.seq,
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
  if (begin.seq < impl_->expected_seq) {
    return std::nullopt;
  }
  if (begin.seq > impl_->expected_seq) {
    return impl_->nack(begin, NackReason::BadSeq, "future seq");
  }
  if (!should_use_chunk_mode(begin.final_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "file size does not require chunk mode");
  }
  if (begin.chunk_size == 0) {
    return impl_->nack(begin, NackReason::BadChunk, "chunk_size must be non-zero");
  }
  if (begin.chunk_count != chunk_count_for_size(begin.final_size, begin.chunk_size)) {
    return impl_->nack(begin, NackReason::BadChunk, "chunk_count does not match final_size");
  }
  if (impl_->active.find(begin.seq) != impl_->active.end()) {
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
  active.seq = begin.seq;
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
  impl_->active.emplace(begin.seq, std::move(active));
  return std::nullopt;
}

std::optional<Nack> ChunkedReceiverStream::apply(const Chunk& chunk) {
  if (chunk.stream_id != impl_->stream_id) {
    return impl_->nack(chunk, NackReason::BadSession, "wrong stream");
  }
  if (chunk.seq < impl_->expected_seq) {
    return std::nullopt;
  }
  if (chunk.seq > impl_->expected_seq) {
    return impl_->nack(chunk, NackReason::BadSeq, "future seq");
  }

  auto it = impl_->active.find(chunk.seq);
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
  return std::nullopt;
}

std::optional<Nack> ChunkedReceiverStream::apply(const FileCommit& commit) {
  ChunkCommitTask task;
  auto result = prepare_commit(commit, task);
  if (result.has_value() || task.seq == 0) {
    return result;
  }
  try {
    finish_commit(ChunkedReceiverStream::write_commit_task(task));
  } catch (const std::exception& ex) {
    abort_commit(commit.seq);
    return impl_->nack(commit, NackReason::IoError, ex.what());
  }
  return std::nullopt;
}

}  // namespace yisync
