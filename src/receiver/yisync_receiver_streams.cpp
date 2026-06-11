#include "receiver/yisync_receiver_streams.hpp"

#include <utility>

namespace yisync {

T_ReceiverStreamContext::T_ReceiverStreamContext(T_ReceiverStreamContext&& other) noexcept
    : stream_id(other.stream_id),
      root(std::move(other.root)),
      append(std::move(other.append)),
      chunk(std::move(other.chunk)),
      append_durable_file_id(other.append_durable_file_id),
      append_durable_path(std::move(other.append_durable_path)),
      append_durable_offset(other.append_durable_offset),
      append_durable_target(other.append_durable_target),
      append_fsync_active_target(other.append_fsync_active_target),
      append_heartbeat_line_id(other.append_heartbeat_line_id),
      append_fsync_queued(other.append_fsync_queued),
      append_fsync_completed(other.append_fsync_completed.load(std::memory_order_relaxed)),
      append_fsync_failed(other.append_fsync_failed.load(std::memory_order_relaxed)),
      append_fsync_error(other.append_fsync_error),
      chunk_commit_queued(other.chunk_commit_queued),
      chunk_commit_line_id(other.chunk_commit_line_id),
      chunk_commit_seq(other.chunk_commit_seq),
      chunk_commit_file_id(other.chunk_commit_file_id),
      chunk_commit_final_size(other.chunk_commit_final_size),
      chunk_commit_final_path(std::move(other.chunk_commit_final_path)),
      chunk_commit_completion(std::move(other.chunk_commit_completion)) {}

T_ReceiverStreamContext& T_ReceiverStreamContext::operator=(T_ReceiverStreamContext&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  stream_id = other.stream_id;
  root = std::move(other.root);
  append = std::move(other.append);
  chunk = std::move(other.chunk);
  append_durable_file_id = other.append_durable_file_id;
  append_durable_path = std::move(other.append_durable_path);
  append_durable_offset = other.append_durable_offset;
  append_durable_target = other.append_durable_target;
  append_fsync_active_target = other.append_fsync_active_target;
  append_heartbeat_line_id = other.append_heartbeat_line_id;
  append_fsync_queued = other.append_fsync_queued;
  append_fsync_completed.store(other.append_fsync_completed.load(std::memory_order_relaxed),
                               std::memory_order_relaxed);
  append_fsync_failed.store(other.append_fsync_failed.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
  append_fsync_error = other.append_fsync_error;
  chunk_commit_queued = other.chunk_commit_queued;
  chunk_commit_line_id = other.chunk_commit_line_id;
  chunk_commit_seq = other.chunk_commit_seq;
  chunk_commit_file_id = other.chunk_commit_file_id;
  chunk_commit_final_size = other.chunk_commit_final_size;
  chunk_commit_final_path = std::move(other.chunk_commit_final_path);
  chunk_commit_completion = std::move(other.chunk_commit_completion);
  return *this;
}

T_ReceiverStreamMap::T_ReceiverStreamMap(std::uint64_t default_stream_id,
                                     std::filesystem::path default_root,
                                     std::unordered_map<std::uint64_t, std::filesystem::path>* roots)
    : default_root_(std::move(default_root)),
      default_stream_id_(default_stream_id),
      roots_(roots) {}

std::filesystem::path T_ReceiverStreamMap::stream_root_for(std::uint64_t stream_id) const {
  if (roots_ != nullptr) {
    const auto it = roots_->find(stream_id);
    if (it != roots_->end()) {
      return it->second;
    }
  }
  if (stream_id == default_stream_id_) {
    return default_root_;
  }
  return default_root_ / std::to_string(stream_id);
}

T_ReceiverStreamContext& T_ReceiverStreamMap::context_for(std::uint64_t stream_id) {
  auto it = streams_.find(stream_id);
  if (it != streams_.end()) {
    return it->second;
  }
  T_ReceiverStreamContext context;
  context.stream_id = stream_id;
  context.root = stream_root_for(stream_id);
  std::filesystem::create_directories(context.root);
  auto [inserted, _] = streams_.emplace(stream_id, std::move(context));
  return inserted->second;
}

T_ReceiverStream& T_ReceiverStreamMap::append_receiver_for(std::uint64_t stream_id) {
  auto& context = context_for(stream_id);
  if (!context.append) {
    context.append = std::make_unique<T_ReceiverStream>(stream_id, context.root);
  }
  return *context.append;
}

T_ChunkedReceiverStream& T_ReceiverStreamMap::chunk_receiver_for(std::uint64_t stream_id) {
  auto& context = context_for(stream_id);
  if (!context.chunk) {
    context.chunk = std::make_unique<T_ChunkedReceiverStream>(stream_id, context.root);
  }
  return *context.chunk;
}

bool T_ReceiverStreamMap::append_durable_idle() const {
  for (const auto& [stream_id, context] : streams_) {
    (void)stream_id;
    if (context.append_fsync_queued || context.append_durable_target > context.append_durable_offset) {
      return false;
    }
    if (context.chunk_commit_queued) {
      return false;
    }
  }
  return true;
}

bool T_ReceiverStreamMap::has_pending_chunk_commit() const {
  for (const auto& [stream_id, context] : streams_) {
    (void)stream_id;
    if (context.chunk_commit_queued) {
      return true;
    }
  }
  return false;
}

T_ReceiverStreamMap::iterator T_ReceiverStreamMap::begin() noexcept {
  return streams_.begin();
}

T_ReceiverStreamMap::iterator T_ReceiverStreamMap::end() noexcept {
  return streams_.end();
}

T_ReceiverStreamMap::const_iterator T_ReceiverStreamMap::begin() const noexcept {
  return streams_.begin();
}

T_ReceiverStreamMap::const_iterator T_ReceiverStreamMap::end() const noexcept {
  return streams_.end();
}

}  // namespace yisync
