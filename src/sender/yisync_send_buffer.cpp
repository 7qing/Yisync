#include "sender/yisync_send_buffer.hpp"

#include <algorithm>
#include <functional>
#include <utility>

namespace yisync {

std::size_t T_SendBufferKeyHash::operator()(const T_SendBufferKey& key) const noexcept {
  std::size_t hash = static_cast<std::size_t>(key.kind);
  const auto mix = [&hash](std::uint64_t value) {
    hash ^= std::hash<std::uint64_t>{}(value) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
  };
  mix(key.stream_id);
  mix(key.file_id);
  mix(key.seq);
  mix(key.offset);
  mix(key.chunk_index);
  return hash;
}

T_SendBufferKey T_SenderSendBuffer::key_for_request(const T_SendRequest& request) {
  return T_SendBufferKey{
      .kind = request.kind,
      .stream_id = request.stream_id,
      .file_id = request.file_id,
      .seq = request.seq,
      .offset = request.offset,
      .chunk_index = request.chunk_index,
  };
}

T_SendBufferKey T_SenderSendBuffer::key_for_lost_send(const T_LostSend& lost) {
  return T_SendBufferKey{
      .kind = lost.kind,
      .stream_id = lost.stream_id,
      .file_id = lost.file_id,
      .seq = lost.seq,
      .offset = lost.offset,
      .chunk_index = lost.chunk_index,
  };
}

void T_SenderSendBuffer::clear() {
  entries_.clear();
  retransmit_queue_.clear();
}

void T_SenderSendBuffer::remember(T_Message message, T_SendRequest request, std::uint64_t sent_tick) {
  entries_[key_for_request(request)] = T_BufferedSend{
      .message = std::move(message),
      .request = std::move(request),
      .last_sent_tick = sent_tick,
      .nack_retries = 0,
  };
}

std::vector<T_AckedSendSample> T_SenderSendBuffer::erase_completed(const T_Heartbeat& heartbeat,
                                                               std::uint64_t current_tick) {
  std::vector<T_AckedSendSample> samples;
  const auto stream_id = heartbeat.stream_id;
  const auto next_seq = heartbeat.next_seq;
  if (next_seq == 0) {
    return samples;
  }
  for (auto it = entries_.begin(); it != entries_.end();) {
    const auto& key = it->first;
    const bool same_stream_file = key.stream_id == stream_id &&
                                  key.file_id == heartbeat.file_id;
    bool acked = key.stream_id == stream_id && key.seq < next_seq;
    if (!acked && same_stream_file && key.kind == EM_SendKind::DATA) {
      const auto end_offset = it->second.request.end_offset;
      acked = end_offset != 0 &&
              (heartbeat.offset >= end_offset || heartbeat.durable_offset >= end_offset);
    }
    if (acked) {
      samples.push_back(sample_for(key, it->second, current_tick));
      it = entries_.erase(it);
    } else {
      ++it;
    }
  }
  return samples;
}

std::optional<T_AckedSendSample> T_SenderSendBuffer::erase_file_begin(std::uint64_t stream_id,
                                                                  std::uint64_t file_id,
                                                                  std::uint64_t seq,
                                                                  std::uint64_t current_tick) {
  const T_SendBufferKey key{
      .kind = EM_SendKind::FILE_BEGIN,
      .stream_id = stream_id,
      .file_id = file_id,
      .seq = seq,
      .offset = 0,
      .chunk_index = 0,
  };
  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return std::nullopt;
  }
  auto sample = sample_for(key, it->second, current_tick);
  entries_.erase(it);
  return sample;
}

std::optional<T_AckedSendSample> T_SenderSendBuffer::erase_chunk(const T_ReceivedChunk& chunk,
                                                            std::uint64_t stream_id,
                                                            std::uint64_t current_tick) {
  const T_SendBufferKey key{
      .kind = EM_SendKind::CHUNK,
      .stream_id = stream_id,
      .file_id = chunk.file_id,
      .seq = chunk.seq,
      .offset = 0,
      .chunk_index = chunk.chunk_index,
  };
  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return std::nullopt;
  }
  auto sample = sample_for(key, it->second, current_tick);
  entries_.erase(it);
  return sample;
}

std::optional<T_SendBufferKey> T_SenderSendBuffer::key_for_nack(const T_Nack& nack,
                                                            std::uint64_t chunk_size) const {
  if (nack.file_id == 0) {
    return std::nullopt;
  }

  const auto find_ordered = [&](std::uint64_t seq) -> std::optional<T_SendBufferKey> {
    if (seq == 0) {
      return std::nullopt;
    }
    auto create = T_SendBufferKey{
        .kind = EM_SendKind::CREATE,
        .stream_id = nack.stream_id,
        .file_id = nack.file_id,
        .seq = seq,
        .offset = 0,
        .chunk_index = 0,
    };
    if (entries_.find(create) != entries_.end()) {
      return create;
    }
    auto data = create;
    data.kind = EM_SendKind::DATA;
    data.offset = nack.offset;
    if (entries_.find(data) != entries_.end()) {
      return data;
    }
    return std::nullopt;
  };

  const auto find_file_begin = [&](std::uint64_t seq) -> std::optional<T_SendBufferKey> {
    if (seq == 0) {
      return std::nullopt;
    }
    auto begin = T_SendBufferKey{
        .kind = EM_SendKind::FILE_BEGIN,
        .stream_id = nack.stream_id,
        .file_id = nack.file_id,
        .seq = seq,
        .offset = 0,
        .chunk_index = 0,
    };
    if (entries_.find(begin) != entries_.end()) {
      return begin;
    }
    return std::nullopt;
  };

  const auto find_file_commit = [&](std::uint64_t seq) -> std::optional<T_SendBufferKey> {
    if (seq == 0) {
      return std::nullopt;
    }
    auto commit = T_SendBufferKey{
        .kind = EM_SendKind::FILE_COMMIT,
        .stream_id = nack.stream_id,
        .file_id = nack.file_id,
        .seq = seq,
        .offset = 0,
        .chunk_index = 0,
    };
    if (entries_.find(commit) != entries_.end()) {
      return commit;
    }
    return std::nullopt;
  };

  const auto find_chunk = [&](std::uint64_t seq) -> std::optional<T_SendBufferKey> {
    if (seq == 0) {
      return std::nullopt;
    }
    const auto chunk_index = chunk_size == 0 ? 0 : nack.offset / chunk_size;
    auto chunk = T_SendBufferKey{
        .kind = EM_SendKind::CHUNK,
        .stream_id = nack.stream_id,
        .file_id = nack.file_id,
        .seq = seq,
        .offset = 0,
        .chunk_index = chunk_index,
    };
    if (entries_.find(chunk) != entries_.end()) {
      return chunk;
    }
    return std::nullopt;
  };

  if (nack.reason == EM_NackReason::BAD_SEQ) {
    if (auto key = find_ordered(nack.expected_seq)) {
      return key;
    }
    if (auto key = find_file_begin(nack.expected_seq)) {
      return key;
    }
    if (auto key = find_file_commit(nack.expected_seq)) {
      return key;
    }
    if (auto key = find_chunk(nack.expected_seq)) {
      return key;
    }
  }

  if (nack.reason == EM_NackReason::BAD_CHECKSUM ||
      nack.reason == EM_NackReason::BAD_OFFSET) {
    if (auto key = find_chunk(nack.got_seq)) {
      return key;
    }
    if (auto key = find_ordered(nack.got_seq)) {
      return key;
    }
  }

  if (nack.reason == EM_NackReason::BAD_CHUNK) {
    if (auto key = find_file_begin(nack.got_seq)) {
      return key;
    }
    if (auto key = find_chunk(nack.got_seq)) {
      return key;
    }
  }

  if (nack.reason == EM_NackReason::BAD_COMMIT) {
    if (auto key = find_file_commit(nack.got_seq)) {
      return key;
    }
  }

  auto ordered = T_SendBufferKey{
      .kind = EM_SendKind::CREATE,
      .stream_id = nack.stream_id,
      .file_id = nack.file_id,
      .seq = nack.got_seq,
      .offset = 0,
      .chunk_index = 0,
  };
  if (entries_.find(ordered) != entries_.end()) {
    return ordered;
  }
  ordered.kind = EM_SendKind::DATA;
  ordered.offset = nack.offset;
  if (entries_.find(ordered) != entries_.end()) {
    return ordered;
  }

  auto begin = ordered;
  begin.kind = EM_SendKind::FILE_BEGIN;
  begin.offset = 0;
  if (entries_.find(begin) != entries_.end()) {
    return begin;
  }

  auto commit = ordered;
  commit.kind = EM_SendKind::FILE_COMMIT;
  commit.offset = 0;
  if (entries_.find(commit) != entries_.end()) {
    return commit;
  }

  if (nack.got_seq != 0) {
    const auto chunk_index = chunk_size == 0 ? 0 : nack.offset / chunk_size;
    auto chunk = ordered;
    chunk.kind = EM_SendKind::CHUNK;
    chunk.offset = 0;
    chunk.chunk_index = chunk_index;
    if (entries_.find(chunk) != entries_.end()) {
      return chunk;
    }
  }

  return std::nullopt;
}

bool T_SenderSendBuffer::enqueue_retransmit(const T_SendBufferKey& key) {
  if (entries_.find(key) == entries_.end()) {
    return false;
  }
  if (std::find(retransmit_queue_.begin(), retransmit_queue_.end(), key) == retransmit_queue_.end()) {
    retransmit_queue_.push_back(key);
  }
  return true;
}

bool T_SenderSendBuffer::enqueue_nack_retransmit(const T_Nack& nack, std::uint64_t chunk_size) {
  const auto key = key_for_nack(nack, chunk_size);
  if (!key.has_value()) {
    return false;
  }
  return enqueue_retransmit(*key);
}

bool T_SenderSendBuffer::mark_retransmitted(const T_SendBufferKey& key,
                                          std::uint64_t bytes,
                                          std::uint64_t sent_tick) {
  auto* send = find(key);
  if (send == nullptr) {
    return false;
  }
  send->request.bytes = bytes;
  send->last_sent_tick = sent_tick;
  send->nack_retries += 1;
  return true;
}

T_BufferedSend* T_SenderSendBuffer::find(const T_SendBufferKey& key) {
  auto it = entries_.find(key);
  return it == entries_.end() ? nullptr : &it->second;
}

const T_BufferedSend* T_SenderSendBuffer::find(const T_SendBufferKey& key) const {
  auto it = entries_.find(key);
  return it == entries_.end() ? nullptr : &it->second;
}

std::vector<T_SendBufferKey> T_SenderSendBuffer::expired_keys(std::uint64_t current_tick,
                                                          std::uint64_t rto_ticks,
                                                          std::size_t max_keys) const {
  std::vector<T_SendBufferKey> result;
  if (rto_ticks == 0 || max_keys == 0) {
    return result;
  }
  result.reserve(std::min(max_keys, entries_.size()));
  for (const auto& [key, send] : entries_) {
    if (current_tick < send.last_sent_tick) {
      continue;
    }
    if (current_tick - send.last_sent_tick < rto_ticks) {
      continue;
    }
    result.push_back(key);
    if (result.size() >= max_keys) {
      break;
    }
  }
  return result;
}

std::vector<T_SendBufferKey>& T_SenderSendBuffer::retransmit_queue() noexcept {
  return retransmit_queue_;
}

void T_SenderSendBuffer::erase_retransmit(std::size_t index) {
  if (index >= retransmit_queue_.size()) {
    return;
  }
  retransmit_queue_.erase(retransmit_queue_.begin() + static_cast<std::ptrdiff_t>(index));
}

std::size_t T_SenderSendBuffer::size() const noexcept {
  return entries_.size();
}

std::size_t T_SenderSendBuffer::retransmit_queue_size() const noexcept {
  return retransmit_queue_.size();
}

T_AckedSendSample T_SenderSendBuffer::sample_for(const T_SendBufferKey& key,
                                             const T_BufferedSend& send,
                                             std::uint64_t current_tick) {
  return T_AckedSendSample{
      .key = key,
      .request = send.request,
      .rtt_ticks = current_tick >= send.last_sent_tick ? current_tick - send.last_sent_tick : 0,
  };
}

}  // namespace yisync
