#pragma once

#include "core/yisync_protocol.hpp"
#include "network/yisync_scheduler.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace yisync {

struct T_SendBufferKey {
  EM_SendKind kind = EM_SendKind::DATA;
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t chunk_index = 0;

  bool operator==(const T_SendBufferKey& other) const noexcept {
    return kind == other.kind &&
           stream_id == other.stream_id &&
           file_id == other.file_id &&
           seq == other.seq &&
           offset == other.offset &&
           chunk_index == other.chunk_index;
  }
};

struct T_SendBufferKeyHash {
  std::size_t operator()(const T_SendBufferKey& key) const noexcept;
};

struct T_BufferedSend {
  T_Message message;
  T_SendRequest request;
  std::uint64_t last_sent_tick = 0;
  std::uint64_t nack_retries = 0;
};

struct T_AckedSendSample {
  T_SendBufferKey key;
  T_SendRequest request;
  std::uint64_t rtt_ticks = 0;
};

class T_SenderSendBuffer {
 public:
  static T_SendBufferKey key_for_request(const T_SendRequest& request);
  static T_SendBufferKey key_for_lost_send(const T_LostSend& lost);

  void clear();
  void remember(T_Message message, T_SendRequest request, std::uint64_t sent_tick);
  std::vector<T_AckedSendSample> erase_completed(const T_Heartbeat& heartbeat,
                                               std::uint64_t current_tick);
  std::optional<T_AckedSendSample> erase_file_begin(std::uint64_t stream_id,
                                                  std::uint64_t file_id,
                                                  std::uint64_t seq,
                                                  std::uint64_t current_tick);
  std::optional<T_AckedSendSample> erase_chunk(const T_ReceivedChunk& chunk,
                                             std::uint64_t stream_id,
                                             std::uint64_t current_tick);

  std::optional<T_SendBufferKey> key_for_nack(const T_Nack& nack, std::uint64_t chunk_size) const;
  bool enqueue_retransmit(const T_SendBufferKey& key);
  bool enqueue_nack_retransmit(const T_Nack& nack, std::uint64_t chunk_size);
  bool mark_retransmitted(const T_SendBufferKey& key,
                          std::uint64_t bytes,
                          std::uint64_t sent_tick);

  T_BufferedSend* find(const T_SendBufferKey& key);
  const T_BufferedSend* find(const T_SendBufferKey& key) const;
  std::vector<T_SendBufferKey> expired_keys(std::uint64_t current_tick,
                                          std::uint64_t rto_ticks,
                                          std::size_t max_keys) const;
  std::vector<T_SendBufferKey>& retransmit_queue() noexcept;
  void erase_retransmit(std::size_t index);
  std::size_t size() const noexcept;
  std::size_t retransmit_queue_size() const noexcept;

 private:
  std::unordered_map<T_SendBufferKey, T_BufferedSend, T_SendBufferKeyHash> entries_;
  std::vector<T_SendBufferKey> retransmit_queue_;

  static T_AckedSendSample sample_for(const T_SendBufferKey& key,
                                    const T_BufferedSend& send,
                                    std::uint64_t current_tick);
};

}  // namespace yisync
