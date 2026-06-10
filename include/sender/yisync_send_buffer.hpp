#pragma once

#include "core/yisync_protocol.hpp"
#include "network/yisync_scheduler.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace yisync {

struct SendBufferKey {
  SendKind kind = SendKind::Data;
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t chunk_index = 0;

  bool operator==(const SendBufferKey& other) const noexcept {
    return kind == other.kind &&
           stream_id == other.stream_id &&
           file_id == other.file_id &&
           seq == other.seq &&
           offset == other.offset &&
           chunk_index == other.chunk_index;
  }
};

struct SendBufferKeyHash {
  std::size_t operator()(const SendBufferKey& key) const noexcept;
};

struct BufferedSend {
  Message message;
  SendRequest request;
  std::uint64_t last_sent_tick = 0;
  std::uint64_t nack_retries = 0;
};

struct AckedSendSample {
  SendBufferKey key;
  SendRequest request;
  std::uint64_t rtt_ticks = 0;
};

class SenderSendBuffer {
 public:
  static SendBufferKey key_for_request(const SendRequest& request);
  static SendBufferKey key_for_lost_send(const LostSend& lost);

  void clear();
  void remember(Message message, SendRequest request, std::uint64_t sent_tick);
  std::vector<AckedSendSample> erase_completed(const Heartbeat& heartbeat,
                                               std::uint64_t current_tick);
  std::optional<AckedSendSample> erase_file_begin(std::uint64_t stream_id,
                                                  std::uint64_t file_id,
                                                  std::uint64_t seq,
                                                  std::uint64_t current_tick);
  std::optional<AckedSendSample> erase_chunk(const ReceivedChunk& chunk,
                                             std::uint64_t stream_id,
                                             std::uint64_t current_tick);

  std::optional<SendBufferKey> key_for_nack(const Nack& nack, std::uint64_t chunk_size) const;
  bool enqueue_retransmit(const SendBufferKey& key);
  bool enqueue_nack_retransmit(const Nack& nack, std::uint64_t chunk_size);
  bool mark_retransmitted(const SendBufferKey& key,
                          std::uint64_t bytes,
                          std::uint64_t sent_tick);

  BufferedSend* find(const SendBufferKey& key);
  const BufferedSend* find(const SendBufferKey& key) const;
  std::vector<SendBufferKey> expired_keys(std::uint64_t current_tick,
                                          std::uint64_t rto_ticks,
                                          std::size_t max_keys) const;
  std::vector<SendBufferKey>& retransmit_queue() noexcept;
  void erase_retransmit(std::size_t index);
  std::size_t size() const noexcept;
  std::size_t retransmit_queue_size() const noexcept;

 private:
  std::unordered_map<SendBufferKey, BufferedSend, SendBufferKeyHash> entries_;
  std::vector<SendBufferKey> retransmit_queue_;

  static AckedSendSample sample_for(const SendBufferKey& key,
                                    const BufferedSend& send,
                                    std::uint64_t current_tick);
};

}  // namespace yisync
