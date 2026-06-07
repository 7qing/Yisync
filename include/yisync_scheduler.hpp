#pragma once

#include "yisync_protocol.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace yisync {

using LineId = std::uint32_t;

struct TokenBucketConfig {
  std::uint64_t tokens_per_tick = 20 * 1024;
  std::uint64_t capacity = 20 * 1024;
  std::chrono::milliseconds tick{10};
};

class TokenBucket {
 public:
  TokenBucket() = default;
  explicit TokenBucket(TokenBucketConfig config);

  std::uint64_t available() const noexcept;
  std::uint64_t capacity() const noexcept;
  std::uint64_t tokens_per_tick() const noexcept;

  void refill_ticks(std::uint64_t ticks);
  bool can_consume(std::uint64_t bytes) const noexcept;
  bool try_consume(std::uint64_t bytes) noexcept;

 private:
  TokenBucketConfig config_;
  std::uint64_t tokens_ = 0;
};

struct LineConfig {
  LineId id = 0;
  std::string name;
  TokenBucketConfig limiter;
  std::uint64_t initial_recv_window_bytes = 0;
};

struct LineSnapshot {
  LineId id = 0;
  std::string name;
  std::uint64_t tokens = 0;
  std::uint64_t inflight_bytes = 0;
  std::uint64_t recv_window_bytes = 0;
};

enum class SendKind : std::uint8_t {
  Ordered = 1,
  Chunk = 2,
};

struct SendRequest {
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t bytes = 0;
  bool split_allowed = false;
  SendKind kind = SendKind::Ordered;
  std::uint64_t order_seq = 0;
  std::uint64_t chunk_index = 0;
};

struct SendGrant {
  LineId line_id = 0;
  std::uint64_t bytes = 0;
};

class MultiLineScheduler {
 public:
  explicit MultiLineScheduler(std::vector<LineConfig> configs);

  void refill_ticks(std::uint64_t ticks);
  std::optional<SendGrant> try_acquire(const SendRequest& request);
  void on_heartbeat(LineId line_id, const Heartbeat& heartbeat);
  void on_nack(LineId line_id);

  std::vector<LineSnapshot> snapshots() const;

 private:
  struct LineState {
    struct PendingSend {
      SendKind kind = SendKind::Ordered;
      std::uint64_t stream_id = 0;
      std::uint64_t file_id = 0;
      std::uint64_t seq = 0;
      std::uint64_t order_seq = 0;
      std::uint64_t chunk_index = 0;
      std::uint64_t bytes = 0;
    };

    LineConfig config;
    TokenBucket bucket;
    std::uint64_t inflight_bytes = 0;
    std::uint64_t recv_window_bytes = 0;
    bool healthy = true;
    std::vector<PendingSend> pending;
  };

  std::vector<LineState> lines_;

  LineState* find_line(LineId line_id);
};

}  // namespace yisync
