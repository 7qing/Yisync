#pragma once

#include "core/yisync_protocol.hpp"

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
  std::uint64_t heartbeat_timeout_ticks = 300;
  bool initially_connected = true;
};

struct LineSnapshot {
  LineId id = 0;
  std::string name;
  std::uint64_t tokens = 0;
  std::uint64_t inflight_bytes = 0;
  std::uint64_t recv_window_bytes = 0;
  bool connected = true;
  bool negotiated = false;
  bool healthy = true;
  bool stale = false;
  std::uint64_t missed_heartbeat_ticks = 0;
  std::uint64_t consecutive_failures = 0;
  std::uint64_t pending_sends = 0;
  std::uint64_t last_completed_bytes = 0;
};

enum class SendKind : std::uint8_t {
  Create = 1,
  Data = 2,
  FileBegin = 3,
  Chunk = 4,
  FileCommit = 5,
};

struct SendRequest {
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t end_offset = 0;
  std::uint64_t bytes = 0;
  bool split_allowed = false;
  SendKind kind = SendKind::Data;
  std::uint64_t chunk_index = 0;
};

struct SendGrant {
  LineId line_id = 0;
  std::uint64_t bytes = 0;
};

struct LostSend {
  LineId line_id = 0;
  SendKind kind = SendKind::Data;
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t end_offset = 0;
  std::uint64_t chunk_index = 0;
  std::uint64_t bytes = 0;
};

class MultiLineScheduler {
 public:
  explicit MultiLineScheduler(std::vector<LineConfig> configs);

  void refill_ticks(std::uint64_t ticks);
  std::optional<SendGrant> try_acquire(const SendRequest& request);
  void on_line_connected(LineId line_id);
  void on_line_negotiated(LineId line_id, std::uint64_t recv_window_bytes);
  void on_line_disconnected(LineId line_id);
  void on_line_protocol_error(LineId line_id);
  void on_line_failure(LineId line_id);
  void on_heartbeat(LineId line_id, const Heartbeat& heartbeat);
  void on_nack(LineId line_id);

  std::optional<LineId> choose_control_line() const;
  std::vector<LineSnapshot> snapshots() const;
  std::vector<LostSend> take_lost_sends();

 private:
  struct LineState {
    struct PendingSend {
      SendKind kind = SendKind::Data;
      std::uint64_t stream_id = 0;
      std::uint64_t file_id = 0;
      std::uint64_t seq = 0;
      std::uint64_t offset = 0;
      std::uint64_t end_offset = 0;
      std::uint64_t chunk_index = 0;
      std::uint64_t bytes = 0;
    };

    LineConfig config;
    TokenBucket bucket;
    std::uint64_t inflight_bytes = 0;
    std::uint64_t recv_window_bytes = 0;
    std::uint64_t missed_heartbeat_ticks = 0;
    std::uint64_t consecutive_failures = 0;
    std::uint64_t last_completed_bytes = 0;
    bool connected = true;
    bool negotiated = false;
    bool healthy = true;
    bool stale = false;
    std::vector<PendingSend> pending;
  };

  std::vector<LineState> lines_;
  std::vector<LostSend> lost_sends_;

  LineState* find_line(LineId line_id);
  const LineState* find_line(LineId line_id) const;
  static std::uint64_t score_line(const LineState& line, const SendRequest& request);
  void clear_inflight(LineState& line);
};

}  // namespace yisync
