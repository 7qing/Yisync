#pragma once

#include "core/yisync_protocol.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace yisync {

using LineId = std::uint32_t;

struct T_TokenBucketConfig {
  std::uint64_t tokens_per_tick = 20 * 1024;
  std::uint64_t capacity = 20 * 1024;
  std::chrono::milliseconds tick{10};
};

class TokenBucket {
 public:
  TokenBucket() = default;
  explicit TokenBucket(T_TokenBucketConfig config);

  std::uint64_t available() const noexcept;
  std::uint64_t capacity() const noexcept;
  std::uint64_t tokens_per_tick() const noexcept;

  void refill_ticks(std::uint64_t ticks);
  bool can_consume(std::uint64_t bytes) const noexcept;
  bool try_consume(std::uint64_t bytes) noexcept;

 private:
  T_TokenBucketConfig config_;
  std::uint64_t tokens_ = 0;
};

struct T_LineConfig {
  LineId id = 0;
  std::string name;
  T_TokenBucketConfig limiter;
  std::uint64_t initial_recv_window_bytes = 0;
  std::uint64_t heartbeat_timeout_ticks = 300;
  bool initially_connected = true;
};

struct T_LineSnapshot {
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

enum class EM_SendKind : std::uint8_t {
  CREATE = 1,
  DATA = 2,
  FILE_BEGIN = 3,
  CHUNK = 4,
  FILE_COMMIT = 5,
};

struct T_SendRequest {
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t end_offset = 0;
  std::uint64_t bytes = 0;
  bool split_allowed = false;
  EM_SendKind kind = EM_SendKind::DATA;
  std::uint64_t chunk_index = 0;
};

struct T_SendGrant {
  LineId line_id = 0;
  std::uint64_t bytes = 0;
};

struct T_LostSend {
  LineId line_id = 0;
  EM_SendKind kind = EM_SendKind::DATA;
  std::uint64_t stream_id = 0;
  std::uint64_t file_id = 0;
  std::uint64_t seq = 0;
  std::uint64_t offset = 0;
  std::uint64_t end_offset = 0;
  std::uint64_t chunk_index = 0;
  std::uint64_t bytes = 0;
};

class T_MultiLineScheduler {
 public:
  explicit T_MultiLineScheduler(std::vector<T_LineConfig> configs);

  void refill_ticks(std::uint64_t ticks);
  std::optional<T_SendGrant> try_acquire(const T_SendRequest& request);
  void on_line_connected(LineId line_id);
  void on_line_negotiated(LineId line_id, std::uint64_t recv_window_bytes);
  void on_line_disconnected(LineId line_id);
  void on_line_protocol_error(LineId line_id);
  void on_line_failure(LineId line_id);
  void on_heartbeat(LineId line_id, const T_Heartbeat& heartbeat);
  void on_nack(LineId line_id);

  std::optional<LineId> choose_control_line() const;
  std::vector<T_LineSnapshot> snapshots() const;
  std::vector<T_LostSend> take_lost_sends();

 private:
  struct LineState {
    struct T_PendingSend {
      EM_SendKind kind = EM_SendKind::DATA;
      std::uint64_t stream_id = 0;
      std::uint64_t file_id = 0;
      std::uint64_t seq = 0;
      std::uint64_t offset = 0;
      std::uint64_t end_offset = 0;
      std::uint64_t chunk_index = 0;
      std::uint64_t bytes = 0;
    };

    T_LineConfig config;
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
    std::vector<T_PendingSend> pending;
  };

  std::vector<LineState> lines_;
  std::vector<T_LostSend> lost_sends_;

  LineState* find_line(LineId line_id);
  const LineState* find_line(LineId line_id) const;
  static std::uint64_t score_line(const LineState& line, const T_SendRequest& request);
  void clear_inflight(LineState& line);
};

}  // namespace yisync
