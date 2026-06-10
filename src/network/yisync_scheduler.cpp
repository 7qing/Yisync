#include "network/yisync_scheduler.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace yisync {

TokenBucket::TokenBucket(TokenBucketConfig config)
    : config_(config), tokens_(config.capacity) {
  if (config_.tick.count() <= 0) {
    throw std::invalid_argument("token bucket tick must be positive");
  }
  if (config_.capacity == 0) {
    throw std::invalid_argument("token bucket capacity must be positive");
  }
  if (config_.tokens_per_tick == 0) {
    throw std::invalid_argument("token bucket tokens_per_tick must be positive");
  }
}

std::uint64_t TokenBucket::available() const noexcept {
  return tokens_;
}

std::uint64_t TokenBucket::capacity() const noexcept {
  return config_.capacity;
}

std::uint64_t TokenBucket::tokens_per_tick() const noexcept {
  return config_.tokens_per_tick;
}

void TokenBucket::refill_ticks(std::uint64_t ticks) {
  if (ticks == 0) {
    return;
  }
  const auto added = ticks > (UINT64_MAX / config_.tokens_per_tick)
                         ? UINT64_MAX
                         : ticks * config_.tokens_per_tick;
  tokens_ = std::min(config_.capacity, tokens_ > UINT64_MAX - added ? UINT64_MAX : tokens_ + added);
}

bool TokenBucket::can_consume(std::uint64_t bytes) const noexcept {
  return bytes <= tokens_;
}

bool TokenBucket::try_consume(std::uint64_t bytes) noexcept {
  if (!can_consume(bytes)) {
    return false;
  }
  tokens_ -= bytes;
  return true;
}

MultiLineScheduler::MultiLineScheduler(std::vector<LineConfig> configs) {
  if (configs.empty()) {
    throw std::invalid_argument("at least one line is required");
  }

  lines_.reserve(configs.size());
  for (auto& config : configs) {
    if (config.id == 0) {
      throw std::invalid_argument("line id must be non-zero");
    }
    if (find_line(config.id) != nullptr) {
      throw std::invalid_argument("duplicate line id");
    }
    LineState state;
    state.recv_window_bytes = config.initial_recv_window_bytes;
    state.connected = config.initially_connected;
    state.negotiated = config.initially_connected;
    state.healthy = config.initially_connected;
    state.bucket = TokenBucket(config.limiter);
    state.config = std::move(config);
    lines_.push_back(std::move(state));
  }
}

void MultiLineScheduler::refill_ticks(std::uint64_t ticks) {
  for (auto& line : lines_) {
    line.bucket.refill_ticks(ticks);
    if (!line.connected || ticks == 0) {
      continue;
    }
    line.missed_heartbeat_ticks =
        line.missed_heartbeat_ticks > UINT64_MAX - ticks ? UINT64_MAX : line.missed_heartbeat_ticks + ticks;
    if (!line.pending.empty() &&
        line.config.heartbeat_timeout_ticks > 0 &&
        line.missed_heartbeat_ticks > line.config.heartbeat_timeout_ticks) {
      line.stale = true;
    }
  }
}

std::optional<SendGrant> MultiLineScheduler::try_acquire(const SendRequest& request) {
  if (request.bytes == 0) {
    return std::nullopt;
  }

  LineState* best = nullptr;
  std::uint64_t best_score = 0;

  for (auto& line : lines_) {
    if (!line.connected || !line.negotiated || !line.healthy || line.stale) {
      continue;
    }
    if (line.recv_window_bytes <= line.inflight_bytes) {
      continue;
    }

    const auto window_headroom = line.recv_window_bytes - line.inflight_bytes;
    if (window_headroom < request.bytes) {
      continue;
    }
    if (!line.bucket.can_consume(request.bytes)) {
      continue;
    }

    const auto score = score_line(line, request);
    if (best == nullptr || score > best_score) {
      best = &line;
      best_score = score;
    }
  }

  if (best == nullptr) {
    return std::nullopt;
  }

  if (!best->bucket.try_consume(request.bytes)) {
    return std::nullopt;
  }
  if (best->pending.empty()) {
    best->missed_heartbeat_ticks = 0;
    best->stale = false;
  }
  best->inflight_bytes += request.bytes;
  best->pending.push_back(LineState::PendingSend{
      .kind = request.kind,
      .stream_id = request.stream_id,
      .file_id = request.file_id,
      .seq = request.seq,
      .offset = request.offset,
      .end_offset = request.end_offset,
      .chunk_index = request.chunk_index,
      .bytes = request.bytes,
  });
  return SendGrant{best->config.id, request.bytes};
}

void MultiLineScheduler::on_line_connected(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->connected = true;
  line->negotiated = false;
  line->healthy = false;
  line->stale = false;
  line->missed_heartbeat_ticks = 0;
  line->last_completed_bytes = 0;
  line->recv_window_bytes = line->config.initial_recv_window_bytes;
  clear_inflight(*line);
}

void MultiLineScheduler::on_line_negotiated(LineId line_id, std::uint64_t recv_window_bytes) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->connected = true;
  line->negotiated = true;
  line->healthy = true;
  line->stale = false;
  line->missed_heartbeat_ticks = 0;
  line->consecutive_failures = 0;
  line->last_completed_bytes = 0;
  line->recv_window_bytes = recv_window_bytes == 0
                                ? line->config.initial_recv_window_bytes
                                : recv_window_bytes;
}

void MultiLineScheduler::on_line_disconnected(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->connected = false;
  line->negotiated = false;
  line->healthy = false;
  line->stale = true;
  line->consecutive_failures += 1;
  clear_inflight(*line);
}

void MultiLineScheduler::on_line_protocol_error(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->connected = false;
  line->negotiated = false;
  line->healthy = false;
  line->stale = true;
  line->consecutive_failures += 1;
  clear_inflight(*line);
}

void MultiLineScheduler::on_line_failure(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->healthy = false;
  line->stale = true;
  line->consecutive_failures += 1;
  clear_inflight(*line);
}

void MultiLineScheduler::on_heartbeat(LineId line_id, const Heartbeat& heartbeat) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  std::uint64_t completed_bytes = 0;
  auto& pending = line->pending;
  const auto chunk_completed = [&](const LineState::PendingSend& send) {
    return std::any_of(heartbeat.received_chunks.begin(),
                       heartbeat.received_chunks.end(),
                       [&](const ReceivedChunk& chunk) {
                         return send.stream_id == heartbeat.stream_id &&
                                send.file_id == chunk.file_id &&
                                send.seq == chunk.seq &&
                                send.chunk_index == chunk.chunk_index;
                       });
  };
  pending.erase(std::remove_if(pending.begin(), pending.end(), [&](const auto& send) {
                  bool completed = false;
                  if (send.kind == SendKind::Chunk) {
                    completed = chunk_completed(send);
                  } else if (send.kind == SendKind::FileBegin ||
                             send.kind == SendKind::FileCommit ||
                             send.kind == SendKind::Create) {
                    completed = send.stream_id == heartbeat.stream_id &&
                                send.file_id == heartbeat.file_id &&
                                send.seq < heartbeat.next_seq;
                  } else if (send.kind == SendKind::Data) {
                    completed = send.stream_id == heartbeat.stream_id &&
                                send.file_id == heartbeat.file_id &&
                                (send.seq < heartbeat.next_seq ||
                                 heartbeat.offset >= send.end_offset ||
                                 heartbeat.durable_offset >= send.end_offset);
                  } else {
                    completed = send.stream_id == heartbeat.stream_id && send.seq < heartbeat.next_seq;
                  }
                  if (completed) {
                    completed_bytes += send.bytes;
                    return true;
                  }
                  return false;
                }),
                pending.end());
  line->inflight_bytes = completed_bytes >= line->inflight_bytes ? 0 : line->inflight_bytes - completed_bytes;
  line->recv_window_bytes = heartbeat.recv_window_bytes;
  line->missed_heartbeat_ticks = 0;
  line->consecutive_failures = 0;
  line->last_completed_bytes = completed_bytes;
  line->connected = true;
  line->negotiated = true;
  line->healthy = true;
  line->stale = false;
}

void MultiLineScheduler::on_nack(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->healthy = false;
  line->stale = true;
  line->consecutive_failures += 1;
  clear_inflight(*line);
}

std::optional<LineId> MultiLineScheduler::choose_control_line() const {
  const LineState* best = nullptr;
  std::uint64_t best_score = 0;
  for (const auto& line : lines_) {
    if (!line.connected || !line.negotiated || !line.healthy || line.stale) {
      continue;
    }
    const auto score = line.recv_window_bytes > line.inflight_bytes
                           ? line.recv_window_bytes - line.inflight_bytes
                           : 1;
    if (best == nullptr || score > best_score) {
      best = &line;
      best_score = score;
    }
  }
  if (best == nullptr) {
    return std::nullopt;
  }
  return best->config.id;
}

std::vector<LineSnapshot> MultiLineScheduler::snapshots() const {
  std::vector<LineSnapshot> result;
  result.reserve(lines_.size());
  for (const auto& line : lines_) {
    result.push_back(LineSnapshot{
        .id = line.config.id,
        .name = line.config.name,
        .tokens = line.bucket.available(),
        .inflight_bytes = line.inflight_bytes,
        .recv_window_bytes = line.recv_window_bytes,
        .connected = line.connected,
        .negotiated = line.negotiated,
        .healthy = line.healthy,
        .stale = line.stale,
        .missed_heartbeat_ticks = line.missed_heartbeat_ticks,
        .consecutive_failures = line.consecutive_failures,
        .pending_sends = static_cast<std::uint64_t>(line.pending.size()),
        .last_completed_bytes = line.last_completed_bytes,
    });
  }
  return result;
}

std::vector<LostSend> MultiLineScheduler::take_lost_sends() {
  auto result = std::move(lost_sends_);
  lost_sends_.clear();
  return result;
}

MultiLineScheduler::LineState* MultiLineScheduler::find_line(LineId line_id) {
  auto it = std::find_if(lines_.begin(), lines_.end(), [line_id](const auto& line) {
    return line.config.id == line_id;
  });
  return it == lines_.end() ? nullptr : &*it;
}

const MultiLineScheduler::LineState* MultiLineScheduler::find_line(LineId line_id) const {
  auto it = std::find_if(lines_.begin(), lines_.end(), [line_id](const auto& line) {
    return line.config.id == line_id;
  });
  return it == lines_.end() ? nullptr : &*it;
}

std::uint64_t MultiLineScheduler::score_line(const LineState& line, const SendRequest& request) {
  const auto window_headroom = line.recv_window_bytes - line.inflight_bytes;
  const auto token_headroom = line.bucket.available() - request.bytes;
  const auto failure_penalty = std::min<std::uint64_t>(line.consecutive_failures, 16) * 1024 * 1024;
  const auto inflight_penalty = line.inflight_bytes / 2;
  const auto stale_penalty = line.missed_heartbeat_ticks * 1024;
  const auto completed_bonus = std::min<std::uint64_t>(line.last_completed_bytes, 1024 * 1024);
  const auto raw_score = window_headroom + token_headroom + completed_bonus;
  const auto penalty = failure_penalty + inflight_penalty + stale_penalty;
  return raw_score > penalty ? raw_score - penalty : 1;
}

void MultiLineScheduler::clear_inflight(LineState& line) {
  for (const auto& send : line.pending) {
    lost_sends_.push_back(LostSend{
        .line_id = line.config.id,
        .kind = send.kind,
        .stream_id = send.stream_id,
        .file_id = send.file_id,
        .seq = send.seq,
        .offset = send.offset,
        .end_offset = send.end_offset,
        .chunk_index = send.chunk_index,
        .bytes = send.bytes,
    });
  }
  line.inflight_bytes = 0;
  line.last_completed_bytes = 0;
  line.pending.clear();
}

}  // namespace yisync
