#include "yisync_scheduler.hpp"

#include <algorithm>
#include <vector>
#include <stdexcept>

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
    state.bucket = TokenBucket(config.limiter);
    state.config = std::move(config);
    lines_.push_back(std::move(state));
  }
}

void MultiLineScheduler::refill_ticks(std::uint64_t ticks) {
  for (auto& line : lines_) {
    line.bucket.refill_ticks(ticks);
  }
}

std::optional<SendGrant> MultiLineScheduler::try_acquire(const SendRequest& request) {
  if (request.bytes == 0) {
    return std::nullopt;
  }

  LineState* best = nullptr;
  std::uint64_t best_headroom = 0;

  for (auto& line : lines_) {
    if (!line.healthy) {
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

    if (best == nullptr || window_headroom > best_headroom) {
      best = &line;
      best_headroom = window_headroom;
    }
  }

  if (best == nullptr) {
    return std::nullopt;
  }

  if (!best->bucket.try_consume(request.bytes)) {
    return std::nullopt;
  }
  best->inflight_bytes += request.bytes;
  best->pending.push_back(LineState::PendingSend{
      .kind = request.kind,
      .stream_id = request.stream_id,
      .file_id = request.file_id,
      .seq = request.seq,
      .order_seq = request.order_seq,
      .chunk_index = request.chunk_index,
      .bytes = request.bytes,
  });
  return SendGrant{best->config.id, request.bytes};
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
                                send.order_seq == chunk.order_seq &&
                                send.chunk_index == chunk.chunk_index;
                       });
  };
  pending.erase(std::remove_if(pending.begin(), pending.end(), [&](const auto& send) {
                  const bool completed =
                      send.kind == SendKind::Chunk
                          ? chunk_completed(send)
                          : (send.stream_id == heartbeat.stream_id && send.seq < heartbeat.next_seq);
                  if (completed) {
                    completed_bytes += send.bytes;
                    return true;
                  }
                  return false;
                }),
                pending.end());
  line->inflight_bytes = completed_bytes >= line->inflight_bytes ? 0 : line->inflight_bytes - completed_bytes;
  line->recv_window_bytes = heartbeat.recv_window_bytes;
  line->healthy = true;
}

void MultiLineScheduler::on_nack(LineId line_id) {
  auto* line = find_line(line_id);
  if (line == nullptr) {
    throw std::invalid_argument("unknown line id");
  }
  line->healthy = false;
  line->inflight_bytes = 0;
  line->pending.clear();
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
    });
  }
  return result;
}

MultiLineScheduler::LineState* MultiLineScheduler::find_line(LineId line_id) {
  auto it = std::find_if(lines_.begin(), lines_.end(), [line_id](const auto& line) {
    return line.config.id == line_id;
  });
  return it == lines_.end() ? nullptr : &*it;
}

}  // namespace yisync
