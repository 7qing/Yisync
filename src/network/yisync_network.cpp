#include "network/yisync_network.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <utility>
#include <unistd.h>

namespace yisync::network {
namespace {

std::uint64_t clamp_shift(std::uint64_t value, std::uint64_t max_value) noexcept {
  return std::min(value, max_value);
}

bool contains_compression(const std::vector<Compression>& values, Compression target) {
  return std::find(values.begin(), values.end(), target) != values.end();
}

bool contains_checksum(const std::vector<ChecksumAlgo>& values, ChecksumAlgo target) {
  return std::find(values.begin(), values.end(), target) != values.end();
}

std::string hex_u64(std::uint64_t value) {
  std::ostringstream out;
  out << "0x" << std::hex << value;
  return out.str();
}

}  // namespace

std::string_view protocol_name(Protocol protocol) noexcept {
  switch (protocol) {
    case Protocol::Tcp:
      return "tcp";
    case Protocol::Udp:
      return "udp";
    case Protocol::Quic:
      return "quic";
    case Protocol::Areon:
      return "areon";
  }
  return "unknown";
}

std::chrono::milliseconds reconnect_delay(const ReconnectPolicy& policy,
                                          std::uint64_t attempts) noexcept {
  const auto shift = clamp_shift(attempts, policy.max_shift);
  const auto scaled = policy.base_delay.count() << shift;
  return std::chrono::milliseconds(std::min<std::int64_t>(policy.max_delay.count(), scaled));
}

void log_line_state(std::ostream& out, const LineLogState& state) {
  out << state.owner
      << " line " << state.event
      << " id=" << state.id
      << " protocol=" << protocol_name(state.protocol)
      << " endpoint=" << state.endpoint.host << ":" << state.endpoint.port;
  if (state.attempt != 0) {
    out << " attempt=" << state.attempt;
  }
  if (state.delay.count() != 0) {
    out << " delay_ms=" << state.delay.count();
  }
  if (!state.detail.empty()) {
    out << " " << state.detail;
  }
  out << "\n";
}

Hello make_hello(Role role,
                 std::string node_id,
                 std::uint32_t chunk_size,
                 std::uint64_t max_inflight_bytes) {
  return Hello{
      .node_id = std::move(node_id),
      .role = role,
      .min_version = kProtocolVersion,
      .max_version = kProtocolVersion,
      .feature_flags = kSupportedFeatureFlags,
      .required_feature_flags = kRequiredFeatureFlags,
      .chunk_size = chunk_size,
      .max_inflight_bytes = max_inflight_bytes,
      .supported_compression = {Compression::None},
      .supported_checksum = {ChecksumAlgo::Crc32c},
  };
}

NegotiationResult negotiate_hello(const Hello& local,
                                  const Hello& peer,
                                  Role expected_peer_role) {
  if (peer.role != expected_peer_role) {
    return NegotiationResult{
        .ok = false,
        .error = "unexpected peer role",
    };
  }
  const auto min_version = std::max(local.min_version, peer.min_version);
  const auto max_version = std::min(local.max_version, peer.max_version);
  if (min_version > max_version || max_version < kProtocolVersion) {
    return NegotiationResult{
        .ok = false,
        .error = "no compatible protocol version",
    };
  }
  const auto common_features = local.feature_flags & peer.feature_flags;
  if ((local.required_feature_flags & common_features) != local.required_feature_flags) {
    return NegotiationResult{
        .ok = false,
        .error = "peer missing local required features required=" +
                 hex_u64(local.required_feature_flags) +
                 " common=" + hex_u64(common_features),
    };
  }
  if ((peer.required_feature_flags & common_features) != peer.required_feature_flags) {
    return NegotiationResult{
        .ok = false,
        .error = "local missing peer required features required=" +
                 hex_u64(peer.required_feature_flags) +
                 " common=" + hex_u64(common_features),
    };
  }
  if (!contains_compression(local.supported_compression, Compression::None) ||
      !contains_compression(peer.supported_compression, Compression::None)) {
    return NegotiationResult{
        .ok = false,
        .error = "Compression::None is required",
    };
  }
  if (!contains_checksum(local.supported_checksum, ChecksumAlgo::Crc32c) ||
      !contains_checksum(peer.supported_checksum, ChecksumAlgo::Crc32c)) {
    return NegotiationResult{
        .ok = false,
        .error = "ChecksumAlgo::Crc32c is required",
    };
  }
  if (local.chunk_size != 0 && peer.chunk_size != 0 && local.chunk_size != peer.chunk_size) {
    return NegotiationResult{
        .ok = false,
        .error = "chunk_size mismatch local=" + std::to_string(local.chunk_size) +
                 " peer=" + std::to_string(peer.chunk_size),
    };
  }
  return NegotiationResult{
      .ok = true,
      .negotiated_version = max_version,
      .negotiated_features = common_features,
      .recv_window_bytes = peer.max_inflight_bytes,
  };
}

SenderLineSet::SenderLineSet(EventLoop& loop,
                             std::vector<LineEndpoint> lines,
                             std::uint64_t max_frame_bytes,
                             ReconnectPolicy reconnect_policy)
    : loop_(loop),
      max_frame_bytes_(max_frame_bytes),
      reconnect_policy_(reconnect_policy) {
  lines_.reserve(lines.size());
  for (auto& endpoint : lines) {
    lines_.push_back(Line{.endpoint = std::move(endpoint)});
  }
}

void SenderLineSet::start_all() {
  for (const auto& line : lines_) {
    connect(line.endpoint.id);
  }
}

void SenderLineSet::stop_reconnects() {
  reconnects_enabled_ = false;
  for (auto& line : lines_) {
    line.reconnect_scheduled = false;
  }
}

void SenderLineSet::connect(LineId id) {
  auto& line = find(id);
  if (!reconnects_enabled_ || can_send(id) || line.connecting) {
    return;
  }
  if (line.endpoint.protocol != Protocol::Tcp) {
    throw std::runtime_error("only TCP sender lines are implemented");
  }
  line.reconnect_scheduled = false;
  line.connecting = true;
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "SENDER",
                     .id = id,
                     .protocol = line.endpoint.protocol,
                     .endpoint = line.endpoint.endpoint,
                     .event = "connecting",
                     .attempt = line.reconnect_attempts + 1,
                 });
  line.connector = async_connect_tcp(
      loop_,
      line.endpoint.endpoint,
      max_frame_bytes_,
      [this, id](std::shared_ptr<AsyncFrameConnection> connection) {
        auto& line = find(id);
        line.connector.reset();
        line.connecting = false;
        line.reconnect_scheduled = false;
        line.reconnect_attempts = 0;
        line.connection = std::move(connection);
        line.connection->on_message([this, id](Message message) {
          if (message_callback_) {
            message_callback_(id, std::move(message));
          }
        });
        line.connection->on_error([this, id](std::string error) {
          if (!reconnects_enabled_) {
            return;
          }
          log_line_state(std::cerr,
                         LineLogState{
                             .owner = "SENDER",
                             .id = id,
                             .protocol = find(id).endpoint.protocol,
                             .endpoint = find(id).endpoint.endpoint,
                             .event = "error",
                             .detail = "error=" + error,
                         });
          mark_unavailable(id);
        });
        line.connection->on_close([this, id] {
          if (!reconnects_enabled_) {
            return;
          }
          log_line_state(std::cerr,
                         LineLogState{
                             .owner = "SENDER",
                             .id = id,
                             .protocol = find(id).endpoint.protocol,
                             .endpoint = find(id).endpoint.endpoint,
                             .event = "closed",
                         });
          mark_unavailable(id);
        });
        line.connection->start(loop_);
        log_line_state(std::cout,
                       LineLogState{
                           .owner = "SENDER",
                           .id = id,
                           .protocol = line.endpoint.protocol,
                           .endpoint = line.endpoint.endpoint,
                           .event = "connected",
                       });
        if (connected_callback_) {
          connected_callback_(id);
        }
      },
      [this, id](std::string error) {
        auto& line = find(id);
        line.connector.reset();
        line.connecting = false;
        log_line_state(std::cerr,
                       LineLogState{
                           .owner = "SENDER",
                           .id = id,
                           .protocol = line.endpoint.protocol,
                           .endpoint = line.endpoint.endpoint,
                           .event = "connect_failed",
                           .detail = "error=" + error,
                       });
        if (unavailable_callback_) {
          unavailable_callback_(id, false);
        }
        schedule_reconnect(id);
      });
}

void SenderLineSet::schedule_reconnect(LineId id) {
  auto& line = find(id);
  if (!reconnects_enabled_ || can_send(id) || line.connecting || line.reconnect_scheduled) {
    return;
  }
  const auto delay = reconnect_delay(reconnect_policy_, line.reconnect_attempts);
  line.reconnect_attempts += 1;
  line.reconnect_scheduled = true;
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "SENDER",
                     .id = id,
                     .protocol = line.endpoint.protocol,
                     .endpoint = line.endpoint.endpoint,
                     .event = "reconnect_scheduled",
                     .attempt = line.reconnect_attempts,
                     .delay = delay,
                 });
  loop_.call_later(delay, [this, id] {
    auto& line = find(id);
    line.reconnect_scheduled = false;
    if (reconnects_enabled_ && !can_send(id) && !line.connecting) {
      connect(id);
    }
  });
}

void SenderLineSet::mark_unavailable(LineId id) {
  auto& line = find(id);
  const auto had_connection = static_cast<bool>(line.connection);
  if (!can_send(id) && !line.connecting && line.reconnect_scheduled) {
    return;
  }
  line.connecting = false;
  line.connector.reset();
  auto connection = std::move(line.connection);
  if (unavailable_callback_) {
    unavailable_callback_(id, had_connection);
  }
  if (reconnects_enabled_) {
    schedule_reconnect(id);
  }
  if (connection && !connection->closed()) {
    connection->close();
  }
}

void SenderLineSet::close(LineId id) {
  auto& line = find(id);
  line.connecting = false;
  line.connector.reset();
  if (line.connection && !line.connection->closed()) {
    line.connection->close();
  }
  line.connection.reset();
}

void SenderLineSet::send(LineId id, const Message& message) {
  auto& line = find(id);
  if (!line.connection || line.connection->closed()) {
    throw std::runtime_error("sender line is not connected");
  }
  line.connection->send(message);
}

bool SenderLineSet::can_send(LineId id) const {
  const auto& line = find(id);
  return line.connection && !line.connection->closed();
}

Endpoint SenderLineSet::endpoint(LineId id) const {
  return find(id).endpoint.endpoint;
}

void SenderLineSet::on_message(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void SenderLineSet::on_connected(ConnectedCallback callback) {
  connected_callback_ = std::move(callback);
}

void SenderLineSet::on_unavailable(UnavailableCallback callback) {
  unavailable_callback_ = std::move(callback);
}

SenderLineSet::Line& SenderLineSet::find(LineId id) {
  auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
    return line.endpoint.id == id;
  });
  if (it == lines_.end()) {
    throw std::runtime_error("unknown sender network line");
  }
  return *it;
}

const SenderLineSet::Line& SenderLineSet::find(LineId id) const {
  auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
    return line.endpoint.id == id;
  });
  if (it == lines_.end()) {
    throw std::runtime_error("unknown sender network line");
  }
  return *it;
}

ReceiverLineSet::ReceiverLineSet(EventLoop& loop,
                                 std::vector<LineEndpoint> lines,
                                 std::uint64_t max_frame_bytes)
    : loop_(loop),
      max_frame_bytes_(max_frame_bytes) {
  lines_.reserve(lines.size());
  for (auto& endpoint : lines) {
    lines_.push_back(Line{.endpoint = std::move(endpoint)});
  }
}

void ReceiverLineSet::listen_all() {
  for (auto& line : lines_) {
    if (line.endpoint.protocol != Protocol::Tcp) {
      throw std::runtime_error("only TCP receiver lines are implemented");
    }
    line.listener = listen_async_tcp(line.endpoint.endpoint);
    line.endpoint.endpoint = line.listener->local_endpoint();
    log_line_state(std::cout,
                   LineLogState{
                       .owner = "RECEIVER",
                       .id = line.endpoint.id,
                       .protocol = line.endpoint.protocol,
                       .endpoint = line.endpoint.endpoint,
                       .event = "listening",
                   });
    line.listener->start(loop_, [this, id = line.endpoint.id](int fd, Endpoint peer) {
      on_accept(id, fd, std::move(peer));
    });
  }
}

void ReceiverLineSet::send(LineId id, const Message& message) {
  auto& line = find(id);
  if (!line.connection || line.connection->closed()) {
    throw std::runtime_error("receiver line has no active connection");
  }
  line.connection->send(message);
}

void ReceiverLineSet::close(LineId id) {
  auto& line = find(id);
  if (line.connection && !line.connection->closed()) {
    line.connection->close();
  }
  line.connection.reset();
}

bool ReceiverLineSet::can_send(LineId id) const {
  const auto& line = find(id);
  return line.connection && !line.connection->closed();
}

Endpoint ReceiverLineSet::endpoint(LineId id) const {
  return find(id).endpoint.endpoint;
}

void ReceiverLineSet::on_message(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void ReceiverLineSet::on_accepted(AcceptedCallback callback) {
  accepted_callback_ = std::move(callback);
}

void ReceiverLineSet::on_error(ErrorCallback callback) {
  error_callback_ = std::move(callback);
}

void ReceiverLineSet::on_close(CloseCallback callback) {
  close_callback_ = std::move(callback);
}

ReceiverLineSet::Line& ReceiverLineSet::find(LineId id) {
  auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
    return line.endpoint.id == id;
  });
  if (it == lines_.end()) {
    throw std::runtime_error("unknown receiver network line");
  }
  return *it;
}

const ReceiverLineSet::Line& ReceiverLineSet::find(LineId id) const {
  auto it = std::find_if(lines_.begin(), lines_.end(), [id](const auto& line) {
    return line.endpoint.id == id;
  });
  if (it == lines_.end()) {
    throw std::runtime_error("unknown receiver network line");
  }
  return *it;
}

void ReceiverLineSet::on_accept(LineId id, int fd, Endpoint peer) {
  auto& line = find(id);
  if (line.connection && !line.connection->closed()) {
    ::close(fd);
    return;
  }
  auto connection = std::make_shared<AsyncFrameConnection>(fd, max_frame_bytes_);
  connection->on_message([this, id](Message message) {
    if (message_callback_) {
      message_callback_(id, std::move(message));
    }
  });
  connection->on_error([this, id](std::string error) {
    if (error_callback_) {
      error_callback_(id, std::move(error));
    }
  });
  connection->on_close([this, id] {
    if (close_callback_) {
      close_callback_(id);
    }
  });
  connection->start(loop_);
  line.connection = std::move(connection);
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "RECEIVER",
                     .id = id,
                     .protocol = line.endpoint.protocol,
                     .endpoint = peer,
                     .event = "accepted",
                 });
  if (accepted_callback_) {
    accepted_callback_(id, std::move(peer));
  }
}

ReceiverNetwork::ReceiverNetwork(EventLoop& loop,
                                 std::vector<LineEndpoint> endpoints,
                                 std::uint64_t max_frame_bytes,
                                 Hello local_hello)
    : lines_(loop, std::move(endpoints), max_frame_bytes),
      local_hello_(std::move(local_hello)) {
  lines_.on_message([this](LineId line_id, Message message) {
    handle_message(line_id, std::move(message));
  });
  lines_.on_accepted([this](LineId line_id, Endpoint peer) {
    negotiated_lines_.erase(line_id);
    if (accepted_callback_) {
      accepted_callback_(line_id, std::move(peer));
    }
  });
  lines_.on_error([this](LineId line_id, std::string error) {
    negotiated_lines_.erase(line_id);
    if (error_callback_) {
      error_callback_(line_id, std::move(error));
    }
  });
  lines_.on_close([this](LineId line_id) {
    negotiated_lines_.erase(line_id);
    if (close_callback_) {
      close_callback_(line_id);
    }
  });
}

void ReceiverNetwork::listen() {
  lines_.listen_all();
}

void ReceiverNetwork::send(LineId line_id, const Message& message) {
  lines_.send(line_id, message);
}

void ReceiverNetwork::queue_heartbeat(LineId line_id, Heartbeat heartbeat) {
  const HeartbeatKey key{line_id, heartbeat.stream_id};
  auto& pending = pending_heartbeats_[key];
  if (!pending.dirty) {
    pending.heartbeat = std::move(heartbeat);
    pending.dirty = true;
    maybe_flush_heartbeat_batch(line_id, key);
    return;
  }

  auto& target = pending.heartbeat;
  target.stream_id = heartbeat.stream_id;
  target.next_seq = heartbeat.next_seq;
  target.file_id = heartbeat.file_id;
  target.offset = heartbeat.offset;
  target.durable_offset = heartbeat.durable_offset;
  target.recv_window_bytes = heartbeat.recv_window_bytes;
  target.received_chunks.insert(target.received_chunks.end(),
                                heartbeat.received_chunks.begin(),
                                heartbeat.received_chunks.end());
  target.missing_ranges = std::move(heartbeat.missing_ranges);
  maybe_flush_heartbeat_batch(line_id, key);
}

void ReceiverNetwork::flush_heartbeats(LineId line_id) {
  std::vector<HeartbeatKey> keys;
  for (const auto& [key, pending] : pending_heartbeats_) {
    if (pending.dirty && key.line_id == line_id) {
      keys.push_back(key);
    }
  }
  for (const auto& key : keys) {
    auto it = pending_heartbeats_.find(key);
    if (it == pending_heartbeats_.end() || !it->second.dirty) {
      continue;
    }
    auto heartbeat = std::move(it->second.heartbeat);
    it->second = PendingHeartbeat{};
    send(line_id, Message{heartbeat});
  }
}

void ReceiverNetwork::flush_all_heartbeats() {
  std::vector<HeartbeatKey> keys;
  keys.reserve(pending_heartbeats_.size());
  for (const auto& [key, pending] : pending_heartbeats_) {
    if (pending.dirty) {
      keys.push_back(key);
    }
  }
  for (const auto& key : keys) {
    auto it = pending_heartbeats_.find(key);
    if (it == pending_heartbeats_.end() || !it->second.dirty) {
      continue;
    }
    auto heartbeat = std::move(it->second.heartbeat);
    it->second = PendingHeartbeat{};
    send(key.line_id, Message{heartbeat});
  }
}

void ReceiverNetwork::set_heartbeat_ack_batch_size(std::uint64_t ack_batch_size) noexcept {
  heartbeat_ack_batch_size_ = ack_batch_size;
}

bool ReceiverNetwork::can_send(LineId line_id) const {
  return lines_.can_send(line_id);
}

Endpoint ReceiverNetwork::endpoint(LineId line_id) const {
  return lines_.endpoint(line_id);
}

void ReceiverNetwork::on_message(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void ReceiverNetwork::on_accepted(AcceptedCallback callback) {
  accepted_callback_ = std::move(callback);
}

void ReceiverNetwork::on_error(ErrorCallback callback) {
  error_callback_ = std::move(callback);
}

void ReceiverNetwork::on_close(CloseCallback callback) {
  close_callback_ = std::move(callback);
}

std::size_t ReceiverNetwork::HeartbeatKeyHash::operator()(const HeartbeatKey& key) const noexcept {
  const auto lhs = std::hash<LineId>{}(key.line_id);
  const auto rhs = std::hash<std::uint64_t>{}(key.stream_id);
  return lhs ^ (rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6) + (lhs >> 2));
}

void ReceiverNetwork::maybe_flush_heartbeat_batch(LineId line_id, const HeartbeatKey& key) {
  if (heartbeat_ack_batch_size_ == 0) {
    return;
  }
  auto it = pending_heartbeats_.find(key);
  if (it == pending_heartbeats_.end() || !it->second.dirty) {
    return;
  }
  if (it->second.heartbeat.received_chunks.size() < heartbeat_ack_batch_size_) {
    return;
  }
  flush_heartbeats(line_id);
}

void ReceiverNetwork::handle_message(LineId line_id, Message message) {
  if (const auto* hello = std::get_if<Hello>(&message)) {
    handle_hello(line_id, *hello);
    return;
  }
  const auto negotiated = negotiated_lines_.find(line_id);
  if (negotiated == negotiated_lines_.end() || !negotiated->second.ok) {
    fail_protocol(line_id, "business message before Hello negotiation");
    return;
  }
  if (message_callback_) {
    message_callback_(line_id, std::move(message));
  }
}

void ReceiverNetwork::handle_hello(LineId line_id, const Hello& hello) {
  const auto result = negotiate_hello(local_hello_, hello, Role::Sender);
  negotiated_lines_[line_id] = result;
  if (!result.ok) {
    fail_protocol(line_id, "negotiation failed: " + result.error);
    return;
  }
  lines_.send(line_id, Message{local_hello_});
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "RECEIVER",
                     .id = line_id,
                     .protocol = Protocol::Tcp,
                     .endpoint = lines_.endpoint(line_id),
                     .event = "negotiated",
                     .detail = "version=" + std::to_string(result.negotiated_version) +
                               " features=" + hex_u64(result.negotiated_features),
                 });
}

void ReceiverNetwork::fail_protocol(LineId line_id, std::string error) {
  log_line_state(std::cerr,
                 LineLogState{
                     .owner = "RECEIVER",
                     .id = line_id,
                     .protocol = Protocol::Tcp,
                     .endpoint = lines_.endpoint(line_id),
                     .event = "protocol_error",
                     .detail = "error=" + error,
                 });
  negotiated_lines_.erase(line_id);
  lines_.close(line_id);
  if (error_callback_) {
    error_callback_(line_id, std::move(error));
  }
}

SenderNetwork::SenderNetwork(EventLoop& loop,
                             std::vector<LineEndpoint> endpoints,
                             std::vector<LineConfig> line_configs,
                             std::uint64_t max_frame_bytes,
                             ReconnectPolicy reconnect_policy,
                             Hello local_hello)
    : lines_(loop, std::move(endpoints), max_frame_bytes, reconnect_policy),
      scheduler_(std::move(line_configs)),
      local_hello_(std::move(local_hello)) {
  lines_.on_message([this](LineId line_id, Message message) {
    handle_message(line_id, std::move(message));
  });
  lines_.on_connected([this](LineId line_id) {
    handle_connected(line_id);
  });
  lines_.on_unavailable([this](LineId line_id, bool had_connection) {
    handle_unavailable(line_id, had_connection);
  });
}

void SenderNetwork::start() {
  lines_.start_all();
}

void SenderNetwork::stop() {
  lines_.stop_reconnects();
}

NetworkSendResult SenderNetwork::send(const Message& message, const SendRequest& request) {
  flush_controls();
  const auto grant = scheduler_.try_acquire(request);
  if (!grant.has_value()) {
    return {};
  }
  if (!lines_.can_send(grant->line_id)) {
    lines_.mark_unavailable(grant->line_id);
    return {};
  }
  lines_.send(grant->line_id, message);
  return NetworkSendResult{
      .sent = true,
      .line_id = grant->line_id,
      .bytes = grant->bytes,
  };
}

NetworkControlResult SenderNetwork::send_control(Message message, std::string log_label) {
  control_queue_.push_back(ControlMessage{
      .message = std::move(message),
      .log_label = std::move(log_label),
  });
  NetworkControlResult result{
      .queued = true,
      .sent = false,
      .line_id = 0,
  };
  if (auto line_id = flush_one_control()) {
    result.sent = true;
    result.line_id = *line_id;
    return result;
  }
  return result;
}

void SenderNetwork::mark_unavailable(LineId line_id) {
  lines_.mark_unavailable(line_id);
}

void SenderNetwork::on_heartbeat(LineId line_id, const Heartbeat& heartbeat) {
  scheduler_.on_heartbeat(line_id, heartbeat);
}

void SenderNetwork::refill_ticks(std::uint64_t ticks) {
  scheduler_.refill_ticks(ticks);
  for (const auto& snapshot : scheduler_.snapshots()) {
    if (snapshot.connected && snapshot.stale && snapshot.pending_sends > 0) {
      log_line_state(std::cerr,
                     LineLogState{
                         .owner = "SENDER",
                         .id = snapshot.id,
                         .protocol = Protocol::Tcp,
                         .endpoint = lines_.endpoint(snapshot.id),
                         .event = "heartbeat_timeout",
                         .detail = "pending=" + std::to_string(snapshot.pending_sends),
                     });
      lines_.mark_unavailable(snapshot.id);
    }
  }
  flush_controls();
}

std::vector<LineSnapshot> SenderNetwork::snapshots() const {
  return scheduler_.snapshots();
}

void SenderNetwork::on_message(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void SenderNetwork::on_connected(ConnectedCallback callback) {
  connected_callback_ = std::move(callback);
}

void SenderNetwork::on_lost_sends(LostSendCallback callback) {
  lost_send_callback_ = std::move(callback);
}

void SenderNetwork::handle_connected(LineId line_id) {
  scheduler_.on_line_connected(line_id);
  negotiated_lines_.erase(line_id);
  lines_.send(line_id, Message{local_hello_});
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "SENDER",
                     .id = line_id,
                     .protocol = Protocol::Tcp,
                     .endpoint = lines_.endpoint(line_id),
                     .event = "hello_sent",
                     .detail = "version_min=" + std::to_string(local_hello_.min_version) +
                               " version_max=" + std::to_string(local_hello_.max_version) +
                               " features=" + hex_u64(local_hello_.feature_flags),
                 });
}

void SenderNetwork::handle_unavailable(LineId line_id, bool had_connection) {
  scheduler_.on_line_disconnected(line_id);
  negotiated_lines_.erase(line_id);
  if (had_connection) {
    emit_lost_sends();
  }
}

void SenderNetwork::handle_message(LineId line_id, Message message) {
  if (const auto* hello = std::get_if<Hello>(&message)) {
    handle_hello(line_id, *hello);
    return;
  }
  const auto negotiated = negotiated_lines_.find(line_id);
  if (negotiated == negotiated_lines_.end() || !negotiated->second.ok) {
    log_line_state(std::cerr,
                   LineLogState{
                       .owner = "SENDER",
                       .id = line_id,
                       .protocol = Protocol::Tcp,
                       .endpoint = lines_.endpoint(line_id),
                       .event = "protocol_error",
                       .detail = "business message before Hello negotiation",
                   });
    scheduler_.on_line_protocol_error(line_id);
    lines_.mark_unavailable(line_id);
    return;
  }
  if (message_callback_) {
    message_callback_(line_id, std::move(message));
  }
}

void SenderNetwork::handle_hello(LineId line_id, const Hello& hello) {
  const auto result = negotiate_hello(local_hello_, hello, Role::Receiver);
  negotiated_lines_[line_id] = result;
  if (!result.ok) {
    log_line_state(std::cerr,
                   LineLogState{
                       .owner = "SENDER",
                       .id = line_id,
                       .protocol = Protocol::Tcp,
                       .endpoint = lines_.endpoint(line_id),
                       .event = "negotiation_failed",
                       .detail = "error=" + result.error,
                   });
    scheduler_.on_line_protocol_error(line_id);
    lines_.mark_unavailable(line_id);
    return;
  }
  scheduler_.on_line_negotiated(line_id, result.recv_window_bytes);
  log_line_state(std::cout,
                 LineLogState{
                     .owner = "SENDER",
                     .id = line_id,
                     .protocol = Protocol::Tcp,
                     .endpoint = lines_.endpoint(line_id),
                     .event = "negotiated",
                     .detail = "version=" + std::to_string(result.negotiated_version) +
                               " features=" + hex_u64(result.negotiated_features),
                 });
  if (connected_callback_) {
    connected_callback_(line_id);
  }
  flush_controls();
}

void SenderNetwork::emit_lost_sends() {
  auto lost = scheduler_.take_lost_sends();
  if (!lost.empty() && lost_send_callback_) {
    lost_send_callback_(lost);
  }
}

std::optional<LineId> SenderNetwork::flush_one_control() {
  if (control_queue_.empty()) {
    return std::nullopt;
  }
  const auto line_id = scheduler_.choose_control_line();
  if (!line_id.has_value()) {
    return std::nullopt;
  }
  if (!lines_.can_send(*line_id)) {
    lines_.mark_unavailable(*line_id);
    return std::nullopt;
  }
  auto control = std::move(control_queue_.front());
  lines_.send(*line_id, control.message);
  if (!control.log_label.empty()) {
    log_line_state(std::cout,
                   LineLogState{
                       .owner = "SENDER",
                       .id = *line_id,
                       .protocol = Protocol::Tcp,
                       .endpoint = lines_.endpoint(*line_id),
                       .event = "control_sent",
                       .detail = control.log_label,
                   });
  }
  control_queue_.pop_front();
  return line_id;
}

void SenderNetwork::flush_controls() {
  while (flush_one_control().has_value()) {
  }
}

}  // namespace yisync::network
