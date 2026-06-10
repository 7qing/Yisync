#pragma once

#include "network/yisync_async.hpp"
#include "network/yisync_scheduler.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yisync::network {

enum class Protocol : std::uint8_t {
  Tcp = 1,
  Udp = 2,
  Quic = 3,
  Areon = 4,
};

std::string_view protocol_name(Protocol protocol) noexcept;

struct LineEndpoint {
  LineId id = 0;
  Protocol protocol = Protocol::Tcp;
  Endpoint endpoint;
  std::string name;
};

struct ReconnectPolicy {
  std::chrono::milliseconds base_delay{100};
  std::chrono::milliseconds max_delay{2000};
  std::uint64_t max_shift = 5;
};

std::chrono::milliseconds reconnect_delay(const ReconnectPolicy& policy,
                                          std::uint64_t attempts) noexcept;

struct LineLogState {
  std::string owner;
  LineId id = 0;
  Protocol protocol = Protocol::Tcp;
  Endpoint endpoint;
  std::string event;
  std::string detail;
  std::uint64_t attempt = 0;
  std::chrono::milliseconds delay{0};
};

void log_line_state(std::ostream& out, const LineLogState& state);

struct NegotiationResult {
  bool ok = false;
  std::uint16_t negotiated_version = 0;
  std::uint64_t negotiated_features = 0;
  std::uint64_t recv_window_bytes = 0;
  std::string error;
};

Hello make_hello(Role role,
                 std::string node_id,
                 std::uint32_t chunk_size,
                 std::uint64_t max_inflight_bytes);
NegotiationResult negotiate_hello(const Hello& local,
                                  const Hello& peer,
                                  Role expected_peer_role);

class SenderLineSet {
 public:
  using MessageCallback = std::function<void(LineId, Message)>;
  using ConnectedCallback = std::function<void(LineId)>;
  using UnavailableCallback = std::function<void(LineId, bool had_connection)>;

  SenderLineSet(EventLoop& loop,
                std::vector<LineEndpoint> lines,
                std::uint64_t max_frame_bytes,
                ReconnectPolicy reconnect_policy = {});

  void start_all();
  void stop_reconnects();
  void connect(LineId id);
  void schedule_reconnect(LineId id);
  void mark_unavailable(LineId id);
  void close(LineId id);
  void send(LineId id, const Message& message);

  bool can_send(LineId id) const;
  Endpoint endpoint(LineId id) const;

  void on_message(MessageCallback callback);
  void on_connected(ConnectedCallback callback);
  void on_unavailable(UnavailableCallback callback);

 private:
  struct Line {
    LineEndpoint endpoint;
    std::shared_ptr<AsyncFrameConnection> connection;
    std::shared_ptr<PendingTcpConnect> connector;
    bool connecting = false;
    bool reconnect_scheduled = false;
    std::uint64_t reconnect_attempts = 0;
  };

  Line& find(LineId id);
  const Line& find(LineId id) const;

  EventLoop& loop_;
  std::uint64_t max_frame_bytes_ = 0;
  ReconnectPolicy reconnect_policy_;
  std::vector<Line> lines_;
  MessageCallback message_callback_;
  ConnectedCallback connected_callback_;
  UnavailableCallback unavailable_callback_;
  bool reconnects_enabled_ = true;
};

class ReceiverLineSet {
 public:
  using MessageCallback = std::function<void(LineId, Message)>;
  using AcceptedCallback = std::function<void(LineId, Endpoint)>;
  using ErrorCallback = std::function<void(LineId, std::string)>;
  using CloseCallback = std::function<void(LineId)>;

  ReceiverLineSet(EventLoop& loop,
                  std::vector<LineEndpoint> lines,
                  std::uint64_t max_frame_bytes);

  void listen_all();
  void send(LineId id, const Message& message);
  void close(LineId id);
  bool can_send(LineId id) const;
  Endpoint endpoint(LineId id) const;

  void on_message(MessageCallback callback);
  void on_accepted(AcceptedCallback callback);
  void on_error(ErrorCallback callback);
  void on_close(CloseCallback callback);

 private:
  struct Line {
    LineEndpoint endpoint;
    std::unique_ptr<AsyncTcpListener> listener;
    std::shared_ptr<AsyncFrameConnection> connection;
  };

  Line& find(LineId id);
  const Line& find(LineId id) const;
  void on_accept(LineId id, int fd, Endpoint peer);

  EventLoop& loop_;
  std::uint64_t max_frame_bytes_ = 0;
  std::vector<Line> lines_;
  MessageCallback message_callback_;
  AcceptedCallback accepted_callback_;
  ErrorCallback error_callback_;
  CloseCallback close_callback_;
};

class ReceiverNetwork {
 public:
  using MessageCallback = ReceiverLineSet::MessageCallback;
  using AcceptedCallback = ReceiverLineSet::AcceptedCallback;
  using ErrorCallback = ReceiverLineSet::ErrorCallback;
  using CloseCallback = ReceiverLineSet::CloseCallback;

  ReceiverNetwork(EventLoop& loop,
                  std::vector<LineEndpoint> endpoints,
                  std::uint64_t max_frame_bytes,
                  Hello local_hello = make_hello(Role::Receiver, "receiver", kDefaultChunkSizeBytes, 0));

  void listen();
  void send(LineId line_id, const Message& message);
  void queue_heartbeat(LineId line_id, Heartbeat heartbeat);
  void flush_heartbeats(LineId line_id);
  void flush_all_heartbeats();
  void set_heartbeat_ack_batch_size(std::uint64_t ack_batch_size) noexcept;
  bool can_send(LineId line_id) const;
  Endpoint endpoint(LineId line_id) const;

  void on_message(MessageCallback callback);
  void on_accepted(AcceptedCallback callback);
  void on_error(ErrorCallback callback);
  void on_close(CloseCallback callback);

 private:
  struct HeartbeatKey {
    LineId line_id = 0;
    std::uint64_t stream_id = 0;

    bool operator==(const HeartbeatKey& other) const noexcept {
      return line_id == other.line_id && stream_id == other.stream_id;
    }
  };

  struct HeartbeatKeyHash {
    std::size_t operator()(const HeartbeatKey& key) const noexcept;
  };

  struct PendingHeartbeat {
    Heartbeat heartbeat;
    bool dirty = false;
  };

  void maybe_flush_heartbeat_batch(LineId line_id, const HeartbeatKey& key);
  void handle_message(LineId line_id, Message message);
  void handle_hello(LineId line_id, const Hello& hello);
  void fail_protocol(LineId line_id, std::string error);

  ReceiverLineSet lines_;
  Hello local_hello_;
  std::unordered_map<LineId, NegotiationResult> negotiated_lines_;
  std::unordered_map<HeartbeatKey, PendingHeartbeat, HeartbeatKeyHash> pending_heartbeats_;
  MessageCallback message_callback_;
  AcceptedCallback accepted_callback_;
  ErrorCallback error_callback_;
  CloseCallback close_callback_;
  std::uint64_t heartbeat_ack_batch_size_ = 20;
};

struct NetworkSendResult {
  bool sent = false;
  LineId line_id = 0;
  std::uint64_t bytes = 0;
};

struct NetworkControlResult {
  bool queued = false;
  bool sent = false;
  LineId line_id = 0;
};

struct ControlMessage {
  Message message;
  std::string log_label;
};

class SenderNetwork {
 public:
  using MessageCallback = std::function<void(LineId, Message)>;
  using ConnectedCallback = std::function<void(LineId)>;
  using LostSendCallback = std::function<void(const std::vector<LostSend>&)>;

  SenderNetwork(EventLoop& loop,
                std::vector<LineEndpoint> endpoints,
                std::vector<LineConfig> line_configs,
                std::uint64_t max_frame_bytes,
                ReconnectPolicy reconnect_policy = {},
                Hello local_hello = make_hello(Role::Sender, "sender", kDefaultChunkSizeBytes, 0));

  void start();
  void stop();
  NetworkSendResult send(const Message& message, const SendRequest& request);
  NetworkControlResult send_control(Message message, std::string log_label = {});
  void mark_unavailable(LineId line_id);
  void on_heartbeat(LineId line_id, const Heartbeat& heartbeat);
  void refill_ticks(std::uint64_t ticks);
  std::vector<LineSnapshot> snapshots() const;

  void on_message(MessageCallback callback);
  void on_connected(ConnectedCallback callback);
  void on_lost_sends(LostSendCallback callback);

 private:
  void handle_connected(LineId line_id);
  void handle_unavailable(LineId line_id, bool had_connection);
  void handle_message(LineId line_id, Message message);
  void handle_hello(LineId line_id, const Hello& hello);
  void emit_lost_sends();
  std::optional<LineId> flush_one_control();
  void flush_controls();

  SenderLineSet lines_;
  MultiLineScheduler scheduler_;
  Hello local_hello_;
  std::unordered_map<LineId, NegotiationResult> negotiated_lines_;
  std::deque<ControlMessage> control_queue_;
  MessageCallback message_callback_;
  ConnectedCallback connected_callback_;
  LostSendCallback lost_send_callback_;
};

}  // namespace yisync::network
