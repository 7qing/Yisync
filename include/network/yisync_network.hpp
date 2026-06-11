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

enum class EM_Protocol : std::uint8_t {
  TCP = 1,
  UDP = 2,
  QUIC = 3,
  AREON = 4,
};

std::string_view protocol_name(EM_Protocol protocol) noexcept;

struct T_LineEndpoint {
  LineId id = 0;
  EM_Protocol protocol = EM_Protocol::TCP;
  Endpoint endpoint;
  std::string name;
};

struct T_ReconnectPolicy {
  std::chrono::milliseconds base_delay{100};
  std::chrono::milliseconds max_delay{2000};
  std::uint64_t max_shift = 5;
};

std::chrono::milliseconds reconnect_delay(const T_ReconnectPolicy& policy,
                                          std::uint64_t attempts) noexcept;

struct T_LineLogState {
  std::string owner;
  LineId id = 0;
  EM_Protocol protocol = EM_Protocol::TCP;
  Endpoint endpoint;
  std::string event;
  std::string detail;
  std::uint64_t attempt = 0;
  std::chrono::milliseconds delay{0};
};

void log_line_state(std::ostream& out, const T_LineLogState& state);

struct T_NegotiationResult {
  bool ok = false;
  std::uint16_t negotiated_version = 0;
  std::uint64_t negotiated_features = 0;
  std::uint64_t recv_window_bytes = 0;
  std::string error;
};

T_Hello make_hello(EM_Role role,
                 std::string node_id,
                 std::uint32_t chunk_size,
                 std::uint64_t max_inflight_bytes);
T_NegotiationResult negotiate_hello(const T_Hello& local,
                                  const T_Hello& peer,
                                  EM_Role expected_peer_role);

class T_SenderLineSet {
 public:
  using MessageCallback = std::function<void(LineId, T_Message)>;
  using ConnectedCallback = std::function<void(LineId)>;
  using UnavailableCallback = std::function<void(LineId, bool had_connection)>;

  T_SenderLineSet(EventLoop& loop,
                std::vector<T_LineEndpoint> lines,
                std::uint64_t max_frame_bytes,
                T_ReconnectPolicy reconnect_policy = {});

  void start_all();
  void stop_reconnects();
  void connect(LineId id);
  void schedule_reconnect(LineId id);
  void mark_unavailable(LineId id);
  void close(LineId id);
  void send(LineId id, const T_Message& message);

  bool can_send(LineId id) const;
  Endpoint endpoint(LineId id) const;

  void on_message(MessageCallback callback);
  void on_connected(ConnectedCallback callback);
  void on_unavailable(UnavailableCallback callback);

 private:
  struct T_Line {
    T_LineEndpoint endpoint;
    std::shared_ptr<AsyncFrameConnection> connection;
    std::shared_ptr<PendingTcpConnect> connector;
    bool connecting = false;
    bool reconnect_scheduled = false;
    std::uint64_t reconnect_attempts = 0;
  };

  T_Line& find(LineId id);
  const T_Line& find(LineId id) const;

  EventLoop& loop_;
  std::uint64_t max_frame_bytes_ = 0;
  T_ReconnectPolicy reconnect_policy_;
  std::vector<T_Line> lines_;
  MessageCallback message_callback_;
  ConnectedCallback connected_callback_;
  UnavailableCallback unavailable_callback_;
  bool reconnects_enabled_ = true;
};

class T_ReceiverLineSet {
 public:
  using MessageCallback = std::function<void(LineId, T_Message)>;
  using AcceptedCallback = std::function<void(LineId, Endpoint)>;
  using ErrorCallback = std::function<void(LineId, std::string)>;
  using CloseCallback = std::function<void(LineId)>;

  T_ReceiverLineSet(EventLoop& loop,
                  std::vector<T_LineEndpoint> lines,
                  std::uint64_t max_frame_bytes);

  void listen_all();
  void send(LineId id, const T_Message& message);
  void close(LineId id);
  bool can_send(LineId id) const;
  Endpoint endpoint(LineId id) const;

  void on_message(MessageCallback callback);
  void on_accepted(AcceptedCallback callback);
  void on_error(ErrorCallback callback);
  void on_close(CloseCallback callback);

 private:
  struct T_Line {
    T_LineEndpoint endpoint;
    std::unique_ptr<AsyncTcpListener> listener;
    std::shared_ptr<AsyncFrameConnection> connection;
  };

  T_Line& find(LineId id);
  const T_Line& find(LineId id) const;
  void on_accept(LineId id, int fd, Endpoint peer);

  EventLoop& loop_;
  std::uint64_t max_frame_bytes_ = 0;
  std::vector<T_Line> lines_;
  MessageCallback message_callback_;
  AcceptedCallback accepted_callback_;
  ErrorCallback error_callback_;
  CloseCallback close_callback_;
};

class T_ReceiverNetwork {
 public:
  using MessageCallback = T_ReceiverLineSet::MessageCallback;
  using AcceptedCallback = T_ReceiverLineSet::AcceptedCallback;
  using ErrorCallback = T_ReceiverLineSet::ErrorCallback;
  using CloseCallback = T_ReceiverLineSet::CloseCallback;

  T_ReceiverNetwork(EventLoop& loop,
                  std::vector<T_LineEndpoint> endpoints,
                  std::uint64_t max_frame_bytes,
                  T_Hello local_hello = make_hello(EM_Role::RECEIVER, "receiver", kDefaultChunkSizeBytes, 0));

  void listen();
  void send(LineId line_id, const T_Message& message);
  void queue_heartbeat(LineId line_id, T_Heartbeat heartbeat);
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
  struct T_HeartbeatKey {
    LineId line_id = 0;
    std::uint64_t stream_id = 0;

    bool operator==(const T_HeartbeatKey& other) const noexcept {
      return line_id == other.line_id && stream_id == other.stream_id;
    }
  };

  struct T_HeartbeatKeyHash {
    std::size_t operator()(const T_HeartbeatKey& key) const noexcept;
  };

  struct T_PendingHeartbeat {
    T_Heartbeat heartbeat;
    bool dirty = false;
  };

  void maybe_flush_heartbeat_batch(LineId line_id, const T_HeartbeatKey& key);
  void handle_message(LineId line_id, T_Message message);
  void handle_hello(LineId line_id, const T_Hello& hello);
  void fail_protocol(LineId line_id, std::string error);

  T_ReceiverLineSet lines_;
  T_Hello local_hello_;
  std::unordered_map<LineId, T_NegotiationResult> negotiated_lines_;
  std::unordered_map<T_HeartbeatKey, T_PendingHeartbeat, T_HeartbeatKeyHash> pending_heartbeats_;
  MessageCallback message_callback_;
  AcceptedCallback accepted_callback_;
  ErrorCallback error_callback_;
  CloseCallback close_callback_;
  std::uint64_t heartbeat_ack_batch_size_ = 20;
};

struct T_NetworkSendResult {
  bool sent = false;
  LineId line_id = 0;
  std::uint64_t bytes = 0;
};

struct T_NetworkControlResult {
  bool queued = false;
  bool sent = false;
  LineId line_id = 0;
};

struct T_ControlMessage {
  T_Message message;
  std::string log_label;
};

class T_SenderNetwork {
 public:
  using MessageCallback = std::function<void(LineId, T_Message)>;
  using ConnectedCallback = std::function<void(LineId)>;
  using LostSendCallback = std::function<void(const std::vector<T_LostSend>&)>;

  T_SenderNetwork(EventLoop& loop,
                std::vector<T_LineEndpoint> endpoints,
                std::vector<T_LineConfig> line_configs,
                std::uint64_t max_frame_bytes,
                T_ReconnectPolicy reconnect_policy = {},
                T_Hello local_hello = make_hello(EM_Role::SENDER, "sender", kDefaultChunkSizeBytes, 0));

  void start();
  void stop();
  T_NetworkSendResult send(const T_Message& message, const T_SendRequest& request);
  T_NetworkControlResult send_control(T_Message message, std::string log_label = {});
  void mark_unavailable(LineId line_id);
  void on_heartbeat(LineId line_id, const T_Heartbeat& heartbeat);
  void refill_ticks(std::uint64_t ticks);
  std::vector<T_LineSnapshot> snapshots() const;

  void on_message(MessageCallback callback);
  void on_connected(ConnectedCallback callback);
  void on_lost_sends(LostSendCallback callback);

 private:
  void handle_connected(LineId line_id);
  void handle_unavailable(LineId line_id, bool had_connection);
  void handle_message(LineId line_id, T_Message message);
  void handle_hello(LineId line_id, const T_Hello& hello);
  void emit_lost_sends();
  std::optional<LineId> flush_one_control();
  void flush_controls();

  T_SenderLineSet lines_;
  T_MultiLineScheduler scheduler_;
  T_Hello local_hello_;
  std::unordered_map<LineId, T_NegotiationResult> negotiated_lines_;
  std::deque<T_ControlMessage> control_queue_;
  MessageCallback message_callback_;
  ConnectedCallback connected_callback_;
  LostSendCallback lost_send_callback_;
};

}  // namespace yisync::network
