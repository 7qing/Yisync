#pragma once

#include "core/yisync_protocol.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace yisync {

struct Endpoint {
  std::string host;
  std::uint16_t port = 0;
};

class EventLoop {
 public:
  using IoCallback = std::function<void(short)>;
  using TimerCallback = std::function<void()>;

  void watch_fd(int fd, short events, IoCallback callback);
  void update_fd(int fd, short events);
  void unwatch_fd(int fd);
  void call_later(std::chrono::milliseconds delay, TimerCallback callback);
  void run();
  void stop() noexcept;

 private:
  struct Watcher {
    int fd = -1;
    short events = 0;
    IoCallback callback;
  };

  struct Timer {
    std::uint64_t id = 0;
    std::chrono::steady_clock::time_point deadline;
    TimerCallback callback;
  };

  bool stopped_ = false;
  std::uint64_t next_timer_id_ = 1;
  std::vector<Watcher> watchers_;
  std::vector<Timer> timers_;
};

class AsyncFrameConnection : public std::enable_shared_from_this<AsyncFrameConnection> {
 public:
  using MessageCallback = std::function<void(Message)>;
  using CloseCallback = std::function<void()>;
  using ErrorCallback = std::function<void(std::string)>;

  AsyncFrameConnection(int fd, std::uint64_t max_frame_bytes = 1024 * 1024);
  ~AsyncFrameConnection();

  AsyncFrameConnection(const AsyncFrameConnection&) = delete;
  AsyncFrameConnection& operator=(const AsyncFrameConnection&) = delete;

  int fd() const noexcept;
  bool closed() const noexcept;
  void start(EventLoop& loop);
  void send(const Message& message);
  void close();

  void on_message(MessageCallback callback);
  void on_close(CloseCallback callback);
  void on_error(ErrorCallback callback);

 private:
  struct PendingWrite {
    Bytes bytes;
    std::size_t offset = 0;
  };

  void handle_io(short revents);
  void read_available();
  void flush_writes();
  void parse_frames();
  void update_watch();
  void fail(std::string message);

  int fd_ = -1;
  std::uint64_t max_frame_bytes_ = 1024 * 1024;
  EventLoop* loop_ = nullptr;
  Bytes read_buffer_;
  std::deque<PendingWrite> write_queue_;
  MessageCallback message_callback_;
  CloseCallback close_callback_;
  ErrorCallback error_callback_;
};

class AsyncTcpListener {
 public:
  using AcceptCallback = std::function<void(int fd, Endpoint peer)>;

  AsyncTcpListener(int fd, Endpoint local);
  ~AsyncTcpListener();

  AsyncTcpListener(const AsyncTcpListener&) = delete;
  AsyncTcpListener& operator=(const AsyncTcpListener&) = delete;

  Endpoint local_endpoint() const;
  void start(EventLoop& loop, AcceptCallback callback);
  void close();
  bool closed() const noexcept;

 private:
  void handle_io(short revents);

  int fd_ = -1;
  Endpoint local_;
  EventLoop* loop_ = nullptr;
  AcceptCallback accept_callback_;
};

std::unique_ptr<AsyncTcpListener> listen_async_tcp(const Endpoint& bind);

class PendingTcpConnect;

using AsyncConnectedCallback =
    std::function<void(std::shared_ptr<AsyncFrameConnection>)>;
using AsyncConnectErrorCallback = std::function<void(std::string)>;

std::shared_ptr<PendingTcpConnect> async_connect_tcp(
    EventLoop& loop,
    const Endpoint& peer,
    std::uint64_t max_frame_bytes,
    AsyncConnectedCallback connected,
    AsyncConnectErrorCallback error);

}  // namespace yisync
