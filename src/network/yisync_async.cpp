#include "network/yisync_async.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace yisync {
namespace {

constexpr std::size_t kFrameHeaderLen = T_MessageHeader::kHeaderLen;
constexpr std::size_t kHeaderLenOffset = 6;
constexpr std::size_t kBodyLenOffset = 8;

std::uint16_t read_le_u16(std::span<const std::byte> bytes, std::size_t offset) {
  return static_cast<std::uint16_t>(static_cast<std::uint8_t>(bytes[offset])) |
         (static_cast<std::uint16_t>(static_cast<std::uint8_t>(bytes[offset + 1])) << 8);
}

std::uint32_t read_le_u32(std::span<const std::byte> bytes, std::size_t offset) {
  return static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset])) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[offset + 3])) << 24);
}

std::string errno_message(std::string_view action) {
  return std::string(action) + ": " + std::strerror(errno);
}

void close_fd(int& fd) {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

void set_socket_options(int fd) {
  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}

void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    throw std::runtime_error(errno_message("fcntl(F_GETFL)"));
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    throw std::runtime_error(errno_message("fcntl(F_SETFL)"));
  }
}

Endpoint endpoint_from_sockaddr(const sockaddr_storage& storage) {
  char host[INET6_ADDRSTRLEN] = {};
  std::uint16_t port = 0;

  if (storage.ss_family == AF_INET) {
    const auto* addr = reinterpret_cast<const sockaddr_in*>(&storage);
    ::inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host));
    port = ntohs(addr->sin_port);
  } else if (storage.ss_family == AF_INET6) {
    const auto* addr = reinterpret_cast<const sockaddr_in6*>(&storage);
    ::inet_ntop(AF_INET6, &addr->sin6_addr, host, sizeof(host));
    port = ntohs(addr->sin6_port);
  } else {
    throw std::runtime_error("unsupported socket address family");
  }

  return Endpoint{host, port};
}

Endpoint local_endpoint_for_fd(int fd) {
  sockaddr_storage storage{};
  socklen_t len = sizeof(storage);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&storage), &len) != 0) {
    throw std::runtime_error(errno_message("getsockname"));
  }
  return endpoint_from_sockaddr(storage);
}

struct AddrInfoDeleter {
  void operator()(addrinfo* info) const {
    if (info != nullptr) {
      ::freeaddrinfo(info);
    }
  }
};

using AddrInfoPtr = std::unique_ptr<addrinfo, AddrInfoDeleter>;

AddrInfoPtr resolve_endpoint(const Endpoint& endpoint, bool passive) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  if (passive) {
    hints.ai_flags = AI_PASSIVE;
  }

  addrinfo* raw = nullptr;
  const auto service = std::to_string(endpoint.port);
  const char* host = endpoint.host.empty() ? nullptr : endpoint.host.c_str();
  const int rc = ::getaddrinfo(host, service.c_str(), &hints, &raw);
  if (rc != 0) {
    throw std::runtime_error(std::string("getaddrinfo: ") + ::gai_strerror(rc));
  }
  return AddrInfoPtr(raw);
}

}  // namespace

void EventLoop::watch_fd(int fd, short events, IoCallback callback) {
  if (fd < 0) {
    throw std::invalid_argument("invalid fd");
  }
  unwatch_fd(fd);
  watchers_.push_back(Watcher{fd, events, std::move(callback)});
}

void EventLoop::update_fd(int fd, short events) {
  auto it = std::find_if(watchers_.begin(), watchers_.end(), [fd](const auto& watcher) {
    return watcher.fd == fd;
  });
  if (it == watchers_.end()) {
    return;
  }
  it->events = events;
}

void EventLoop::unwatch_fd(int fd) {
  watchers_.erase(std::remove_if(watchers_.begin(), watchers_.end(), [fd](const auto& watcher) {
                    return watcher.fd == fd;
                  }),
                  watchers_.end());
}

void EventLoop::call_later(std::chrono::milliseconds delay, TimerCallback callback) {
  timers_.push_back(Timer{
      .id = next_timer_id_++,
      .deadline = std::chrono::steady_clock::now() + delay,
      .callback = std::move(callback),
  });
}

void EventLoop::run() {
  stopped_ = false;
  while (!stopped_) {
    if (watchers_.empty() && timers_.empty()) {
      return;
    }

    std::vector<pollfd> fds;
    fds.reserve(watchers_.size());
    for (const auto& watcher : watchers_) {
      fds.push_back(pollfd{watcher.fd, watcher.events, 0});
    }

    int timeout_ms = -1;
    if (!timers_.empty()) {
      const auto now = std::chrono::steady_clock::now();
      const auto nearest = std::min_element(timers_.begin(), timers_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.deadline < rhs.deadline;
      });
      timeout_ms = static_cast<int>(
          std::max<std::int64_t>(0, std::chrono::duration_cast<std::chrono::milliseconds>(nearest->deadline - now).count()));
    }

    const int rc = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeout_ms);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(errno_message("poll"));
    }

    if (rc > 0) {
      for (const auto& fd : fds) {
        if (fd.revents == 0 || stopped_) {
          continue;
        }
        auto it = std::find_if(watchers_.begin(), watchers_.end(), [&](const auto& watcher) {
          return watcher.fd == fd.fd;
        });
        if (it != watchers_.end()) {
          auto callback = it->callback;
          callback(fd.revents);
        }
      }
    }

    const auto now = std::chrono::steady_clock::now();
    std::vector<Timer> due;
    timers_.erase(std::remove_if(timers_.begin(),
                                 timers_.end(),
                                 [&](Timer& timer) {
                                   if (timer.deadline <= now) {
                                     due.push_back(std::move(timer));
                                     return true;
                                   }
                                   return false;
                                 }),
                  timers_.end());
    for (auto& timer : due) {
      if (stopped_) {
        break;
      }
      timer.callback();
    }
  }
}

void EventLoop::stop() noexcept {
  stopped_ = true;
}

AsyncFrameConnection::AsyncFrameConnection(int fd, std::uint64_t max_frame_bytes)
    : fd_(fd), max_frame_bytes_(max_frame_bytes) {
  if (fd_ < 0) {
    throw std::invalid_argument("invalid async TCP fd");
  }
  set_socket_options(fd_);
  set_nonblocking(fd_);
}

AsyncFrameConnection::~AsyncFrameConnection() {
  close();
}

int AsyncFrameConnection::fd() const noexcept {
  return fd_;
}

bool AsyncFrameConnection::closed() const noexcept {
  return fd_ < 0;
}

void AsyncFrameConnection::start(EventLoop& loop) {
  if (fd_ < 0) {
    throw std::runtime_error("connection is closed");
  }
  loop_ = &loop;
  const auto weak = weak_from_this();
  loop_->watch_fd(fd_, POLLIN, [weak](short revents) {
    if (auto self = weak.lock()) {
      self->handle_io(revents);
    }
  });
  update_watch();
}

void AsyncFrameConnection::send(const T_Message& message) {
  if (fd_ < 0) {
    throw std::runtime_error("connection is closed");
  }
  write_queue_.push_back(PendingWrite{encode_frame(message), 0});
  update_watch();
}

void AsyncFrameConnection::close() {
  if (fd_ < 0) {
    return;
  }
  const int old_fd = fd_;
  if (loop_ != nullptr) {
    loop_->unwatch_fd(old_fd);
  }
  close_fd(fd_);
  if (close_callback_) {
    close_callback_();
  }
}

void AsyncFrameConnection::on_message(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void AsyncFrameConnection::on_close(CloseCallback callback) {
  close_callback_ = std::move(callback);
}

void AsyncFrameConnection::on_error(ErrorCallback callback) {
  error_callback_ = std::move(callback);
}

void AsyncFrameConnection::handle_io(short revents) {
  if (fd_ < 0) {
    return;
  }
  if ((revents & POLLIN) != 0) {
    read_available();
  }
  if (fd_ >= 0 && (revents & POLLOUT) != 0) {
    flush_writes();
  }
  if (fd_ >= 0 && (revents & (POLLERR | POLLNVAL)) != 0) {
    fail("TCP connection error");
  }
  if (fd_ >= 0 && (revents & POLLHUP) != 0) {
    close();
  }
}

void AsyncFrameConnection::read_available() {
  std::array<std::byte, 64 * 1024> buffer{};
  while (fd_ >= 0) {
    const auto n = ::recv(fd_, reinterpret_cast<char*>(buffer.data()), buffer.size(), 0);
    if (n > 0) {
      read_buffer_.insert(read_buffer_.end(), buffer.begin(), buffer.begin() + n);
      continue;
    }
    if (n == 0) {
      close();
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    fail(errno_message("recv"));
    return;
  }
  parse_frames();
}

void AsyncFrameConnection::flush_writes() {
  while (fd_ >= 0 && !write_queue_.empty()) {
    auto& front = write_queue_.front();
    const auto remaining = front.bytes.size() - front.offset;
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    const auto n = ::send(fd_,
                          reinterpret_cast<const char*>(front.bytes.data() + static_cast<std::ptrdiff_t>(front.offset)),
                          remaining,
                          flags);
    if (n > 0) {
      front.offset += static_cast<std::size_t>(n);
      if (front.offset == front.bytes.size()) {
        write_queue_.pop_front();
      }
      continue;
    }
    if (n == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    fail(errno_message("send"));
    return;
  }
  update_watch();
}

void AsyncFrameConnection::parse_frames() {
  while (fd_ >= 0 && read_buffer_.size() >= kFrameHeaderLen) {
    const auto header = std::span<const std::byte>(read_buffer_.data(), kFrameHeaderLen);
    const auto header_len = read_le_u16(header, kHeaderLenOffset);
    const auto body_len = read_le_u32(header, kBodyLenOffset);
    if (header_len != kFrameHeaderLen) {
      fail("unsupported frame header length");
      return;
    }
    if (header_len + body_len > max_frame_bytes_) {
      fail("frame exceeds max_frame_bytes");
      return;
    }
    const auto frame_len = static_cast<std::size_t>(header_len + body_len);
    if (read_buffer_.size() < frame_len) {
      return;
    }

    Bytes frame_bytes(read_buffer_.begin(), read_buffer_.begin() + static_cast<std::ptrdiff_t>(frame_len));
    read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + static_cast<std::ptrdiff_t>(frame_len));

    try {
      auto message = decode_message(decode_frame(std::span<const std::byte>(frame_bytes.data(), frame_bytes.size())));
      if (message_callback_) {
        message_callback_(std::move(message));
      }
    } catch (const std::exception& ex) {
      fail(ex.what());
      return;
    }
  }
}

void AsyncFrameConnection::update_watch() {
  if (fd_ < 0 || loop_ == nullptr) {
    return;
  }
  short events = POLLIN;
  if (!write_queue_.empty()) {
    events |= POLLOUT;
  }
  loop_->update_fd(fd_, events);
}

void AsyncFrameConnection::fail(std::string message) {
  if (error_callback_) {
    error_callback_(message);
  }
  close();
}

AsyncTcpListener::AsyncTcpListener(int fd, Endpoint local)
    : fd_(fd), local_(std::move(local)) {
  if (fd_ < 0) {
    throw std::invalid_argument("invalid async listener fd");
  }
  set_socket_options(fd_);
  set_nonblocking(fd_);
}

AsyncTcpListener::~AsyncTcpListener() {
  close();
}

Endpoint AsyncTcpListener::local_endpoint() const {
  return local_;
}

void AsyncTcpListener::start(EventLoop& loop, AcceptCallback callback) {
  loop_ = &loop;
  accept_callback_ = std::move(callback);
  loop_->watch_fd(fd_, POLLIN, [this](short revents) {
    handle_io(revents);
  });
}

void AsyncTcpListener::close() {
  if (fd_ < 0) {
    return;
  }
  const int old_fd = fd_;
  if (loop_ != nullptr) {
    loop_->unwatch_fd(old_fd);
  }
  close_fd(fd_);
}

bool AsyncTcpListener::closed() const noexcept {
  return fd_ < 0;
}

void AsyncTcpListener::handle_io(short revents) {
  if (fd_ < 0) {
    return;
  }
  if ((revents & (POLLERR | POLLNVAL)) != 0) {
    close();
    return;
  }
  if ((revents & POLLIN) == 0) {
    return;
  }

  while (fd_ >= 0) {
    sockaddr_storage peer{};
    socklen_t peer_len = sizeof(peer);
    const int client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (client_fd >= 0) {
      set_socket_options(client_fd);
      set_nonblocking(client_fd);
      if (accept_callback_) {
        accept_callback_(client_fd, endpoint_from_sockaddr(peer));
      } else {
        ::close(client_fd);
      }
      continue;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    close();
    return;
  }
}

std::unique_ptr<AsyncTcpListener> listen_async_tcp(const Endpoint& bind) {
  const auto addresses = resolve_endpoint(bind, true);
  int listen_fd = -1;

  for (auto* address = addresses.get(); address != nullptr; address = address->ai_next) {
    listen_fd = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (listen_fd < 0) {
      continue;
    }
    set_socket_options(listen_fd);
    set_nonblocking(listen_fd);
    if (::bind(listen_fd, address->ai_addr, address->ai_addrlen) == 0 &&
        ::listen(listen_fd, 16) == 0) {
      const auto local = local_endpoint_for_fd(listen_fd);
      return std::make_unique<AsyncTcpListener>(listen_fd, local);
    }
    close_fd(listen_fd);
  }

  throw std::runtime_error(errno_message("listen_async_tcp"));
}

class PendingTcpConnect : public std::enable_shared_from_this<PendingTcpConnect> {
 public:
  PendingTcpConnect(EventLoop& loop,
                    Endpoint peer,
                    std::uint64_t max_frame_bytes,
                    AsyncConnectedCallback connected,
                    AsyncConnectErrorCallback error)
      : loop_(loop),
        peer_(std::move(peer)),
        max_frame_bytes_(max_frame_bytes),
        connected_(std::move(connected)),
        error_(std::move(error)) {}

  ~PendingTcpConnect() { close_fd(fd_); }

  void start() {
    const auto addresses = resolve_endpoint(peer_, false);
    for (auto* address = addresses.get(); address != nullptr; address = address->ai_next) {
      fd_ = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
      if (fd_ < 0) {
        continue;
      }
      set_socket_options(fd_);
      set_nonblocking(fd_);
      const int rc = ::connect(fd_, address->ai_addr, address->ai_addrlen);
      if (rc == 0) {
        finish_success();
        return;
      }
      if (errno == EINPROGRESS) {
        const auto weak = weak_from_this();
        loop_.watch_fd(fd_, POLLOUT, [weak](short revents) {
          if (auto self = weak.lock()) {
            self->handle_io(revents);
          }
        });
        return;
      }
      close_fd(fd_);
    }
    finish_error(errno_message("async_connect_tcp"));
  }

 private:
  void handle_io(short revents) {
    if (fd_ < 0) {
      return;
    }
    if ((revents & (POLLERR | POLLNVAL | POLLHUP | POLLOUT)) == 0) {
      return;
    }
    int error = 0;
    socklen_t len = sizeof(error);
    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) != 0) {
      finish_error(errno_message("getsockopt(SO_ERROR)"));
      return;
    }
    if (error != 0) {
      finish_error(std::string("connect: ") + std::strerror(error));
      return;
    }
    finish_success();
  }

  void finish_success() {
    const int connected_fd = fd_;
    fd_ = -1;
    loop_.unwatch_fd(connected_fd);
    auto connection = std::make_shared<AsyncFrameConnection>(connected_fd, max_frame_bytes_);
    if (connected_) {
      connected_(std::move(connection));
    }
  }

  void finish_error(std::string message) {
    if (fd_ >= 0) {
      loop_.unwatch_fd(fd_);
      close_fd(fd_);
    }
    if (error_) {
      error_(std::move(message));
    }
  }

  EventLoop& loop_;
  Endpoint peer_;
  std::uint64_t max_frame_bytes_ = 1024 * 1024;
  AsyncConnectedCallback connected_;
  AsyncConnectErrorCallback error_;
  int fd_ = -1;
};

std::shared_ptr<PendingTcpConnect> async_connect_tcp(EventLoop& loop,
                                                     const Endpoint& peer,
                                                     std::uint64_t max_frame_bytes,
                                                     AsyncConnectedCallback connected,
                                                     AsyncConnectErrorCallback error) {
  auto pending = std::make_shared<PendingTcpConnect>(
      loop, peer, max_frame_bytes, std::move(connected), std::move(error));
  pending->start();
  return pending;
}

}  // namespace yisync
