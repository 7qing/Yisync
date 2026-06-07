#include "yisync_transport.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>

namespace yisync {
namespace {

constexpr std::size_t kFrameHeaderLen = 20;
constexpr std::size_t kHeaderLenOffset = 8;
constexpr std::size_t kBodyLenOffset = 12;

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

void send_all(int fd, std::span<const std::byte> bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    const auto* data = reinterpret_cast<const char*>(bytes.data() + static_cast<std::ptrdiff_t>(sent));
    const auto remaining = bytes.size() - sent;
    const auto n = ::send(fd, data, remaining, flags);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(errno_message("send"));
    }
    if (n == 0) {
      throw std::runtime_error("send returned 0");
    }
    sent += static_cast<std::size_t>(n);
  }
}

bool recv_exact(int fd, std::span<std::byte> bytes) {
  std::size_t received = 0;
  while (received < bytes.size()) {
    auto* data = reinterpret_cast<char*>(bytes.data() + static_cast<std::ptrdiff_t>(received));
    const auto remaining = bytes.size() - received;
    const auto n = ::recv(fd, data, remaining, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(errno_message("recv"));
    }
    if (n == 0) {
      return false;
    }
    received += static_cast<std::size_t>(n);
  }
  return true;
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

struct MemoryPipeState {
  std::deque<Bytes> a_to_b;
  std::deque<Bytes> b_to_a;
  bool a_closed = false;
  bool b_closed = false;
  std::uint64_t max_frame_bytes = 1024 * 1024;
  mutable std::mutex mutex;
};

class MemoryFrameTransport final : public IFrameTransport {
 public:
  MemoryFrameTransport(std::shared_ptr<MemoryPipeState> state, bool endpoint_a)
      : state_(std::move(state)), endpoint_a_(endpoint_a) {}

  TransportProperties properties() const override {
    return TransportProperties{
        .kind = TransportKind::Memory,
        .reliability = TransportReliability::Reliable,
        .ordering = TransportOrdering::Ordered,
        .preserves_message_boundaries = true,
        .max_frame_bytes = state_->max_frame_bytes,
    };
  }

  void send_frame(Bytes frame) override {
    std::lock_guard lock(state_->mutex);
    if (closed_locked()) {
      throw std::runtime_error("transport is closed");
    }
    if (frame.size() > state_->max_frame_bytes) {
      throw std::runtime_error("frame exceeds transport max_frame_bytes");
    }

    auto& outbound = endpoint_a_ ? state_->a_to_b : state_->b_to_a;
    outbound.push_back(std::move(frame));
  }

  std::optional<Bytes> receive_frame() override {
    std::lock_guard lock(state_->mutex);
    auto& inbound = endpoint_a_ ? state_->b_to_a : state_->a_to_b;
    if (inbound.empty()) {
      return std::nullopt;
    }

    Bytes frame = std::move(inbound.front());
    inbound.pop_front();
    return frame;
  }

  void close() override {
    std::lock_guard lock(state_->mutex);
    if (endpoint_a_) {
      state_->a_closed = true;
    } else {
      state_->b_closed = true;
    }
  }

  bool closed() const override {
    std::lock_guard lock(state_->mutex);
    return closed_locked();
  }

 private:
  bool closed_locked() const {
    return endpoint_a_ ? state_->a_closed : state_->b_closed;
  }

  std::shared_ptr<MemoryPipeState> state_;
  bool endpoint_a_ = true;
};

class TcpFrameTransport final : public IFrameTransport {
 public:
  TcpFrameTransport(int fd, std::uint64_t max_frame_bytes)
      : fd_(fd), max_frame_bytes_(max_frame_bytes) {
    if (fd_ < 0) {
      throw std::invalid_argument("invalid TCP socket fd");
    }
    set_socket_options(fd_);
  }

  ~TcpFrameTransport() override { close(); }

  TcpFrameTransport(const TcpFrameTransport&) = delete;
  TcpFrameTransport& operator=(const TcpFrameTransport&) = delete;

  TransportProperties properties() const override {
    return TransportProperties{
        .kind = TransportKind::Tcp,
        .reliability = TransportReliability::Reliable,
        .ordering = TransportOrdering::Ordered,
        .preserves_message_boundaries = false,
        .max_frame_bytes = max_frame_bytes_,
    };
  }

  void send_frame(Bytes frame) override {
    if (closed()) {
      throw std::runtime_error("TCP transport is closed");
    }
    if (frame.size() > max_frame_bytes_) {
      throw std::runtime_error("frame exceeds TCP transport max_frame_bytes");
    }
    send_all(fd_, frame);
  }

  std::optional<Bytes> receive_frame() override {
    if (closed()) {
      return std::nullopt;
    }

    Bytes header(kFrameHeaderLen);
    if (!recv_exact(fd_, header)) {
      close();
      return std::nullopt;
    }

    const auto header_len = read_le_u32(header, kHeaderLenOffset);
    const auto body_len = read_le_u32(header, kBodyLenOffset);
    if (header_len != kFrameHeaderLen) {
      throw std::runtime_error("unsupported TCP frame header length");
    }
    if (header_len + body_len > max_frame_bytes_) {
      throw std::runtime_error("TCP frame exceeds max_frame_bytes");
    }

    Bytes frame;
    frame.reserve(header_len + body_len);
    frame.insert(frame.end(), header.begin(), header.end());
    if (body_len > 0) {
      Bytes body(body_len);
      if (!recv_exact(fd_, body)) {
        close();
        return std::nullopt;
      }
      frame.insert(frame.end(), body.begin(), body.end());
    }
    return frame;
  }

  void close() override { close_fd(fd_); }

  bool closed() const override { return fd_ < 0; }

 private:
  int fd_ = -1;
  std::uint64_t max_frame_bytes_ = 1024 * 1024;
};

class TcpFrameListener final : public IFrameListener {
 public:
  TcpFrameListener(int fd, Endpoint local, std::uint64_t max_frame_bytes)
      : fd_(fd), local_(std::move(local)), max_frame_bytes_(max_frame_bytes) {
    if (fd_ < 0) {
      throw std::invalid_argument("invalid TCP listener fd");
    }
  }

  ~TcpFrameListener() override { close(); }

  TcpFrameListener(const TcpFrameListener&) = delete;
  TcpFrameListener& operator=(const TcpFrameListener&) = delete;

  Endpoint local_endpoint() const override { return local_; }

  std::unique_ptr<IFrameTransport> accept() override {
    while (true) {
      sockaddr_storage peer{};
      socklen_t peer_len = sizeof(peer);
      const int client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
      if (client_fd < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error(errno_message("accept"));
      }
      return std::make_unique<TcpFrameTransport>(client_fd, max_frame_bytes_);
    }
  }

  void close() override { close_fd(fd_); }

  bool closed() const override { return fd_ < 0; }

 private:
  int fd_ = -1;
  Endpoint local_;
  std::uint64_t max_frame_bytes_ = 1024 * 1024;
};

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

[[noreturn]] void throw_unimplemented_transport(TransportKind kind) {
  switch (kind) {
    case TransportKind::Tcp:
      throw std::runtime_error("TCP transport adapter is handled by connect_tcp_transport");
    case TransportKind::Udp:
      throw std::runtime_error("UDP transport adapter is not implemented yet");
    case TransportKind::QuicStream:
      throw std::runtime_error("QUIC stream transport adapter is not implemented yet");
    case TransportKind::QuicDatagram:
      throw std::runtime_error("QUIC datagram transport adapter is not implemented yet");
    case TransportKind::Memory:
      throw std::runtime_error("memory transport must be created with make_memory_transport_pair");
  }
  throw std::runtime_error("unknown transport adapter");
}

}  // namespace

MessageChannel::MessageChannel(std::unique_ptr<IFrameTransport> transport)
    : transport_(std::move(transport)) {
  if (!transport_) {
    throw std::invalid_argument("transport is null");
  }
}

const IFrameTransport& MessageChannel::transport() const noexcept {
  return *transport_;
}

void MessageChannel::send(const Message& message) {
  transport_->send_frame(encode_frame(message));
}

std::optional<Message> MessageChannel::receive() {
  auto frame_bytes = transport_->receive_frame();
  if (!frame_bytes.has_value()) {
    return std::nullopt;
  }
  return decode_message(decode_frame(*frame_bytes));
}

void MessageChannel::close() {
  transport_->close();
}

bool MessageChannel::closed() const {
  return transport_->closed();
}

std::pair<std::unique_ptr<IFrameTransport>, std::unique_ptr<IFrameTransport>>
make_memory_transport_pair(std::uint64_t max_frame_bytes) {
  auto state = std::make_shared<MemoryPipeState>();
  state->max_frame_bytes = max_frame_bytes;

  return {
      std::make_unique<MemoryFrameTransport>(state, true),
      std::make_unique<MemoryFrameTransport>(state, false),
  };
}

std::unique_ptr<IFrameListener> listen_tcp_transport(const Endpoint& bind,
                                                     std::uint64_t max_frame_bytes) {
  const auto addresses = resolve_endpoint(bind, true);
  int listen_fd = -1;

  for (auto* address = addresses.get(); address != nullptr; address = address->ai_next) {
    listen_fd = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (listen_fd < 0) {
      continue;
    }
    set_socket_options(listen_fd);
    if (::bind(listen_fd, address->ai_addr, address->ai_addrlen) == 0 &&
        ::listen(listen_fd, 16) == 0) {
      const auto local = local_endpoint_for_fd(listen_fd);
      return std::make_unique<TcpFrameListener>(listen_fd, local, max_frame_bytes);
    }
    close_fd(listen_fd);
  }

  throw std::runtime_error(errno_message("listen_tcp_transport"));
}

std::unique_ptr<IFrameTransport> connect_tcp_transport(const Endpoint& peer,
                                                       std::uint64_t max_frame_bytes) {
  const auto addresses = resolve_endpoint(peer, false);
  int fd = -1;

  for (auto* address = addresses.get(); address != nullptr; address = address->ai_next) {
    fd = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (fd < 0) {
      continue;
    }
    set_socket_options(fd);
    if (::connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
      return std::make_unique<TcpFrameTransport>(fd, max_frame_bytes);
    }
    close_fd(fd);
  }

  throw std::runtime_error(errno_message("connect_tcp_transport"));
}

std::unique_ptr<IFrameTransport> make_transport(const TransportConfig& config) {
  if (config.kind == TransportKind::Memory) {
    throw std::runtime_error("memory transport needs a pair; use make_memory_transport_pair");
  }
  if (config.kind == TransportKind::Tcp) {
    return connect_tcp_transport(config.peer, config.max_frame_bytes);
  }
  throw_unimplemented_transport(config.kind);
}

}  // namespace yisync
