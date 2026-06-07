#pragma once

#include "yisync_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace yisync {

enum class TransportKind : std::uint8_t {
  Memory = 1,
  Tcp = 2,
  Udp = 3,
  QuicStream = 4,
  QuicDatagram = 5,
};

enum class TransportReliability : std::uint8_t {
  Reliable = 1,
  BestEffort = 2,
};

enum class TransportOrdering : std::uint8_t {
  Ordered = 1,
  Unordered = 2,
};

struct Endpoint {
  std::string host;
  std::uint16_t port = 0;
};

struct TransportProperties {
  TransportKind kind = TransportKind::Memory;
  TransportReliability reliability = TransportReliability::Reliable;
  TransportOrdering ordering = TransportOrdering::Ordered;
  bool preserves_message_boundaries = true;
  std::uint64_t max_frame_bytes = 1024 * 1024;
};

struct TransportConfig {
  TransportKind kind = TransportKind::Memory;
  Endpoint bind;
  Endpoint peer;
  std::uint64_t max_frame_bytes = 1024 * 1024;
};

class IFrameTransport {
 public:
  virtual ~IFrameTransport() = default;

  virtual TransportProperties properties() const = 0;
  virtual void send_frame(Bytes frame) = 0;
  virtual std::optional<Bytes> receive_frame() = 0;
  virtual void close() = 0;
  virtual bool closed() const = 0;
};

class IFrameListener {
 public:
  virtual ~IFrameListener() = default;

  virtual Endpoint local_endpoint() const = 0;
  virtual std::unique_ptr<IFrameTransport> accept() = 0;
  virtual void close() = 0;
  virtual bool closed() const = 0;
};

class MessageChannel {
 public:
  explicit MessageChannel(std::unique_ptr<IFrameTransport> transport);

  const IFrameTransport& transport() const noexcept;
  void send(const Message& message);
  std::optional<Message> receive();
  void close();
  bool closed() const;

 private:
  std::unique_ptr<IFrameTransport> transport_;
};

std::pair<std::unique_ptr<IFrameTransport>, std::unique_ptr<IFrameTransport>>
make_memory_transport_pair(std::uint64_t max_frame_bytes = 1024 * 1024);

std::unique_ptr<IFrameListener> listen_tcp_transport(
    const Endpoint& bind,
    std::uint64_t max_frame_bytes = 1024 * 1024);

std::unique_ptr<IFrameTransport> connect_tcp_transport(
    const Endpoint& peer,
    std::uint64_t max_frame_bytes = 1024 * 1024);

std::unique_ptr<IFrameTransport> make_transport(const TransportConfig& config);

}  // namespace yisync
