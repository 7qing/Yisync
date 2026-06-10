#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <thread>

namespace yisync {

inline constexpr std::size_t kDefaultDiskWriterQueueCapacity = 128;

class SpscDiskWriter {
 public:
  SpscDiskWriter();
  ~SpscDiskWriter();

  SpscDiskWriter(const SpscDiskWriter&) = delete;
  SpscDiskWriter& operator=(const SpscDiskWriter&) = delete;

  bool enqueue(std::function<void()> task);
  void drain();
  bool failed() const noexcept;
  const char* error() const noexcept;
  void stop();

 private:
  static constexpr std::size_t kCapacity = kDefaultDiskWriterQueueCapacity + 1;

  static std::size_t increment(std::size_t value) noexcept;
  void run();

  std::array<std::function<void()>, kCapacity> queue_;
  std::atomic<std::size_t> head_{0};
  std::atomic<std::size_t> tail_{0};
  std::atomic<bool> busy_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> failed_{false};
  std::array<char, 160> error_{};
  std::thread worker_;
};

}  // namespace yisync
