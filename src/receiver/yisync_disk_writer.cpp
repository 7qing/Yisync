#include "receiver/yisync_disk_writer.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <string_view>
#include <utility>

namespace yisync {

SpscDiskWriter::SpscDiskWriter()
    : worker_([this] { run(); }) {}

SpscDiskWriter::~SpscDiskWriter() {
  stop();
}

bool SpscDiskWriter::enqueue(std::function<void()> task) {
  if (stopping_.load(std::memory_order_acquire)) {
    return false;
  }
  const auto tail = tail_.load(std::memory_order_relaxed);
  const auto next_tail = increment(tail);
  if (next_tail == head_.load(std::memory_order_acquire)) {
    return false;
  }
  queue_[tail] = std::move(task);
  tail_.store(next_tail, std::memory_order_release);
  return true;
}

void SpscDiskWriter::drain() {
  while (head_.load(std::memory_order_acquire) != tail_.load(std::memory_order_acquire) ||
         busy_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

bool SpscDiskWriter::failed() const noexcept {
  return failed_.load(std::memory_order_acquire);
}

const char* SpscDiskWriter::error() const noexcept {
  return error_.data();
}

void SpscDiskWriter::stop() {
  const bool was_stopping = stopping_.exchange(true, std::memory_order_acq_rel);
  if (was_stopping) {
    return;
  }
  if (worker_.joinable()) {
    worker_.join();
  }
}

std::size_t SpscDiskWriter::increment(std::size_t value) noexcept {
  return (value + 1) % kCapacity;
}

void SpscDiskWriter::run() {
  while (true) {
    const auto head = head_.load(std::memory_order_relaxed);
    if (head == tail_.load(std::memory_order_acquire)) {
      if (stopping_.load(std::memory_order_acquire)) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    busy_.store(true, std::memory_order_release);
    auto task = std::move(queue_[head]);
    queue_[head] = nullptr;
    head_.store(increment(head), std::memory_order_release);
    try {
      task();
    } catch (const std::exception& ex) {
      const auto message = std::string_view(ex.what());
      const auto count = std::min<std::size_t>(message.size(), error_.size() - 1);
      std::copy_n(message.begin(), count, error_.begin());
      error_[count] = '\0';
      failed_.store(true, std::memory_order_release);
    }
    busy_.store(false, std::memory_order_release);
  }
}

}  // namespace yisync
