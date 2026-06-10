#pragma once

#include "network/yisync_async.hpp"

#include <chrono>
#include <functional>

namespace yisync {

class CommitCompletionPoller {
 public:
  CommitCompletionPoller(EventLoop& loop, std::chrono::milliseconds interval);

  void schedule(std::function<void()> callback);
  void mark_idle() noexcept;

 private:
  EventLoop& loop_;
  std::chrono::milliseconds interval_;
  bool scheduled_ = false;
};

}  // namespace yisync
