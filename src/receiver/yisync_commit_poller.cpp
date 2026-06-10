#include "receiver/yisync_commit_poller.hpp"

#include <utility>

namespace yisync {

CommitCompletionPoller::CommitCompletionPoller(EventLoop& loop,
                                               std::chrono::milliseconds interval)
    : loop_(loop),
      interval_(interval) {}

void CommitCompletionPoller::schedule(std::function<void()> callback) {
  if (scheduled_) {
    return;
  }
  scheduled_ = true;
  loop_.call_later(interval_, [this, callback = std::move(callback)] {
    scheduled_ = false;
    callback();
  });
}

void CommitCompletionPoller::mark_idle() noexcept {
  scheduled_ = false;
}

}  // namespace yisync
