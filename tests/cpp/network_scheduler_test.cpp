#include "network/yisync_scheduler.hpp"

#include <chrono>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

yisync::T_LineConfig line_config(yisync::LineId id) {
  return yisync::T_LineConfig{
      .id = id,
      .name = "line-" + std::to_string(id),
      .limiter = yisync::T_TokenBucketConfig{
          .tokens_per_tick = 1024 * 1024,
          .capacity = 1024 * 1024,
          .tick = std::chrono::milliseconds(10),
      },
      .initial_recv_window_bytes = 1024 * 1024,
      .heartbeat_timeout_ticks = 5,
      .initially_connected = false,
  };
}

}  // namespace

TEST(NetworkSchedulerTest, DoesNotUseLineBeforeHelloNegotiation) {
  yisync::T_MultiLineScheduler scheduler({line_config(1)});
  scheduler.on_line_connected(1);

  const auto grant_before = scheduler.try_acquire(yisync::T_SendRequest{
      .stream_id = 9001,
      .file_id = 1,
      .seq = 1,
      .bytes = 1024,
      .kind = yisync::EM_SendKind::CHUNK,
  });
  EXPECT_FALSE(grant_before.has_value());

  scheduler.on_line_negotiated(1, 1024 * 1024);
  const auto grant_after = scheduler.try_acquire(yisync::T_SendRequest{
      .stream_id = 9001,
      .file_id = 1,
      .seq = 1,
      .bytes = 1024,
      .kind = yisync::EM_SendKind::CHUNK,
  });
  ASSERT_TRUE(grant_after.has_value());
  EXPECT_EQ(grant_after->line_id, 1U);
}

TEST(NetworkSchedulerTest, DisconnectTurnsPendingIntoLostSends) {
  yisync::T_MultiLineScheduler scheduler({line_config(1)});
  scheduler.on_line_connected(1);
  scheduler.on_line_negotiated(1, 1024 * 1024);
  ASSERT_TRUE(scheduler.try_acquire(yisync::T_SendRequest{
      .stream_id = 9001,
      .file_id = 42,
      .seq = 3,
      .bytes = 4096,
      .kind = yisync::EM_SendKind::CHUNK,
      .chunk_index = 7,
  }).has_value());

  scheduler.on_line_disconnected(1);
  const auto lost = scheduler.take_lost_sends();
  ASSERT_EQ(lost.size(), 1U);
  EXPECT_EQ(lost.front().stream_id, 9001U);
  EXPECT_EQ(lost.front().file_id, 42U);
  EXPECT_EQ(lost.front().chunk_index, 7U);
}

TEST(NetworkSchedulerTest, HeartbeatClearsPendingAndKeepsLineHealthy) {
  yisync::T_MultiLineScheduler scheduler({line_config(1)});
  scheduler.on_line_connected(1);
  scheduler.on_line_negotiated(1, 1024 * 1024);
  ASSERT_TRUE(scheduler.try_acquire(yisync::T_SendRequest{
      .stream_id = 9001,
      .file_id = 42,
      .seq = 3,
      .bytes = 4096,
      .kind = yisync::EM_SendKind::CHUNK,
      .chunk_index = 2,
  }).has_value());

  scheduler.on_heartbeat(1, yisync::T_Heartbeat{
      .stream_id = 9001,
      .next_seq = 3,
      .file_id = 42,
      .recv_window_bytes = 1024 * 1024,
      .received_chunks = {
          yisync::T_ReceivedChunk{.seq = 3, .file_id = 42, .chunk_index = 2},
      },
  });

  const auto snapshots = scheduler.snapshots();
  ASSERT_EQ(snapshots.size(), 1U);
  EXPECT_TRUE(snapshots.front().connected);
  EXPECT_TRUE(snapshots.front().negotiated);
  EXPECT_TRUE(snapshots.front().healthy);
  EXPECT_FALSE(snapshots.front().stale);
  EXPECT_EQ(snapshots.front().pending_sends, 0U);
}

TEST(NetworkSchedulerTest, HeartbeatTimeoutMarksLineStale) {
  yisync::T_MultiLineScheduler scheduler({line_config(1)});
  scheduler.on_line_connected(1);
  scheduler.on_line_negotiated(1, 1024 * 1024);
  ASSERT_TRUE(scheduler.try_acquire(yisync::T_SendRequest{
      .stream_id = 9001,
      .file_id = 42,
      .seq = 3,
      .bytes = 4096,
      .kind = yisync::EM_SendKind::DATA,
      .end_offset = 4096,
  }).has_value());

  scheduler.refill_ticks(6);
  const auto snapshots = scheduler.snapshots();
  ASSERT_EQ(snapshots.size(), 1U);
  EXPECT_TRUE(snapshots.front().stale);
}
