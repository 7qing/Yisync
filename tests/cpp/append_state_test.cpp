#include "sender/yisync_append_state.hpp"

#include <gtest/gtest.h>

TEST(AppendStateTest, StartsCreateMissingPlanAndAdvancesWithHeartbeat) {
  yisync::AppendSendState state;
  yisync::start_append_plan(state,
                            yisync::SyncStart{
                                .stream_id = 9001,
                                .start_file_id = 7,
                                .start_offset = 0,
                                .start_action = yisync::StartAction::CreateMissing,
                            },
                            3,
                            96 * 1024);

  EXPECT_TRUE(state.needs_create);
  EXPECT_FALSE(state.create_ready);
  EXPECT_EQ(state.next_offset, 0U);
  EXPECT_EQ(state.done_next_seq, 4U);

  yisync::mark_append_create_sent(state, 2);
  EXPECT_TRUE(yisync::mark_append_create_ready_from_heartbeat(
      state,
      yisync::Heartbeat{.stream_id = 9001, .next_seq = 3, .file_id = 7}));
  EXPECT_TRUE(state.create_ready);

  yisync::mark_append_data_sent(state, 1, 64 * 1024);
  EXPECT_TRUE(yisync::mark_append_data_ready_from_heartbeat(
      state,
      yisync::Heartbeat{
          .stream_id = 9001,
          .next_seq = 3,
          .file_id = 7,
          .offset = 64 * 1024,
          .durable_offset = 64 * 1024,
      }));
  EXPECT_EQ(state.next_offset, 64 * 1024U);
  EXPECT_FALSE(state.data_sent);
}

TEST(AppendStateTest, ResumeExistingPlanStartsReadyWithoutCreate) {
  yisync::AppendSendState state;
  yisync::start_append_plan(state,
                            yisync::SyncStart{
                                .stream_id = 9001,
                                .start_file_id = 9,
                                .start_offset = 32 * 1024,
                                .start_action = yisync::StartAction::ResumeExisting,
                            },
                            5,
                            48 * 1024);

  EXPECT_FALSE(state.needs_create);
  EXPECT_TRUE(state.create_ready);
  EXPECT_EQ(state.offset, 32 * 1024U);
  EXPECT_EQ(state.next_offset, 32 * 1024U);

  yisync::mark_append_data_sent(state, 1, 16 * 1024);
  const yisync::Heartbeat complete_heartbeat{
      .stream_id = 9001,
      .next_seq = 6,
      .file_id = 9,
      .offset = 48 * 1024,
      .durable_offset = 48 * 1024,
  };
  EXPECT_TRUE(yisync::mark_append_data_ready_from_heartbeat(state, complete_heartbeat));
  EXPECT_TRUE(yisync::append_complete_by_heartbeat(state,
                                                  complete_heartbeat,
                                                  yisync::EntryKind::RegularFile,
                                                  48 * 1024));
}

TEST(AppendStateTest, DetectsLostAppendInflightSend) {
  yisync::AppendSendState state;
  yisync::start_append_plan(state,
                            yisync::SyncStart{
                                .stream_id = 9001,
                                .start_file_id = 11,
                                .start_offset = 0,
                                .start_action = yisync::StartAction::CreateMissing,
                            },
                            8,
                            64);
  yisync::mark_append_data_sent(state, 2, 64);

  EXPECT_TRUE(yisync::append_lost_matches(
      state,
      yisync::LostSend{
          .line_id = 2,
          .kind = yisync::SendKind::Data,
          .stream_id = 9001,
          .file_id = 11,
          .seq = 8,
          .offset = 0,
          .end_offset = 64,
      }));

  yisync::reset_append_inflight(state);
  EXPECT_FALSE(state.data_sent);
  EXPECT_FALSE(yisync::append_lost_matches(
      state,
      yisync::LostSend{
          .line_id = 2,
          .kind = yisync::SendKind::Data,
          .stream_id = 9001,
          .file_id = 11,
          .seq = 8,
          .offset = 0,
          .end_offset = 64,
      }));
}
