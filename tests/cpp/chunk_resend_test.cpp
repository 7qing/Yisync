#include "sender/yisync_chunk_resend.hpp"

#include <vector>

#include <gtest/gtest.h>

TEST(ChunkResendTest, UsesDefaultTailFirstOrderThenSequentialChunks) {
  yisync::ChunkResendState state;
  yisync::initialize_chunk_resend_state(state, 3, {});

  EXPECT_EQ(yisync::next_chunk_to_send(state, 1, 10), 2U);
  auto mark = yisync::mark_chunk_sent(state, 2, 7, 1);
  EXPECT_TRUE(mark.marked);
  EXPECT_FALSE(mark.retransmit);
  EXPECT_EQ(mark.attempt, 1U);

  EXPECT_EQ(yisync::next_chunk_to_send(state, 2, 10), 0U);
}

TEST(ChunkResendTest, MissingHintPromotesGapAfterRetransmitTicks) {
  yisync::ChunkResendState state;
  yisync::initialize_chunk_resend_state(state, 3, {});

  (void)yisync::mark_chunk_sent(state, 2, 1, 1);
  (void)yisync::mark_chunk_sent(state, 0, 1, 2);
  EXPECT_TRUE(yisync::acknowledge_chunk(state, 2));

  const auto applied = yisync::apply_missing_hints(
      state,
      9,
      4,
      std::vector<yisync::MissingChunkRange>{
          yisync::MissingChunkRange{
              .seq = 9,
              .file_id = 4,
              .first_chunk_index = 0,
              .last_chunk_index = 1,
          },
      });

  ASSERT_EQ(applied.size(), 1U);
  EXPECT_EQ(applied.front().first_chunk_index, 0U);
  EXPECT_EQ(applied.front().last_chunk_index, 1U);
  EXPECT_EQ(yisync::next_chunk_to_send(state, 3, 10), 1U);
  (void)yisync::mark_chunk_sent(state, 1, 2, 3);
  EXPECT_FALSE(yisync::next_chunk_to_send(state, 5, 10).has_value());
  EXPECT_EQ(yisync::next_chunk_to_send(state, 12, 10), 0U);
}

TEST(ChunkResendTest, LostChunkBecomesPriorityAgain) {
  yisync::ChunkResendState state;
  yisync::initialize_chunk_resend_state(state, 2, {0, 1});

  (void)yisync::mark_chunk_sent(state, 0, 3, 4);
  ASSERT_TRUE(yisync::mark_chunk_lost(state, 0));
  EXPECT_EQ(yisync::next_chunk_to_send(state, 5, 100), 0U);

  const auto mark = yisync::mark_chunk_sent(state, 0, 4, 5);
  EXPECT_TRUE(mark.retransmit);
  EXPECT_EQ(mark.attempt, 2U);
}
