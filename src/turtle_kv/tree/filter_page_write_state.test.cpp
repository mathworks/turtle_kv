#include <turtle_kv/tree/filter_page_write_state.hpp>
//
#include <turtle_kv/tree/filter_page_write_state.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/async/task.hpp>

#include <boost/asio/io_context.hpp>

namespace {

using namespace ::turtle_kv::int_types;
using ::turtle_kv::FilterPageWriteState;

constexpr usize kNumFakeWrites = 10;
constexpr usize kNumTaskActivations = 10;

/*
  Test plan:

  1. Create a FilterPageWriteState, verify all counts are zero and is_halted is false.
  2. is_halted should be true after halt is called
  3. join should return immediately if active_count is zero and halt has already been called
  4. join should wait for halt to be called
  5. join should wait for finished count to catch up with started (do this for active count
      of 1..100)
  6. start after halt should return kClosed

  DEATH

  7. extra finish calls (not matched to a start) should panic

 */

class TreeFilterPageWriteStateTest : public ::testing::Test
{
 public:
  boost::intrusive_ptr<FilterPageWriteState> state = FilterPageWriteState::make_new();
  boost::asio::io_context io;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 1. Create a FilterPageWriteState, verify all counts are zero and is_halted is false.
//
TEST_F(TreeFilterPageWriteStateTest, MakeNew)
{
  ASSERT_NE(this->state, nullptr);
  EXPECT_FALSE(this->state->is_halted());
  EXPECT_EQ(this->state->started_count(), 0);
  EXPECT_EQ(this->state->finished_count(), 0);
  EXPECT_EQ(this->state->active_count(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 2. is_halted should be true after halt is called
//
TEST_F(TreeFilterPageWriteStateTest, IsHalted)
{
  this->state->halt();

  EXPECT_TRUE(this->state->is_halted());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 3. join should return immediately if active_count is zero and halt has already been called
//
TEST_F(TreeFilterPageWriteStateTest, JoinWhenInactiveAndHaltedAsync)
{
  this->state->halt();

  batt::Task task{this->io.get_executor(), [this] {
                    this->state->join();
                  }};

  EXPECT_FALSE(task.try_join());

  this->io.poll();
  this->io.restart();

  EXPECT_TRUE(task.try_join());
}

TEST_F(TreeFilterPageWriteStateTest, JoinWhenInactiveAndHaltedSync)
{
  this->state->halt();
  this->state->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 4. join should wait for halt to be called
//
TEST_F(TreeFilterPageWriteStateTest, JoinWaitForHalt)
{
  for (usize i = 0; i < kNumFakeWrites; ++i) {
    EXPECT_TRUE(this->state->start().ok());
    this->state->finish();
  }

  EXPECT_EQ(this->state->active_count(), 0);
  EXPECT_EQ(this->state->started_count(), kNumFakeWrites);
  EXPECT_EQ(this->state->finished_count(), kNumFakeWrites);

  batt::Task task{this->io.get_executor(), [this] {
                    this->state->join();
                  }};

  for (usize i = 0; i < kNumTaskActivations; ++i) {
    io.poll();
    io.restart();

    EXPECT_FALSE(task.try_join());
  }

  this->state->halt();

  io.poll();
  io.restart();

  EXPECT_TRUE(task.try_join());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 5. join should wait for finished count to catch up with started (do this for active count
//     of 1..100)
//
TEST_F(TreeFilterPageWriteStateTest, JoinWaitForFinish)
{
  constexpr usize kMaxStarts = 100;

  for (usize i = 1; i < kMaxStarts; ++i) {
    this->state = FilterPageWriteState::make_new();

    for (usize j = 0; j < i; ++j) {
      ASSERT_TRUE(this->state->start().ok());
    }

    this->state->halt();

    ASSERT_TRUE(this->state->is_halted());

    batt::Task task{this->io.get_executor(), [this] {
                      this->state->join();
                    }};

    for (usize j = 0; j < i; ++j) {
      EXPECT_EQ(this->state->active_count(), (i64)(i - j));

      for (usize k = 0; k < kNumTaskActivations; ++k) {
        io.poll();
        io.restart();

        EXPECT_FALSE(task.try_join());
      }

      this->state->finish();
    }

    io.poll();
    io.restart();

    EXPECT_TRUE(task.try_join());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 6. start after halt should return kClosed
//
TEST_F(TreeFilterPageWriteStateTest, StartAfterHalt)
{
  this->state->halt();

  EXPECT_EQ(this->state->start(), batt::StatusCode::kClosed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// 7. (DEATH) extra finish calls (not matched to a start) should panic
//
TEST_F(TreeFilterPageWriteStateTest, UnmatchedFinishDeath)
{
  EXPECT_TRUE(this->state->start().ok());
  this->state->halt();
  this->state->finish();

  EXPECT_DEATH(this->state->finish(),
               "Assertion failed: observed_started - observed_finished >= 0");
}

}  // namespace
