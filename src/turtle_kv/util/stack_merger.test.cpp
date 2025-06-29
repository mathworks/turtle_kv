#include <turtle_kv/util/stack_merger.hpp>
//
#include <turtle_kv/util/stack_merger.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/import/env.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/stream_util.hpp>

#include <algorithm>
#include <random>
#include <vector>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::as_slice;
using turtle_kv::getenv_as;
using turtle_kv::Optional;
using turtle_kv::Slice;
using turtle_kv::StackMerger;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class StackMergerTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    std::cerr << BATT_INSPECT(seed) << BATT_INSPECT(n_trials) << std::endl;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize n_trials = getenv_as<usize>("N").value_or(1000000);
  usize seed = getenv_as<usize>("SEED").value_or(std::random_device{}());
  std::default_random_engine rng{seed};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(StackMergerTest, HeapSort)
{
  bool insert_each = false;
  for (usize trial = 0; trial < n_trials; ++trial, insert_each = !insert_each) {
    // Generate a list of random numbers.
    //
    std::vector<i64> nums(100);
    std::iota(nums.begin(), nums.end(), 1);
    std::shuffle(nums.begin(), nums.end(), rng);

    VLOG(1) << BATT_INSPECT_RANGE(nums);

    // Create a StackMerger from the random numbers.
    //
    Optional<StackMerger<i64>> opt_m;
    if (insert_each) {
      opt_m.emplace(nums.size());
      for (i64& n : nums) {
        opt_m->check_invariants();
        opt_m->insert(&n);
      }
    } else {
      opt_m.emplace(as_slice(nums));
    }
    StackMerger<i64>& m = *opt_m;
    m.check_invariants();

    EXPECT_FALSE(m.empty());
    EXPECT_EQ(*m.first(), 1);

    for (usize i = 0; i < nums.size(); ++i) {
      ASSERT_FALSE(m.empty());
      EXPECT_EQ(*m.first(), i + 1);

      m.remove_first();
      m.check_invariants();
    }

    EXPECT_TRUE(m.empty());
  }
}

template <typename T>
struct SliceFrontOrder {
  bool operator()(const Slice<T>* left, const Slice<T>* right) const
  {
    return left->front() < right->front();
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(StackMergerTest, KWiseMerge)
{
  bool insert_each = false;
  for (usize trial = 0; trial < n_trials; ++trial, insert_each = !insert_each) {
    // Generate a list of random numbers.
    //
    std::vector<i64> nums(100);
    std::iota(nums.begin(), nums.end(), 1);
    std::shuffle(nums.begin(), nums.end(), rng);

    // Break nums into randomly sized slices.
    //
    std::vector<Slice<i64>> slices;
    {
      i64* p_next = nums.data();
      usize n_remaining = nums.size();

      while (n_remaining > 0) {
        std::uniform_int_distribution<usize> pick_size{1, 10};

        usize slice_size = std::min<usize>(pick_size(rng), n_remaining);
        slices.push_back(as_slice(p_next, slice_size));
        p_next += slice_size;
        n_remaining -= slice_size;
      }
    }

    // Sort each of the slices.
    //
    for (Slice<i64>& slice : slices) {
      std::sort(slice.begin(), slice.end());
    }

    VLOG(1) << BATT_INSPECT_RANGE(nums) << std::endl << BATT_INSPECT_RANGE(slices);

    // Create a StackMerger from the sorted slices.
    //
    Optional<StackMerger<Slice<i64>, SliceFrontOrder<i64>>> opt_m;
    if (insert_each) {
      opt_m.emplace(slices.size());
      for (Slice<i64>& s : slices) {
        opt_m->check_invariants();
        opt_m->insert(&s);
      }
    } else {
      opt_m.emplace(as_slice(slices));
    }

    auto& m = *opt_m;
    m.check_invariants();

    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.first()->front(), 1);

    for (usize i = 0; i < nums.size(); ++i) {
      ASSERT_FALSE(m.empty());

      Slice<i64>* first_slice = m.first();

      ASSERT_FALSE(first_slice->empty());
      EXPECT_EQ(first_slice->front(), i + 1);

      first_slice->drop_front();
      if (first_slice->empty()) {
        m.remove_first();
      } else {
        m.update_first();
      }
      m.check_invariants();
    }

    EXPECT_TRUE(m.empty());
  }
}

}  // namespace
