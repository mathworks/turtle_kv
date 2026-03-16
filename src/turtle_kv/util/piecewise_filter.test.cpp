#include <turtle_kv/util/piecewise_filter.hpp>
//
#include <turtle_kv/util/piecewise_filter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

#include <algorithm>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::CInterval;
using turtle_kv::Interval;
using turtle_kv::PiecewiseFilter;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::testing::RandomStringGenerator;

using turtle_kv::drop_item_range;

using llfs::KeyRangeOrder;
using llfs::StableStringStore;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, InvalidFilterTest)
{
  // Interval starting with zero twice.
  //
  std::vector<Interval<usize>> dropped{Interval<usize>{0, 10},
                                       Interval<usize>{20, 30},
                                       Interval<usize>{0, 40}};
  StatusOr<PiecewiseFilter<usize>> filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Overlapping intervals.
  //
  dropped = {Interval<usize>{0, 20}, Interval<usize>{10, 25}};
  filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Backward interval.
  //
  dropped = {Interval<usize>{0, 30}, Interval<usize>{50, 40}};
  filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, QueryTest)
{
  const usize num_items = 1000;

  for (usize seed = 0; seed < 100; ++seed) {
    std::default_random_engine rng{seed};

    PiecewiseFilter<usize> filter;
    EXPECT_TRUE(filter.check_invariants());

    // All items start unfiltered.
    //
    std::set<usize> live_items;
    for (usize i = 0; i < num_items; ++i) {
      live_items.insert(i);
    }

    // Drop random intervals.
    //
    std::uniform_int_distribution<usize> pick_num_dropped{100, num_items / 2};
    usize num_intervals_dropped = pick_num_dropped(rng);
    for (usize i = 0; i < num_intervals_dropped; ++i) {
      std::uniform_int_distribution<usize> pick_interval_start{0, num_items - 1};
      usize start_i = pick_interval_start(rng);

      std::uniform_int_distribution<usize> pick_interval_end{start_i, num_items};
      usize end_i = pick_interval_end(rng);

      for (usize j = start_i; j < end_i; ++j) {
        live_items.erase(j);
      }

      filter.drop_index_range(Interval<usize>{start_i, end_i});
    }

    EXPECT_TRUE(filter.check_invariants());

    // Test live_at_index
    //
    for (usize i = 0; i < num_items; ++i) {
      EXPECT_EQ(filter.live_at_index(i), live_items.count(i) > 0);
    }

    // Test live_lower_bound
    //
    for (usize i = 0; i < num_items; ++i) {
      auto iter = live_items.lower_bound(i);
      usize expected = (iter != live_items.end()) ? *iter : num_items;
      EXPECT_EQ(filter.live_lower_bound(i), expected);
    }

    // Test find_live_range
    //
    for (usize i = 0; i < 100; ++i) {
      std::uniform_int_distribution<usize> pick_interval_start{0, num_items - 1};
      usize start_i = pick_interval_start(rng);

      std::uniform_int_distribution<usize> pick_interval_end{start_i, num_items};
      usize end_i = pick_interval_end(rng);

      auto iter = live_items.lower_bound(start_i);
      if (iter == live_items.end() || *iter >= end_i) {
        EXPECT_EQ(filter.find_live_range(Interval<usize>{start_i, end_i}),
                  (Interval<usize>{end_i, end_i}));
      } else {
        usize first = *iter;
        usize last = first;
        auto next = iter;

        while (next != live_items.end() && *next < end_i && *next == last) {
          ++last;
          ++next;
        }

        EXPECT_EQ(filter.find_live_range(Interval<usize>{start_i, end_i}),
                  (Interval<usize>{first, last}));
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, KeyQueryTest)
{
  for (usize i = 0; i < 100; ++i) {
    u32 num_keys = 1000;
    std::vector<std::string_view> keys;
    keys.reserve(num_keys);

    // Generate some random strings and sort them.
    //
    std::default_random_engine rng{i};
    RandomStringGenerator generate_key;
    llfs::StableStringStore store;
    std::unordered_set<std::string_view> keys_set;
    for (u32 j = 0; j < num_keys; ++j) {
      std::string_view key = generate_key(rng, store);
      if (keys_set.contains(key)) {
        continue;
      }

      keys.emplace_back(key);
    }
    std::sort(keys.begin(), keys.end());

    PiecewiseFilter<u32> filter;

    // First verify that everything should be live since nothing has been dropped yet.
    //
    EXPECT_EQ(filter.dropped_total(), 0);
    EXPECT_TRUE(filter.check_invariants());

    Interval<u32> next_live_interval = filter.find_live_range(Interval<u32>{0, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{0, num_keys}));

    // Drop an interval in the middle of the items range, and query the filter.
    //
    CInterval<std::string_view> cinterval_dropped{keys[100], keys[300]};
    drop_item_range(filter, batt::as_const_slice(keys), cinterval_dropped, KeyRangeOrder{});

    EXPECT_EQ(filter.dropped_total(), 201);

    EXPECT_TRUE(filter.live_at_index(99));
    EXPECT_TRUE(filter.live_at_index(301));
    EXPECT_FALSE(filter.live_at_index(100));
    EXPECT_FALSE(filter.live_at_index(200));
    EXPECT_FALSE(filter.live_at_index(300));

    u32 next_live_index = filter.live_lower_bound(100);
    EXPECT_EQ(next_live_index, 301);

    next_live_interval = filter.find_live_range(Interval<u32>{0, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{0, 100}));

    next_live_interval = filter.find_live_range(Interval<u32>{301, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{301, num_keys}));

    // When find_live_range is called with a filtered starting index, the returned interval
    // starts at the next live index.
    //
    EXPECT_EQ(filter.find_live_range(Interval<u32>{100, num_keys}), (Interval<u32>{301, num_keys}));

    // Drop another interval that is not adjacent to the previously dropped one.
    //
    Interval<std::string_view> interval_dropped{keys[600], keys.back()};
    drop_item_range(filter, batt::as_const_slice(keys), interval_dropped, KeyRangeOrder{});

    EXPECT_TRUE(filter.live_at_index(num_keys - 1));
    EXPECT_TRUE(filter.live_at_index(400));
    EXPECT_FALSE(filter.live_at_index(600));
    EXPECT_FALSE(filter.live_at_index(num_keys - 2));

    next_live_index = filter.live_lower_bound(700);
    EXPECT_EQ(next_live_index, num_keys - 1);

    next_live_index = filter.live_lower_bound(num_keys - 1);
    EXPECT_EQ(next_live_index, num_keys - 1);

    next_live_interval = filter.find_live_range(Interval<u32>{num_keys - 1, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{num_keys - 1, num_keys}));

    next_live_interval = filter.find_live_range(Interval<u32>{301, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{301, 600}));

    // Drop another range in the middle, this time with overlap until the end.
    //
    cinterval_dropped = CInterval<std::string_view>{keys[500], keys[num_keys - 1]};
    drop_item_range(filter, batt::as_const_slice(keys), cinterval_dropped, KeyRangeOrder{});

    EXPECT_FALSE(filter.live_at_index(num_keys - 1));
    EXPECT_TRUE(filter.live_at_index(301));

    next_live_index = filter.live_lower_bound(500);
    EXPECT_EQ(next_live_index, num_keys);

    next_live_interval = filter.find_live_range(Interval<u32>{301, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{301, 500}));

    EXPECT_TRUE(filter.check_invariants());

    // Drop everything.
    //
    filter.drop_index_range(Interval<u32>{0, num_keys});

    EXPECT_EQ(filter.dropped_total(), num_keys);

    EXPECT_FALSE(filter.live_at_index(0));
    next_live_index = filter.live_lower_bound(0);
    EXPECT_EQ(next_live_index, num_keys);

    next_live_interval = filter.find_live_range(Interval<u32>{0, num_keys});
    EXPECT_TRUE(next_live_interval.empty());

    EXPECT_TRUE(filter.check_invariants());
  }
}
}  // namespace
