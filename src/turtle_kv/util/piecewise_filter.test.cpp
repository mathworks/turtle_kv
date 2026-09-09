//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/util/piecewise_filter.hpp>
//
#include <turtle_kv/util/piecewise_filter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/util/packed_piecewise_filter_view.hpp>
#include <turtle_kv/util/piecewise_filter.ipp>
#include <turtle_kv/util/piecewise_filter.live_subranges.hpp>
#include <turtle_kv/util/piecewise_filter.test.hpp>

#include <turtle_kv/core/testing/generate.hpp>

#include <batteries/async/debug_info.hpp>
#include <batteries/bit_ops/first_bit.hpp>
#include <batteries/bit_ops/mask.hpp>
#include <batteries/bit_ops/next_bit.hpp>

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
using turtle_kv::testing::build_filter_with_random_drops;
using turtle_kv::testing::drop_n_disjoint_intervals_from;
using turtle_kv::testing::get_packed_filter_from_data;
using turtle_kv::testing::PackedFilterData;
using turtle_kv::testing::pack_in_memory_filter;
using turtle_kv::testing::RandomDropResult;
using turtle_kv::testing::RandomStringGenerator;
using turtle_kv::testing::verify_filter_queries;

using turtle_kv::drop_item_range;

using llfs::KeyRangeOrder;

using batt::mask_from_interval;
using batt::StableStringStore;

using turtle_kv::PackedPiecewiseFilter;
using turtle_kv::PackedPiecewiseFilterStorage;


//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, InvalidFilterTest)
{
  // Interval starting with zero twice.
  //
  std::vector<Interval<usize>> live{Interval<usize>{0, 10},
                                    Interval<usize>{20, 30},
                                    Interval<usize>{0, 40}};
  StatusOr<PiecewiseFilter<usize>> filter = PiecewiseFilter<usize>::from_live(as_slice(live));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Overlapping intervals.
  //
  live = {Interval<usize>{0, 20}, Interval<usize>{10, 25}};
  filter = PiecewiseFilter<usize>::from_live(as_slice(live));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Backward interval.
  //
  live = {Interval<usize>{0, 30}, Interval<usize>{50, 40}};
  filter = PiecewiseFilter<usize>::from_live(as_slice(live));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, QueryTest)
{
  const u32 num_items = 10000;

  for (u32 seed = 0; seed < 100; ++seed) {
    std::default_random_engine rng{seed};

    auto [filter, live_items] = build_filter_with_random_drops(num_items, rng);

    EXPECT_TRUE(filter.check_invariants());

    verify_filter_queries(filter, live_items, num_items, seed, rng);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, PackedQueryTest)
{
  const u32 num_items = 10000;

  for (u32 seed = 0; seed < 100; ++seed) {
    std::default_random_engine rng{seed};

    auto [filter, live_items] = build_filter_with_random_drops(num_items, rng);

    EXPECT_TRUE(filter.check_invariants());

    // Pack the filter and construct a PackedPiecewiseFilter.
    //
    PackedFilterData packed_data = pack_in_memory_filter(filter);
    PackedPiecewiseFilter packed_filter = get_packed_filter_from_data(packed_data);

    // Verify the packed filter has the same number of live intervals.
    //
    EXPECT_EQ(packed_filter.size(), filter.size()) << BATT_INSPECT(seed);

    // Verify the packed filter produces identical intervals.
    //
    {
      auto mutable_iter = filter.begin();
      auto packed_iter = packed_filter.begin();
      while (mutable_iter != filter.end() && packed_iter != packed_filter.end()) {
        EXPECT_EQ(*mutable_iter, *packed_iter) << BATT_INSPECT(seed);
        ++mutable_iter;
        ++packed_iter;
      }
      EXPECT_EQ(mutable_iter, filter.end()) << BATT_INSPECT(seed);
      EXPECT_EQ(packed_iter, packed_filter.end()) << BATT_INSPECT(seed);
    }

    // Verify queries produce the same results as the in-memory filter.
    //
    verify_filter_queries(packed_filter, live_items, num_items, seed, rng);

    // Converting packed back to in-memory should produce identical filter.
    //
    PiecewiseFilter<u32> converted_filter{packed_filter};
    EXPECT_TRUE(converted_filter.check_invariants());
    Slice<const Interval<u32>> original_live = filter.live();
    Slice<const Interval<u32>> converted_live = converted_filter.live();
    ASSERT_EQ(original_live.size(), converted_live.size()) << BATT_INSPECT(seed);
    for (usize i = 0; i < original_live.size(); ++i) {
      EXPECT_EQ(original_live[i], converted_live[i]) << BATT_INSPECT(seed) << BATT_INSPECT(i);
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
    StableStringStore store;
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
    EXPECT_TRUE(filter.check_invariants());

    Interval<u32> next_live_interval = filter.find_live_range(Interval<u32>{0, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{0, num_keys}));

    // Drop an interval in the middle of the items range, and query the filter.
    //
    CInterval<std::string_view> cinterval_dropped{keys[100], keys[300]};
    Interval<u32> after =
        drop_item_range(filter, batt::as_const_slice(keys), cinterval_dropped, KeyRangeOrder{});

    EXPECT_LE(after.lower_bound, 100);
    EXPECT_GT(after.upper_bound, 300);

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
    Interval<u32> after2 =
        drop_item_range(filter, batt::as_const_slice(keys), interval_dropped, KeyRangeOrder{});

    EXPECT_LE(after2.lower_bound, 600);
    EXPECT_EQ(after2.upper_bound, keys.size() - 1);
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
    Interval<u32> after3 =
        drop_item_range(filter, batt::as_const_slice(keys), cinterval_dropped, KeyRangeOrder{});

    EXPECT_LE(after3.lower_bound, 500);
    EXPECT_GE(after3.upper_bound, num_keys);
    EXPECT_FALSE(filter.live_at_index(num_keys - 1));
    EXPECT_TRUE(filter.live_at_index(301));

    next_live_index = filter.live_lower_bound(500);
    EXPECT_EQ(next_live_index, num_keys);

    next_live_interval = filter.find_live_range(Interval<u32>{301, num_keys});
    EXPECT_EQ(next_live_interval, (Interval<u32>{301, 500}));

    EXPECT_TRUE(filter.check_invariants());

    // Drop everything.
    //
    auto everything = Interval<u32>{0, num_keys};
    Interval<u32> after4 = filter.drop_index_range(everything);

    EXPECT_EQ(after4, everything);

    EXPECT_FALSE(filter.live_at_index(0));
    next_live_index = filter.live_lower_bound(0);
    EXPECT_EQ(next_live_index, num_keys);

    next_live_interval = filter.find_live_range(Interval<u32>{0, num_keys});
    EXPECT_TRUE(next_live_interval.empty());

    EXPECT_TRUE(filter.check_invariants());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, LiveSubranges)
{
  std::uniform_int_distribution<u32> pick_bound{0, 64};

  const auto pick_interval = [&](auto& rng) {
    Interval<u32> i{pick_bound(rng), pick_bound(rng)};
    if (i.upper_bound < i.lower_bound) {
      std::swap(i.lower_bound, i.upper_bound);
    }
    return i;
  };

  std::array<Interval<u32>, 1> init_live{{{0, 64}}};

  const usize n_seeds = 100000;
  const usize n_drops = 32;
  const usize n_queries = 15;
  const usize first_seed = 0;

  PiecewiseFilter<u32> filter;

  const auto query_as_bits = [&](Interval<u32> query) {
    u64 bits = 0;
    filter.live_subranges_of(query) | batt::seq::for_each([&bits](const Interval<u32>& live) {
      bits |= mask_from_interval(live);
    });
    return bits;
  };

  for (usize seed_i = first_seed; seed_i < first_seed + n_seeds; ++seed_i) {
    std::default_random_engine rng{seed_i};

    for (usize i = 0; i < n_drops; ++i) {
      BATT_DEBUG_INFO(BATT_INSPECT(i) << BATT_INSPECT(seed_i));

      filter = BATT_OK_RESULT_OR_PANIC(PiecewiseFilter<u32>::from_live(batt::as_slice(init_live)));
      u64 filter_state = ~u64{0};

      std::vector<Interval<u32>> dropped_ranges =
          drop_n_disjoint_intervals_from(&filter, i, init_live[0], rng).second;

      for (const Interval<u32>& drop_interval : dropped_ranges) {
        const u64 drop_mask = mask_from_interval(drop_interval);
        filter_state &= ~drop_mask;
      }

      // Also test via PackedPiecewiseFilter.
      //
      PackedFilterData packed_data = pack_in_memory_filter(filter);
      PackedPiecewiseFilter packed_filter = get_packed_filter_from_data(packed_data);

      const auto packed_query_as_bits = [&](Interval<u32> query) {
        u64 bits = 0;
        packed_filter.live_subranges_of(query) |
            batt::seq::for_each([&bits](const Interval<u32>& live) {
              bits |= mask_from_interval(live);
            });
        return bits;
      };

      for (usize j = 0; j < n_queries; ++j) {
        const Interval<u32> query_interval = pick_interval(rng);
        const u64 query_mask = mask_from_interval(query_interval);
        const u64 expected_bits = query_mask & filter_state;
        const u64 actual_bits = query_as_bits(query_interval);

        ASSERT_EQ(std::bitset<64>{expected_bits}, std::bitset<64>{actual_bits})
            << BATT_INSPECT(seed_i) << BATT_INSPECT(query_interval)
            << BATT_INSPECT(query_interval.size()) << BATT_INSPECT(std::bitset<64>{query_mask});

        const u64 packed_actual_bits = packed_query_as_bits(query_interval);

        ASSERT_EQ(std::bitset<64>{expected_bits}, std::bitset<64>{packed_actual_bits})
            << "PackedPiecewiseFilter mismatch: " << BATT_INSPECT(seed_i)
            << BATT_INSPECT(query_interval) << BATT_INSPECT(query_interval.size())
            << BATT_INSPECT(std::bitset<64>{query_mask});
      }
    }
  }
}

}  // namespace
