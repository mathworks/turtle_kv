//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PIECEWISE_FILTER_TEST_HPP

#include "piecewise_filter.hpp"
#include "piecewise_filter_storage_model.concept.hpp"

#include <turtle_kv/import/interval.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/debug_info.hpp>

#include <random>
#include <vector>

namespace turtle_kv {
namespace testing {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Randomly drops `n` ranges within the specified live range of the passed filter.
 *
 * Requires that:
 *   - `drop_within` must be live in `filter`
 *   - `drop_within.size()` must be large enough to fit `n` disjoint intervals
 *
 * \return a pair of { total offset size dropped, vector of the dropped intervals }
 */
template <typename Rng, typename OffsetT, PiecewiseFilterMutableStorageModel<OffsetT> ModelT>
inline std::pair<OffsetT, std::vector<Interval<OffsetT>>> drop_n_disjoint_intervals_from(
    BasicPiecewiseFilter<OffsetT, ModelT>* filter,
    usize n,
    const Interval<OffsetT>& drop_within,
    Rng& rng)
{
  constexpr bool debug = false;

  if constexpr (debug) {
    std::cerr << BATT_INSPECT(n) << std::endl;
  }

  OffsetT dropped_total_size = 0;
  std::vector<Interval<OffsetT>> dropped_ranges;

  if (n == 0) {
    return std::make_pair(dropped_total_size, dropped_ranges);
  }

  BATT_CHECK_LE(n * 2 - 1, drop_within.size());
  BATT_CHECK_EQ(filter->live().empty(), false);
  BATT_CHECK_EQ(filter->find_live_range(drop_within), drop_within);

  usize drops_remaining = n;
  OffsetT next_droppable = drop_within.lower_bound;
  const OffsetT live_lower_bound = filter->live().front().lower_bound;
  const OffsetT live_upper_bound = filter->live().back().upper_bound;

  BATT_DEBUG_INFO(BATT_INSPECT(dropped_total_size)
                  << BATT_INSPECT_RANGE(dropped_ranges) << BATT_INSPECT(drop_within)
                  << BATT_INSPECT(n) << BATT_INSPECT(drops_remaining)
                  << BATT_INSPECT(next_droppable) << BATT_INSPECT(live_lower_bound)
                  << BATT_INSPECT(live_upper_bound));

  if constexpr (debug) {
    std::cerr << BATT_INSPECT_RANGE(filter->live()) << std::endl;
  }

  for (usize drop_i = 0; drop_i < n; ++drop_i) {
    BATT_CHECK_GE(next_droppable, 0);
    BATT_CHECK_LT(next_droppable, drop_within.upper_bound);

    std::uniform_int_distribution<usize> pick_lower_bound{
        next_droppable,
        drop_within.upper_bound - (drops_remaining * 2 - 1),
    };
    const OffsetT lower_bound_i = pick_lower_bound(rng);

    std::uniform_int_distribution<usize> pick_upper_bound{
        lower_bound_i + 1,
        drop_within.upper_bound - (drops_remaining * 2 - 2),
    };
    const OffsetT upper_bound_i = pick_upper_bound(rng);

    BATT_CHECK_LT(lower_bound_i, upper_bound_i);
    BATT_CHECK_GE(lower_bound_i, next_droppable);

    dropped_total_size += upper_bound_i - lower_bound_i;

    const usize live_count_before = filter->live().size();
    //----- --- -- -  -  -   -
    dropped_ranges.push_back(Interval<OffsetT>{lower_bound_i, upper_bound_i});
    if constexpr (debug) {
      std::cerr << "  dropping: " << lower_bound_i << ".." << upper_bound_i << std::endl;
    }
    filter->drop_index_range(Interval<OffsetT>{lower_bound_i, upper_bound_i});
    //----- --- -- -  -  -   -
    const usize live_count_after = filter->live().size();

    if constexpr (debug) {
      std::cerr << BATT_INSPECT_RANGE(filter->live()) << std::endl;
    }

    if (lower_bound_i == live_lower_bound && upper_bound_i == live_upper_bound) {
      BATT_CHECK_EQ(live_count_after + 1, live_count_before);

    } else if ((lower_bound_i == live_lower_bound && upper_bound_i != live_upper_bound) ||
               (upper_bound_i == live_upper_bound && lower_bound_i != live_lower_bound)) {
      BATT_CHECK_EQ(live_count_after, live_count_before);

    } else {
      BATT_CHECK_EQ(live_count_after, live_count_before + 1);
    }

    --drops_remaining;
    next_droppable = upper_bound_i + 1;
  }

  return std::make_pair(dropped_total_size, dropped_ranges);
}

struct PackedFilterData {
  std::vector<little_u32> values;
  bool start_is_live;
};

inline PackedFilterData pack_in_memory_filter(const PiecewiseFilter<u32>& filter)
{
  PackedFilterData data;
  data.start_is_live = false;
  Slice<const Interval<u32>> live = filter.live();

  if (live.empty()) {
    return data;
  }

  data.start_is_live = (live[0].lower_bound == PiecewiseFilter<u32>::kMinLowerBound);

  for (const Interval<u32>& range : live) {
    if (range.lower_bound != PiecewiseFilter<u32>::kMinLowerBound) {
      data.values.push_back(range.lower_bound);
    }
    if (range.upper_bound != PiecewiseFilter<u32>::kMaxUpperBound) {
      data.values.push_back(range.upper_bound);
    }
  }

  return data;
}

inline PackedPiecewiseFilter get_packed_filter_from_data(const PackedFilterData& data)
{
  return PackedPiecewiseFilter{PackedPiecewiseFilterStorage{
      batt::as_const_slice(data.values), data.start_is_live}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct RandomDropResult {
  PiecewiseFilter<u32> filter;
  std::set<u32> live_items;
};

inline RandomDropResult build_filter_with_random_drops(u32 num_items, std::default_random_engine& rng)
{
  RandomDropResult result;
  for (u32 i = 0; i < num_items; ++i) {
    result.live_items.insert(i);
  }

  std::uniform_int_distribution<u32> pick_num_dropped{100, num_items / 2};
  u32 num_intervals_dropped = pick_num_dropped(rng);
  for (u32 i = 0; i < num_intervals_dropped; ++i) {
    std::uniform_int_distribution<u32> pick_interval_start{0, num_items - 1};
    u32 start_i = pick_interval_start(rng);

    std::uniform_int_distribution<u32> pick_interval_end{start_i, num_items};
    u32 end_i = pick_interval_end(rng);

    for (u32 j = start_i; j < end_i; ++j) {
      result.live_items.erase(j);
    }

    result.filter.drop_index_range(Interval<u32>{start_i, end_i});
  }

  return result;
}

template <typename FilterT>
inline void verify_filter_queries(const FilterT& filter,
                           const std::set<u32>& live_items,
                           u32 num_items,
                           u32 seed,
                           std::default_random_engine& rng)
{
  for (u32 i = 0; i < num_items; ++i) {
    bool expected_live = live_items.count(i) > 0;
    bool actual_live = filter.live_at_index(i);
    EXPECT_EQ(actual_live, expected_live) << BATT_INSPECT(seed) << BATT_INSPECT(i);
  }

  for (u32 i = 0; i < num_items; ++i) {
    auto iter = live_items.lower_bound(i);
    u32 expected = (iter != live_items.end()) ? *iter : num_items;
    u32 actual = filter.live_lower_bound(i);
    EXPECT_EQ(actual, expected) << BATT_INSPECT(seed) << BATT_INSPECT(i);
  }

  for (u32 i = 0; i < 100; ++i) {
    std::uniform_int_distribution<u32> pick_interval_start{0, num_items - 1};
    u32 start_i = pick_interval_start(rng);

    std::uniform_int_distribution<u32> pick_interval_end{start_i, num_items};
    u32 end_i = pick_interval_end(rng);

    auto iter = live_items.lower_bound(start_i);
    Interval<u32> expected_range;

    if (iter == live_items.end() || *iter >= end_i) {
      expected_range = Interval<u32>{end_i, end_i};
    } else {
      u32 first = *iter;
      u32 last = first + 1;
      auto next = std::next(iter);

      while (next != live_items.end() && *next < end_i && *next == last) {
        ++last;
        ++next;
      }

      expected_range = Interval<u32>{first, last};
    }

    Interval<u32> actual_range = filter.find_live_range(Interval<u32>{start_i, end_i});
    EXPECT_EQ(actual_range, expected_range) << BATT_INSPECT(seed) << BATT_INSPECT(i);
  }
}

}  // namespace testing
}  // namespace turtle_kv
