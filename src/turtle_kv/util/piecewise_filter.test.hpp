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

}  // namespace testing
}  // namespace turtle_kv
