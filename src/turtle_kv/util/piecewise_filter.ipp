//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PIECEWISE_FILTER_IPP

#include "piecewise_filter.hpp"

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
/*static*/ StatusOr<BasicPiecewiseFilter<OffsetT, ModelT>>
BasicPiecewiseFilter<OffsetT, ModelT>::from_live(const Slice<const Interval<OffsetT>>& live)
  requires PiecewiseFilterMutableStorageModel<ModelT, OffsetT>
{
  Self filter;

  filter.live_().clear();
  filter.live_().insert(filter.live_().end(), live.begin(), live.end());

  if (!filter.check_invariants()) {
    return Status{::batt::StatusCode::kInvalidArgument};
  }

  return filter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
BasicPiecewiseFilter<OffsetT, ModelT>::BasicPiecewiseFilter() noexcept
    : ModelT{{Interval<OffsetT>{Self::kMinLowerBound, Self::kMaxUpperBound}}}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
bool BasicPiecewiseFilter<OffsetT, ModelT>::check_invariants() const
{
  Optional<OffsetT> prev_upper_bound = None;

  // Check that:
  //  - all intervals are in non-decreasing order
  //  - no intervals overlap or are adjacent (i.e., prev.upper_bound == next.lower_bound)
  //
  for (const Interval<OffsetT>& range : this->live_()) {
    // If a range has the minimum lower bound, it must be the first.
    //
    if (range.lower_bound == Self::kMinLowerBound && prev_upper_bound) {
      return false;
    }

    // If this not the first range, its lower bound must be strictly greater than the previous upper
    // bound.
    //
    if (prev_upper_bound && range.lower_bound <= *prev_upper_bound) {
      return false;
    }

    // The range must be non-empty and non-negative.
    //
    if (range.upper_bound <= range.lower_bound) {
      return false;
    }

    prev_upper_bound = range.upper_bound;
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
Interval<OffsetT> BasicPiecewiseFilter<OffsetT, ModelT>::drop_index_range(Interval<OffsetT> to_drop)
  requires PiecewiseFilterMutableStorageModel<ModelT, OffsetT>
{
  if (to_drop.empty()) {
    return to_drop;
  }

  auto [first, last] = std::equal_range(this->live_().begin(),
                                        this->live_().end(),
                                        to_drop,
                                        typename Interval<OffsetT>::LinearOrder{});

  Interval<OffsetT> dropped = to_drop;

  // Extend lower and upper bounds if we need to merge with a previously dropped region to set
  // the return value bounds correctly.
  //
  if (first == last || to_drop.lower_bound < first->lower_bound) {
    if (first != this->live_().begin()) {
      // We are starting in a live interval gap (dropped region), so we extend to the previous live
      // interval's upper bound.
      //
      dropped.lower_bound = std::prev(first)->upper_bound;
    } else {
      dropped.lower_bound = Self::kMinLowerBound;
    }
  }

  if (first == last || to_drop.upper_bound >= std::prev(last)->upper_bound) {
    if (last != this->live_().end()) {
      // We are ending in a live interval gap, so we extend to the next live interval's start.
      //
      dropped.upper_bound = last->lower_bound;
    } else {
      dropped.upper_bound = Self::kMaxUpperBound;
    }
  }

  // Edge case: if dropping a sub-range of an already dropped range, do nothing.
  //
  if (first == last) {
    return dropped;
  }

  // We must handle:
  //
  //         ┌────────────────┐
  // Case 1: │    to_drop     │
  //         └────────────────┘
  //             ┌─────┐
  //             │*iter│
  //             └─────┘
  //
  //            ┌─────────┐
  // Case 2a:   │ to_drop │
  //            └─────────┘
  //         ┌────────────────┐
  //         │     *iter      │
  //         └────────────────┘
  //
  //              ┌───────────┐
  // Case 2b:     │  to_drop  │
  //              └───────────┘
  //         ┌────────────────┐
  //         │     *iter      │
  //         └────────────────┘
  //
  //         ┌─────────┐
  // Case 3: │ to_drop │
  //         └─────────┘
  //               ┌──────────┐
  //               │  *iter   │
  //               └──────────┘
  //
  //                ┌─────────┐
  // Case 4:        │ to_drop │
  //                └─────────┘
  //         ┌──────────┐
  //         │  *iter   │
  //         └──────────┘
  //

  // Process all overlapping intervals with `to_drop`.
  //
  while (first != this->live_().end()) {
    if (first->lower_bound >= to_drop.upper_bound) {
      // Interval is entirely after `to_drop`, so there is nothing left to process.
      //
      break;
    }

    if (first->lower_bound < to_drop.lower_bound) {
      if (first->upper_bound > to_drop.upper_bound) {
        // Case 2a.
        //
        Interval<OffsetT> right_half{to_drop.upper_bound, first->upper_bound};
        first->upper_bound = to_drop.lower_bound;
        this->live_().insert(std::next(first), right_half);
        return dropped;
      } else {
        // Cases 2b and 4.
        //
        first->upper_bound = to_drop.lower_bound;
        ++first;
      }
    } else if (first->upper_bound > to_drop.upper_bound) {
      // Case 3.
      //
      first->lower_bound = to_drop.upper_bound;
      return dropped;
    } else {
      // Case 1.
      //
      first = this->live_().erase(first);
    }
  }

  return dropped;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
Slice<const Interval<OffsetT>> BasicPiecewiseFilter<OffsetT, ModelT>::live() const
{
  return as_const_slice(this->live_());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
bool BasicPiecewiseFilter<OffsetT, ModelT>::live_at_index(OffsetT i) const
{
  return this->live_lower_bound(i) == i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
OffsetT BasicPiecewiseFilter<OffsetT, ModelT>::live_lower_bound(OffsetT i) const
{
  // Compute the live interval which could contain `i`.
  //
  auto iter = std::lower_bound(this->live_().begin(),
                               this->live_().end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  // Check if current interval contains `i`.
  //
  if (iter != this->live_().end() && iter->contains(i)) {
    return i;
  }

  // `i` is in a dropped range, so we return the start of the next live interval.
  //
  if (iter != this->live_().end()) {
    return iter->lower_bound;
  }

  // No live intervals exist after `i`, return kMaxUpperBound.
  //
  return Self::kMaxUpperBound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
Interval<OffsetT> BasicPiecewiseFilter<OffsetT, ModelT>::find_live_range(Interval<OffsetT> i) const
{
  OffsetT start_i = i.lower_bound;
  OffsetT end_i = i.upper_bound;

  BATT_CHECK_LE(start_i, end_i);

  auto iter = std::lower_bound(this->live_().begin(),
                               this->live_().end(),
                               start_i,
                               typename Interval<OffsetT>::LinearOrder{});

  // Check if current interval contains or starts at `start_i`.
  //
  if (iter != this->live_().end()) {
    if (iter->contains(start_i)) {
      OffsetT live_end = std::min(end_i, iter->upper_bound);
      return Interval<OffsetT>{start_i, live_end};
    } else if (iter->lower_bound < end_i) {
      // `start_i` is before this interval, but the interval starts before end_i. Now, the start
      // of the live range is the start of the current interval.
      //
      OffsetT live_start = iter->lower_bound;
      OffsetT live_end = std::min(end_i, iter->upper_bound);
      return Interval<OffsetT>{live_start, live_end};
    }
  }

  // No live range found within [start_i, end_i), return empty interval.
  //
  return Interval<OffsetT>{end_i, end_i};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
void BasicPiecewiseFilter<OffsetT, ModelT>::merge(const Self& other)
  requires PiecewiseFilterMutableStorageModel<ModelT, OffsetT>
{
  // If other has no live intervals, we are done.
  //
  if (other.live_().empty()) {
    return;
  }

  // If this has no live intervals, copy from other.
  //
  if (this->live_().empty()) {
    this->live_().insert(this->live_().end(), other.live_().begin(), other.live_().end());
    BATT_CHECK(this->check_invariants());
    return;
  }

  SmallVec<Interval<OffsetT>, 64> merged_intervals;
  merged_intervals.reserve(this->live_().size() + other.live_().size());

  usize i = 0;
  usize j = 0;

  auto add_interval = [&merged_intervals](const Interval<OffsetT>& interval) {
    if (merged_intervals.empty()) {
      merged_intervals.push_back(interval);
    } else {
      Interval<OffsetT>& last = merged_intervals.back();
      // Check if interval overlaps or is adjacent to last.
      //
      if (interval.lower_bound <= last.upper_bound) {
        // Merge.
        //
        if (interval.upper_bound > last.upper_bound) {
          last = Interval<OffsetT>{last.lower_bound, interval.upper_bound};
        }
      } else {
        // No overlap, add interval.
        //
        merged_intervals.push_back(interval);
      }
    }
  };

  // Merge the live intervals arrays.
  //
  while (i < this->live_().size() && j < other.live_().size()) {
    if (this->live_()[i].lower_bound <= other.live_()[j].lower_bound) {
      add_interval(this->live_()[i]);
      ++i;
    } else {
      add_interval(other.live_()[j]);
      ++j;
    }
  }

  // Add remaining intervals..
  //
  while (i < this->live_().size()) {
    add_interval(this->live_()[i]);
    ++i;
  }

  // Add remaining intervals from other.live_.
  //
  while (j < other.live_().size()) {
    add_interval(other.live_()[j]);
    ++j;
  }

  this->live_() = std::move(merged_intervals);

  BATT_CHECK(this->check_invariants());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
SmallFn<void(std::ostream&)> BasicPiecewiseFilter<OffsetT, ModelT>::dump() const
{
  return [this](std::ostream& out) {
    out << batt::dump_range(this->live_());
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
auto BasicPiecewiseFilter<OffsetT, ModelT>::live_subranges_of(Interval<OffsetT> query_range) const
    -> LiveSubranges
{
  const auto [first, last] = std::equal_range(this->live_().begin(),
                                              this->live_().end(),
                                              query_range,
                                              typename Interval<OffsetT>::LinearOrder{});

  return LiveSubranges{query_range, std::ranges::subrange(first, last)};
}

}  // namespace turtle_kv
