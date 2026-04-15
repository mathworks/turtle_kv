//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PIECEWISE_FILTER_IPP

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
/*static*/ StatusOr<PiecewiseFilter<OffsetT>> PiecewiseFilter<OffsetT>::from_dropped(
    const Slice<const Interval<OffsetT>>& dropped, Optional<Interval<OffsetT>> scope)
{
  PiecewiseFilter<OffsetT> filter;

  filter.scope_ = scope;

  filter.dropped_.insert(filter.dropped_.end(), dropped.begin(), dropped.end());
  filter.dropped_total_ = std::accumulate(filter.dropped_.begin(),
                                          filter.dropped_.end(),
                                          0,
                                          [](OffsetT total, const Interval<OffsetT>& i) {
                                            return total += i.size();
                                          });

  if (!filter.check_invariants()) {
    return Status{::batt::StatusCode::kInvalidArgument};
  }

  return filter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
PiecewiseFilter<OffsetT>::PiecewiseFilter() noexcept : dropped_{}
                                                     , scope_{None}
                                                     , dropped_total_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
bool PiecewiseFilter<OffsetT>::check_invariants() const
{
  OffsetT current_items_size = 0;
  for (const Interval<OffsetT>& drop_range : this->dropped_) {
    if ((drop_range.lower_bound == 0 && current_items_size != 0) ||
        (drop_range.lower_bound != 0 && drop_range.lower_bound <= current_items_size) ||
        (drop_range.upper_bound <= drop_range.lower_bound)) {
      return false;
    }
    
    if (this->scope_ && !this->scope_->intersection_with(drop_range).empty()) {
      return false;
    }

    current_items_size = drop_range.upper_bound;
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Interval<OffsetT> PiecewiseFilter<OffsetT>::drop_index_range(Interval<OffsetT> i)
{
  if (i.empty()) {
    return i;
  }

  if (this->scope_) {
    i = i.intersection_with(*this->scope_);
    if (i.empty()) {
      return i;
    }
  }

  // Find the position to insert `i` or begin merging with other intervals.
  //
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter == this->dropped_.end() || !i.adjacent_to(*iter)) {
    // No adjacent range to merge with was found, insert into the back of the dropped ranges.
    //
    iter = this->dropped_.insert(iter, i);
    this->dropped_total_ += i.size();
  } else {
    // Merge with lower bound adjacent range.
    //
    this->dropped_total_ -= iter->size();
    *iter = iter->union_with(i);
    this->dropped_total_ += iter->size();

    // Merge with all subsequent adjacent ranges.
    //
    for (auto after = std::next(iter);
         after != this->dropped_.end() && iter->adjacent_to(*after);) {
      this->dropped_total_ -= after->size() + iter->size();
      *after = after->union_with(*iter);
      this->dropped_total_ += after->size();
      iter = this->dropped_.erase(iter);
    }
  }

  // If necessary, merge with the previous range.
  //
  if (iter != this->dropped_.begin()) {
    auto before = std::prev(iter);
    if (iter->adjacent_to(*before)) {
      this->dropped_total_ -= before->size() + iter->size();
      *before = before->union_with(*iter);
      this->dropped_total_ += before->size();
      this->dropped_.erase(iter);
      iter = before;
    }
  }

  BATT_CHECK_NE(iter, this->dropped_.end());

  return *iter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
OffsetT PiecewiseFilter<OffsetT>::dropped_total() const
{
  return this->dropped_total_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Slice<const Interval<OffsetT>> PiecewiseFilter<OffsetT>::dropped() const
{
  return as_const_slice(this->dropped_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
bool PiecewiseFilter<OffsetT>::live_at_index(OffsetT i) const
{
  if (this->scope_ && !this->scope_->contains(i)) {
    return false;
  }

  StatusOr<OffsetT> live_lower_bound = this->live_lower_bound(i);
  if (live_lower_bound.status() == batt::StatusCode::kOutOfRange) {
    return false;
  }
  BATT_CHECK(live_lower_bound.ok());

  return *live_lower_bound == i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
StatusOr<OffsetT> PiecewiseFilter<OffsetT>::live_lower_bound(OffsetT i) const
{
  if (this->scope_ && !this->scope_->contains(i)) {
    return {batt::StatusCode::kOutOfRange};
  }

  // Compute the dropped interval which could contain `i`.
  //
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter != this->dropped_.end() && iter->contains(i)) {
    OffsetT next_live = iter->upper_bound;
    if (this->scope_ && next_live >= this->scope_->upper_bound) {
      return {batt::StatusCode::kOutOfRange};
    }

    return next_live;
  }

  return i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
StatusOr<Interval<OffsetT>> PiecewiseFilter<OffsetT>::find_live_range(Interval<OffsetT> i) const
{
  if (this->scope_) {
    i = i.intersection_with(*this->scope_);
    if (i.empty()) {
      return {batt::StatusCode::kOutOfRange};
    }
  }

  OffsetT start_i = i.lower_bound;
  OffsetT end_i = i.upper_bound;

  BATT_CHECK_LE(start_i, end_i);

  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               start_i,
                               typename Interval<OffsetT>::LinearOrder{});

  // Start by finding the live lower bound of start_i. If start_i is filtered, adjust it to be the
  // live lower bound.
  //
  if (iter != this->dropped_.end() && iter->contains(start_i)) {
    start_i = iter->upper_bound;
    if (start_i >= end_i) {
      // If this adjustment causes start_i to exceed end_i, no live range exists to return, so
      // return an empty interval.
      //
      return Interval<OffsetT>{end_i, end_i};
    }

    ++iter;
  }

  // If another dropped range exists after the range pointed to by iter, use its lower bound to
  // adjust the value of end_i so that we don't cross over into another filtered range.
  //
  if (iter != this->dropped_.end()) {
    end_i = std::min(end_i, iter->lower_bound);
  }

  BATT_CHECK_LE(start_i, end_i) << BATT_INSPECT(i);

  return Interval<OffsetT>{start_i, end_i};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
void PiecewiseFilter<OffsetT>::narrow_scope(const Interval<OffsetT>& new_scope)
{
  if (!this->scope_) {
    this->scope_ = new_scope;
  } else {
    this->scope_ = this->scope_->intersection_with(new_scope);
    if (this->scope_->empty()) {
      this->scope_ = Interval<OffsetT>{0,0};
    }
  }

  usize i = 0;
  OffsetT new_dropped_total = 0;

  for (usize j = 0; j < this->dropped_.size(); ++j) {
    // Truncate the interval to fit within the new scope.
    //
    Interval<OffsetT> truncated = this->dropped_[j].intersection_with(*this->scope_);
    
    // Only keep non-empty intervals.
    //
    if (!truncated.empty()) {
      this->dropped_[i] = truncated;
      new_dropped_total += truncated.size();
      ++i;
    }
  }

  // Remove intervals that were filtered out.
  //
  this->dropped_.resize(i);
  this->dropped_total_ = new_dropped_total;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Optional<Interval<OffsetT>> PiecewiseFilter<OffsetT>::get_scope() const
{
  return this->scope_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
void PiecewiseFilter<OffsetT>::merge(const PiecewiseFilter& other)
{
if (this->scope_ && other.scope_) {
    bool this_empty = this->scope_->empty();
    bool other_empty = other.scope_->empty();
    
    if (!this_empty && !other_empty) {
      // If both are non-empty, take the union.
      //
      this->scope_ = this->scope_->union_with(*other.scope_);
    } else if (this_empty && !other_empty) {
      this->scope_ = other.scope_;
    }
  } else if (!this->scope_ && other.scope_) {
    this->scope_ = other.scope_;
  }

  // If other has no dropped intervals, we are done.
  //
  if (other.dropped_.empty()) {
    BATT_CHECK(this->check_invariants());
    return;
  }

  // If this has no dropped intervals, copy from other.
  //
  if (this->dropped_.empty()) {
    this->dropped_.insert(this->dropped_.end(), other.dropped_.begin(), other.dropped_.end());
    this->dropped_total_ = other.dropped_total_;
    BATT_CHECK(this->check_invariants());
    return;
  }

  SmallVec<Interval<OffsetT>, 64> merged_intervals;
  merged_intervals.reserve(this->dropped_.size() + other.dropped_.size());

  OffsetT new_dropped_total = 0;
  
  usize i = 0;
  usize j = 0;
  
  auto add_interval = [&merged_intervals, &new_dropped_total](const Interval<OffsetT>& interval) {
    if (merged_intervals.empty()) {
      merged_intervals.push_back(interval);
      new_dropped_total += interval.size();
    } else {
      Interval<OffsetT>& last = merged_intervals.back();
      // Check if interval overlaps or is adjacent to last.
      //
      if (interval.lower_bound <= last.upper_bound) {
        // Merge.
        //
        if (interval.upper_bound > last.upper_bound) {
          new_dropped_total -= last.size();
          last = Interval<OffsetT>{last.lower_bound, interval.upper_bound};
          new_dropped_total += last.size();
        }
      } else {
        // No overlap, add interval.
        //
        merged_intervals.push_back(interval);
        new_dropped_total += interval.size();
      }
    }
  };
  
  // Merge the dropped intervals arrays.
  //
  while (i < this->dropped_.size() && j < other.dropped_.size()) {
    if (this->dropped_[i].lower_bound <= other.dropped_[j].lower_bound) {
      add_interval(this->dropped_[i]);
      ++i;
    } else {
      add_interval(other.dropped_[j]);
      ++j;
    }
  }
  
  // Add remaining intervals from this->dropped_.
  //
  while (i < this->dropped_.size()) {
    add_interval(this->dropped_[i]);
    ++i;
  }
  
  // Add remaining intervals from other.dropped_.
  //
  while (j < other.dropped_.size()) {
    add_interval(other.dropped_[j]);
    ++j;
  }
  
  this->dropped_ = std::move(merged_intervals);
  this->dropped_total_ = new_dropped_total;

  BATT_CHECK(this->check_invariants());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
SmallFn<void(std::ostream&)> PiecewiseFilter<OffsetT>::dump() const
{
  return [this](std::ostream& out) {
    out << batt::dump_range(this->dropped_);
  };
}
}  // namespace turtle_kv
