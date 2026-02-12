#pragma once

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
/*static*/ StatusOr<PiecewiseFilter<OffsetT>> PiecewiseFilter<OffsetT>::from_dropped(
    const Slice<const Interval<OffsetT>>& dropped)
{
  PiecewiseFilter<OffsetT> filter;

  filter.dropped_total_ = 0;
  OffsetT current_items_size = 0;
  for (const Interval<OffsetT>& drop_range : dropped) {
    if ((drop_range.lower_bound == 0 && current_items_size != 0) ||
        (drop_range.lower_bound != 0 && drop_range.lower_bound <= current_items_size) ||
        (drop_range.upper_bound <= drop_range.lower_bound)) {
      return Status{::batt::StatusCode::kInvalidArgument};
    }
    current_items_size = drop_range.upper_bound;
    filter.dropped_total_ += drop_range.size();
  }

  filter.dropped_.insert(filter.dropped_.end(), dropped.begin(), dropped.end());

  return filter;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
PiecewiseFilter<OffsetT>::PiecewiseFilter() noexcept : dropped_{}
                                                     , dropped_total_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
void PiecewiseFilter<OffsetT>::drop_index_range(Interval<OffsetT> i)
{
  if (i.empty()) {
    return;
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
    }
  }
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
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});
  if (iter != this->dropped_.end() && iter->contains(i)) {
    return false;
  }
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
OffsetT PiecewiseFilter<OffsetT>::live_lower_bound(OffsetT i) const
{
  // Compute the dropped interval which could contain `i`.
  //
  auto iter = std::lower_bound(this->dropped_.begin(),
                               this->dropped_.end(),
                               i,
                               typename Interval<OffsetT>::LinearOrder{});

  if (iter != this->dropped_.end() && iter->contains(i)) {
    return iter->upper_bound;
  }

  return i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OffsetT>
Interval<OffsetT> PiecewiseFilter<OffsetT>::find_live_range(Interval<OffsetT> i) const
{
  OffsetT start_i = i.lower_bound;
  OffsetT end_i = i.upper_bound;

  BATT_CHECK_LT(start_i, end_i);

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

  BATT_CHECK_LT(start_i, end_i) << BATT_INSPECT(i);

  return Interval<OffsetT>{start_i, end_i};
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
