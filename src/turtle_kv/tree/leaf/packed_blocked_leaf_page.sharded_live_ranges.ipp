//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_PACKED_BLOCKED_LEAF_PAGE_SHARDED_LIVE_RANGES_IPP

#include "packed_blocked_leaf_page.sharded_live_ranges.hpp"

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline /*explicit*/ PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::ShardedLiveRanges(
    const llfs::PackedArray<little_u32>* block_starts,
    BasicPiecewiseFilter<u32, FilterModelT>::LiveSubranges&& filter_live_ranges,
    const Interval<LeafItemIndex>& subrange) noexcept
    : block_starts_{block_starts}
    , block_index_{0}
    , filter_live_ranges_{std::move(filter_live_ranges)}
    , current_range_{0, 0}
    , subrange_{subrange}
{
  this->advance();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline auto PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::peek() -> Optional<Item>
{
  if (this->current_range_.empty()) {
    return None;
  }
  return Item{BATT_CHECKED_CAST(u32, this->block_index_),
              Interval<LeafItemIndex>{LeafItemIndex{this->current_range_.lower_bound},
                                     LeafItemIndex{this->current_range_.upper_bound}},
              (*this->block_starts_)[this->block_index_].value() == this->subrange_.lower_bound.value(),
              (*this->block_starts_)[this->block_index_ + 1].value() == this->subrange_.upper_bound.value()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline auto PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::next() -> Optional<Item>
{
  Optional<Item> item = this->peek();
  if (item) {
    this->advance();
  }
  return item;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline void PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::advance()
{
  this->current_range_.lower_bound = this->current_range_.upper_bound;

  Optional<Interval<u32>> filter_range = this->filter_live_ranges_.peek();
  if (!filter_range) {
    return;
  }

  // Consume the current range from both current and filter.
  //
  BATT_CHECK_LE(this->current_range_.upper_bound, filter_range->upper_bound);
  filter_range->lower_bound = std::max(filter_range->lower_bound,  //
                                       this->current_range_.upper_bound);

  // If the filter range has been consumed, move to the next filter range.
  //
  if (filter_range->empty()) {
    this->filter_live_ranges_.next();
    filter_range = this->filter_live_ranges_.peek();

    // Once we run out of filter live ranges, we are done.
    //
    if (!filter_range) {
      return;
    }
  }

  // std::cerr << ".. " << BATT_INSPECT(filter_range) << std::endl;

  const usize block_count = this->get_block_count();
  BATT_CHECK_LT(this->block_index_, block_count);
  const usize blocks_remaining = block_count - this->block_index_;
  const usize max_probe_steps = BATT_CHECKED_CAST(usize, batt::log2_ceil(blocks_remaining));
  const usize linear_probe_end = this->block_index_ + max_probe_steps;
  bool tried_binary_search = false;

  while (this->block_index_ < block_count) {
    // Test the intersection of the current block's range with the current filter range;
    // if they intersect, then stop here.
    //
    this->current_range_ = this->get_block_range(this->block_index_)  //
                               .intersection_with(*filter_range);

    if (!this->current_range_.empty()) {
      return;
    }

    // If we can, continue the linear probe.
    //
    ++this->block_index_;
    if (this->block_index_ <= linear_probe_end) {
      continue;
    }

    // The binary search fall-back *must* succeed!  If we ever find we are about to try it a
    // second time, panic.
    //
    BATT_CHECK(!tried_binary_search);

    // Fall-back to binary search.
    //
    auto indices = boost::irange<usize>(this->block_index_, block_count);
    auto iter =
        std::lower_bound(indices.begin(),
                         indices.end(),
                         *filter_range,
                         [this](usize i, const Interval<u32>& range) {
                           return Interval<u32>::LinearOrder{}(this->get_block_range(i), range);
                         });

    // If the first block that might intersect with the filter range is beyond the end of the
    // blocks, then we are done.
    //
    if (iter == indices.end()) {
      this->clear_current_range();
      return;
    }

    this->block_index_ = *iter;
    tried_binary_search = true;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline usize PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::get_block_count()
    const noexcept
{
  return this->block_starts_->size() - 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline Interval<u32> PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::get_block_range(
    usize block_i) const noexcept
{
  return Interval<u32>{
      (*this->block_starts_)[block_i],
      (*this->block_starts_)[block_i + 1],
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline void PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::clear_current_range()
{
  this->current_range_.lower_bound = this->current_range_.upper_bound;
}

}  // namespace turtle_kv
