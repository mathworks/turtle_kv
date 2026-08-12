//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_PACKED_BLOCKED_LEAF_PAGE_SHARDED_LIVE_RANGES_HPP

#include "packed_blocked_leaf_page.hpp"

#include <turtle_kv/util/piecewise_filter.live_subranges.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
class PackedBlockedLeafPage::ShardedLiveRanges
{
 public:
  struct Item {
    u32 block_index;
    Interval<u32> live_item_range;
    bool is_first;
    bool is_last;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ShardedLiveRanges(
      const llfs::PackedArray<little_u32>* block_starts,
      BasicPiecewiseFilter<u32, FilterModelT>::LiveSubranges&& filter_live_ranges,
      const Interval<u32>& subrange) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<Item> peek();

  Optional<Item> next();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void advance();

  usize get_block_count() const noexcept;

  Interval<u32> get_block_range(usize block_i) const noexcept;

  void clear_current_range();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::PackedArray<little_u32>* block_starts_;
  usize block_index_;
  BasicPiecewiseFilter<u32, FilterModelT>::LiveSubranges filter_live_ranges_;
  Interval<u32> current_range_;
  Interval<u32> subrange_;
};

}  // namespace turtle_kv
