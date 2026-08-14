//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_LEAF_SCAN_BLOCKED_LEAF_HPP

#include "packed_blocked_leaf_page.hpp"
#include "packed_blocked_leaf_page.sharded_live_ranges.hpp"
#include "packed_blocked_leaf_page.sharded_live_ranges.ipp"

#include <turtle_kv/config.hpp>
#include <turtle_kv/core/packed_key_value_slot.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>
#include <turtle_kv/util/piecewise_filter.hpp>

#include <batteries/seq/boxed.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT, typename BlockLoaderT>
auto scan_blocked_leaf(const PackedBlockedLeafPage* packed_leaf,
                       BlockLoaderT* block_loader,
                       const BasicPiecewiseFilter<u32, FilterModelT>& filter,
                       const Interval<KeyView>& key_range) noexcept
{
  BATT_CHECK_NOT_NULLPTR(packed_leaf);

  const Interval<u32> index_range =
      packed_leaf->get_block_aligned_index_range_for_key_range(key_range);

  return packed_leaf->sharded_live_ranges(filter, index_range)  //
         |
         batt::seq::filter_map(
             [packed_leaf, block_loader, key_range](
                 const typename PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>::Item& item)
                 -> Optional<StatusOr<Slice<const PackedKeyValueSlotPtr>>> {
               if (item.live_item_range.empty()) {
                 return None;
               }

               StatusOr<const PackedLeafBlock*> block = block_loader->load_block(item.block_index);
               if (!block.ok()) {
                 return block.status();
               }

               Slice<const PackedKeyValueSlotPtr> slice = std::get<Slice<const PackedKeyValueSlotPtr>>(
                   packed_leaf->get_slice_within_block(item.block_index,
                                                       *block,
                                                       item.live_item_range));

               if (item.is_first || item.is_last) {
                 const auto* begin = slice.begin();
                 const auto* end = slice.end();

                 if (item.is_first) {
                   begin =
                       std::lower_bound(begin,
                                        end,
                                        key_range.lower_bound,
                                        [](const PackedKeyValueSlotPtr& slot, const KeyView& key) {
                                          return get_key(slot) < key;
                                        });
                 }
                 if (item.is_last) {
                   end =
                       std::lower_bound(begin,
                                        end,
                                        key_range.upper_bound,
                                        [](const PackedKeyValueSlotPtr& slot, const KeyView& key) {
                                          return get_key(slot) < key;
                                        });
                 }

                 if (begin == end) {
                   return None;
                 }
                 slice = as_slice(begin, end);
               }

               return slice;
             })  //
      ;
}

}  // namespace turtle_kv
