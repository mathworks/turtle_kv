//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_BLOCKED_LEAF_PAGE_IPP

#include "packed_blocked_leaf_page.hpp"
#include "packed_blocked_leaf_page.item_iterator.hpp"

#include <turtle_kv/import/small_vec.hpp>

#include <artc/packed/art_builder.hpp>
#include <artc/packed/query.hpp>

#include <batteries/stable_string_store.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemRangeT>
StatusOr<PackedBlockedLeafPage*> pack_blocked_leaf_page(const usize block_size,
                                                        const ItemRangeT& src_items,
                                                        const MutableBuffer& dst_buffer) noexcept
{
  const usize item_count = std::size(src_items);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Calculate the number of blocks needed and how many items in each one.
  //
  SmallVec<PackedLeafBlockStats, 256> block_stats;
  {
    auto src_iter = std::begin(src_items);
    const auto src_end = std::end(src_items);
    usize blocks_size_remaining = dst_buffer.size() - block_size;
    for (;;) {
      if (src_iter == src_end) {
        break;
      }
      BATT_CHECK_LT(src_iter, src_end);

      if (blocks_size_remaining < block_size) {
        return {batt::StatusCode::kResourceExhausted};
      }

      auto& stats = block_stats.emplace_back(
          PackedLeafBlockStats::from(std::ranges::subrange(src_iter, src_end), block_size));

      blocks_size_remaining -= block_size;
      src_iter = std::next(src_iter, stats.item_count);
    }
  }
  const usize block_count = block_stats.size();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the leaf header.
  //
  MutableBuffer dst_remaining = dst_buffer;
  dst_remaining += sizeof(llfs::PackedPageHeader);

  auto* leaf_header = static_cast<PackedBlockedLeafPage*>(dst_remaining.data());
  dst_remaining += sizeof(PackedBlockedLeafPage);
  {
    leaf_header->magic = PackedBlockedLeafPage::kMagic;
    leaf_header->total_packed_size = 0;
    leaf_header->blocks_per_art_key = 0;
    leaf_header->block_size_bytes = BATT_CHECKED_CAST(u32, block_size);
    leaf_header->block0.offset = 0;
    leaf_header->block_starting_item.offset = 0;
    leaf_header->art_block_index.offset = 0;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack `block_starting_item` array.
  //
  {
    const usize block_starting_item_array_size =
        sizeof(llfs::PackedArray<little_u32>) + sizeof(little_u32) * (block_count + 1);

    auto* block_starting_item = static_cast<llfs::PackedArray<little_u32>*>(dst_remaining.data());
    dst_remaining += block_starting_item_array_size;

    block_starting_item->initialize(block_count + 1);

    little_u32* block_start = block_starting_item->data();
    u32 item_i = 0;
    for (const PackedLeafBlockStats& stats : block_stats) {
      *block_start = item_i;
      item_i += stats.item_count;
      ++block_start;
    }
    *block_start = item_count;

    leaf_header->block_starting_item.reset_unsafe(block_starting_item);
  }
  const llfs::PackedArray<little_u32>& block_starting_item = *(leaf_header->block_starting_item);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Calculate blocks_per_art_key based on available space.
  //
  const usize space_for_art = dst_remaining.size() - block_size * block_count;
  SmallVec<KeyView, 4096> art_keys;
  usize blocks_per_art_key = 1;
  //----- --- -- -  -  -   -
  const auto items = std::begin(src_items);
  const auto key_at = [&items](usize i) {
    return get_key(*(items + i));
  };
  //----- --- -- -  -  -   -
  for (;;) {
    art_keys.clear();
    for (usize block_i = blocks_per_art_key; block_i < block_count; block_i += blocks_per_art_key) {
      const usize item_i = block_starting_item[block_i];
      BATT_CHECK_LT(item_i, item_count);
      BATT_CHECK_GT(item_i, 0);

      KeyView k0 = key_at(item_i - 1);
      KeyView k1 = key_at(item_i);
      KeyView common_prefix = llfs::find_common_prefix(0, k0, k1);
      KeyView min_k1{k1.data(), common_prefix.size() + 1};

      art_keys.emplace_back(min_k1);
    }

    using artc::packed::PackedARTBuilder;

    batt::StableStringStore string_store;

    BATT_DEBUG_INFO(BATT_INSPECT_RANGE(art_keys)
                    << BATT_INSPECT(block_count) << BATT_INSPECT(blocks_per_art_key));

    BATT_ASSIGN_OK_RESULT(auto art_builder,
                          PackedARTBuilder::from_items(art_keys.begin(),
                                                       art_keys.end(),
                                                       BATT_OVERLOADS_OF(get_key),
                                                       string_store));

    if (art_builder.get_packed_size() > space_for_art) {
      ++blocks_per_art_key;
      continue;
    }

    MutableBuffer art_buffer{dst_remaining.data(), art_builder.get_packed_size()};
    dst_remaining += art_buffer.size();
    BATT_CHECK_GE(dst_remaining.size(), block_size * block_count);

    BATT_ASSIGN_OK_RESULT(const artc::packed::NodeBase* art_root, art_builder.build(art_buffer));

    leaf_header->art_block_index.reset_unsafe(art_root);
    leaf_header->blocks_per_art_key = BATT_CHECKED_CAST(u32, blocks_per_art_key);
    break;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Shift the remaining buffer forward so it aligns with the nearest block boundary.
  //
  const usize offset_for_block_align = dst_remaining.size() & (block_size - 1);
  dst_remaining += offset_for_block_align;
  BATT_CHECK_LE(block_size * block_count, dst_remaining.size());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack the blocks.
  //
  leaf_header->block0.reset_unsafe(static_cast<PackedLeafBlock*>(dst_remaining.data()));
  {
    auto src_iter = std::begin(src_items);
    const auto src_end = std::end(src_items);
    usize block_i = 0;
    for (const PackedLeafBlockStats& stats : block_stats) {
      BATT_DEBUG_INFO(BATT_INSPECT(block_i) << BATT_INSPECT(stats));

      BATT_CHECK_NE(src_iter, src_end);
      auto src_block_items = std::ranges::subrange(src_iter, std::next(src_iter, stats.item_count));

      BATT_CHECK_GE(dst_remaining.size(), block_size);
      MutableBuffer dst_block_buffer{dst_remaining.data(), block_size};

      auto block_end_iter =
          BATT_OK_RESULT_OR_PANIC(pack_leaf_block(src_block_items, dst_block_buffer, stats));

      BATT_CHECK_EQ(block_end_iter, std::end(src_block_items));

      src_iter = block_end_iter;
      dst_remaining += block_size;
      ++block_i;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Fill in remaining header fields.
  //
  leaf_header->total_packed_size = BATT_CHECKED_CAST(u32, dst_buffer.size() - dst_remaining.size());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Success!  (nothing succeeds like it)
  //
  return leaf_header;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ const PackedBlockedLeafPage& PackedBlockedLeafPage::view_of(
    const ConstBuffer& buffer) noexcept
{
  BATT_CHECK_GT(buffer.size(), sizeof(PackedBlockedLeafPage) + sizeof(llfs::PackedPageHeader));

  auto* packed = static_cast<const PackedBlockedLeafPage*>(
      advance_pointer(buffer.data(), sizeof(llfs::PackedPageHeader)));

  BATT_CHECK_EQ(packed->magic, PackedBlockedLeafPage::kMagic);

  return *packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PackedBlockedLeafPage::ItemIterator PackedBlockedLeafPage::items_begin() const noexcept
{
  auto first_block = this->blocks_begin();
  return ItemIterator{first_block, first_block->items_begin()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PackedBlockedLeafPage::ItemIterator PackedBlockedLeafPage::items_end() const noexcept
{
  auto last_block = this->blocks_end();
  return ItemIterator{last_block, last_block->items_begin()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PackedBlockedLeafPage::ItemIterator PackedBlockedLeafPage::item_at(usize i) const noexcept
{
  const llfs::PackedArray<little_u32>& starts = *this->block_starting_item;

  BATT_CHECK_NE(starts.size(), 0);
  BATT_CHECK_EQ(starts.front(), 0);

  const auto iter = std::prev(std::upper_bound(starts.begin(), starts.end(), i));
  const isize item_pos_in_block = i - *iter;
  const isize block_i = std::distance(starts.begin(), iter);
  auto block_iter = this->blocks_begin() + block_i;

  return ItemIterator{block_iter, block_iter->items_begin() + item_pos_in_block};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize PackedBlockedLeafPage::find_block_index_containing_key(
    const KeyView& key) const noexcept
{
  using artc::packed::find_lower_bound_rank;
  using artc::packed::LowerBoundRank;

  LowerBoundRank result = find_lower_bound_rank(this->art_block_index.get(), key);

  const usize part_i = result.exact ? (result.rank + 1) : result.rank;
  const usize block_i = part_i * this->blocks_per_art_key;

  BATT_CHECK_LT(block_i, this->block_count());

  return block_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PackedLeafBlock::Iterator PackedBlockedLeafPage::find_block_containing_key(
    const KeyView& key) const noexcept
{
  return this->blocks_begin() + this->find_block_index_containing_key(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedKeyValueSlotPtr* PackedBlockedLeafPage::find_key(
    const KeyView& key) const noexcept
{
  return this->find_block_containing_key(key)->find_key(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline PackedBlockedLeafPage::ItemIterator PackedBlockedLeafPage::lower_bound(
    const KeyView& key) const noexcept
{
  auto block_iter = this->find_block_containing_key(key);

  const PackedKeyValueSlotPtr* p_slot = block_iter->lower_bound(key);
  if (p_slot == block_iter->items_end()) {
    ++block_iter;
    return ItemIterator{block_iter, block_iter->items_begin()};
  }

  return ItemIterator{block_iter, p_slot};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
inline PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>
PackedBlockedLeafPage::sharded_live_ranges(const BasicPiecewiseFilter<u32, FilterModelT>& filter,
                                           const Interval<u32>& subrange) const noexcept
{
  return ShardedLiveRanges<FilterModelT>{
      this->block_starting_item.get(),
      filter.live_subranges_of(subrange),
  };
}

}  // namespace turtle_kv
