//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_BLOCKED_LEAF_PAGE_HPP

#include "packed_leaf_block.hpp"
#include "packed_leaf_block.iterator.hpp"

#include <turtle_kv/core/packed_key_value_slot.hpp>
#include <turtle_kv/core/packed_key_value_slot_slice.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/util/page_buffers.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_cache.hpp>

#include <artc/packed/node_base.hpp>
#include <artc/packed/query.hpp>

#include <batteries/bit_ops/bit_count.hpp>
#include <batteries/seq/flatten.hpp>
#include <batteries/seq/map.hpp>
#include <batteries/strong_typedef.hpp>

#include <boost/range/irange.hpp>

#include <ranges>

namespace turtle_kv {

BATT_STRONG_TYPEDEF(u32, LeafItemIndex);

// Forward-declaration.
//
struct PackedBlockedLeafPage;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedLeafResult {
  PackedBlockedLeafPage* leaf;
  usize items_packed;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Packs a blocked leaf page with the passed block size, containing the passed key/value
 * pairs, into the passed buffer.  Packs as many items as will fit; returns the number packed.
 */
template <typename ItemRangeT>
StatusOr<PackedLeafResult> pack_blocked_leaf_page(const usize block_size,
                                                  const ItemRangeT& src_items,
                                                  const MutableBuffer& dst_buffer) noexcept;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Header for a packed leaf page with blocked structure.
 */
struct PackedBlockedLeafPage  //
{
  /** \brief Must be the first 8 bytes of the header.  \see PackedBlockedLeagPage::magic
   */
  static constexpr u64 kMagic = 0x6456beb7f9558445ull;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using BlockIterator = PackedLeafBlock::Iterator;
  class ItemIterator;

  using BlockItemsSeq = PackedLeafBlock::BlockItemsSeq;

  struct ItemsSeqFromBlock {
    BlockItemsSeq operator()(const PackedLeafBlock& block) const
    {
      return block.items_seq();
    }
  };

  using BlocksSeq = batt::SubRangeSeq<boost::iterator_range<BlockIterator>>;
  using ItemsSeq = batt::seq::Flatten<batt::seq::Map<BlocksSeq, ItemsSeqFromBlock>>;

  struct SlotSliceFromBlock {
    PackedKeyValueSlotSlice operator()(const PackedLeafBlock& block) const
    {
      return {block.items_slice()};
    }
  };

  using SlotSliceSeq = batt::seq::Map<BlocksSeq, SlotSliceFromBlock>;

  template <PiecewiseFilterStorageModel<u32> FilterModelT>
  class ShardedLiveRanges;

  class HeaderShardView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename EditT>
  static usize packed_edit_size(const EditT& edit) noexcept
  {
    return PackedLeafBlock::packed_edit_size(edit);
  }

  static usize estimate_capacity(usize leaf_size,
                                 usize block_size,
                                 usize max_key_size,
                                 usize max_edit_size) noexcept;

  /** \brief Returns the passed object's payload region, validated as a PackedBlockedLeafPage.
   */
  template <typename T>
  static const PackedBlockedLeafPage* view_of(T&& t) noexcept
  {
    const ConstBuffer buffer = get_page_const_payload(BATT_FORWARD(t));
    BATT_CHECK_GE(buffer.size(), sizeof(PackedBlockedLeafPage));

    auto* packed = static_cast<const PackedBlockedLeafPage*>(buffer.data());
    BATT_CHECK_EQ(packed->magic, kMagic);

    return packed;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u64 magic;                                // +8 -> 8
  little_u32 total_packed_size;                 // +4 -> 12
  little_u32 blocks_per_art_key;                // +4 -> 16
  little_u32 block_size_bytes;                  // +4 -> 20
  llfs::PackedPointer<PackedLeafBlock> block0;  // +4 -> 24

  /** \brief Pointer to array that stores, for each block, the starting item index relative to the
   * entire leaf.
   */
  llfs::PackedPointer<llfs::PackedArray<little_u32>> block_starting_item;  // +4 -> 28

  /** \brief Pointer to packed ART index.
   */
  llfs::PackedPointer<const artc::packed::NodeBase> art_block_index;  // +4 -> 32

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageId page_id() const noexcept
  {
    return (reinterpret_cast<const llfs::PackedPageHeader*>(this) - 1)->page_id.unpack();
  }

  Optional<llfs::PageId> page_shard_id_for_block(llfs::PageCache& page_cache,
                                                 usize i,
                                                 llfs::PageId leaf_page_id) const noexcept
  {
    const usize block_begin_offset = this->block_page_offset(i);
    const usize block_end_offset = block_begin_offset + this->block_size_bytes;

    return page_cache.page_shard_id_for(leaf_page_id,
                                        Interval<usize>{block_begin_offset, block_end_offset});
  }

  Optional<llfs::PageId> page_shard_id_for_block(llfs::PageCache& page_cache,
                                                 usize i) const noexcept
  {
    return this->page_shard_id_for_block(page_cache, i, this->page_id());
  }

  usize min_header_shard_size() const noexcept
  {
    return this->block_page_offset(0);
  }

  //----- --- -- -  -  -   -

  usize block_page_offset(usize i) const noexcept
  {
    return sizeof(llfs::PackedPageHeader) + offsetof(PackedBlockedLeafPage, block0) +
           this->block0.offset + i * this->block_size_bytes;
  }

  usize block_count() const noexcept
  {
    return this->block_starting_item->size() - 1;
  }

  BlockIterator blocks_begin() const noexcept
  {
    return BlockIterator{this->block0.get(), (isize)this->block_size_bytes.value()};
  }

  const PackedLeafBlock& blocks_front() const
  {
    return *this->blocks_begin();
  }

  BlockIterator blocks_end() const noexcept
  {
    return this->blocks_begin() + this->block_count();
  }

  const PackedLeafBlock& blocks_back() const
  {
    return *(this->blocks_begin() + (this->block_count() - 1));
  }

  auto blocks() const noexcept
  {
    return std::ranges::subrange<BlockIterator>(this->blocks_begin(), this->blocks_end());
  }

  const PackedLeafBlock& block_at(usize block_i) const noexcept
  {
    return *(this->blocks_begin() + block_i);
  }

  BlocksSeq blocks_seq() const noexcept
  {
    return batt::as_seq(this->blocks());
  }

  Interval<u32> item_index_range_of_block(usize i) const noexcept
  {
    return Interval<u32>{
        (*this->block_starting_item)[i].value(),
        (*this->block_starting_item)[i + 1].value(),
    };
  }

  /** \brief Returns the index of the block that would contain the given key, if it is present in
   * this page.
   *
   * Always returns a valid block index (i.e., less-than this->block_count())
   */
  usize find_block_index_containing_key(const KeyView& key) const noexcept;

  /** \brief Returns a block iterator to the block that would contain the given key, if it is
   * present in this page.
   *
   * Always returns a valid block iterator.
   */
  BlockIterator find_block_containing_key(const KeyView& key) const noexcept;

  //----- --- -- -  -  -   -

  /** \brief Returns the number of key/value pairs in this page.
   */
  usize item_count() const noexcept
  {
    return this->block_starting_item->back();
  }

  /** \brief Returns a sequence of all items in the page, in key order.
   */
  ItemsSeq items_seq() const noexcept
  {
    return this->blocks_seq() | batt::seq::map(ItemsSeqFromBlock{}) | batt::seq::flatten();
  }

  /** \brief Returns an item iterator to the first item in the page.
   */
  ItemIterator items_begin() const noexcept;

  /** \brief Returns an item iterator one-past the last item in the page.
   */
  ItemIterator items_end() const noexcept;

  /** \brief Returns an item iterator to the i-th item in the page.
   */
  ItemIterator item_at(usize i) const noexcept;

  /** \brief Returns an iterator to the given key in this page if found or nullptr if not found.
   */
  const PackedKeyValueSlotPtr* find_key(const KeyView& key) const noexcept;

  /** \brief Returns an iterator to the first item in this page whose key is not less than `key`;
   * if all keys in the page are less than `key`, returns `this->items_end()`.
   */
  ItemIterator lower_bound(const KeyView& key) const noexcept;

  //----- --- -- -  -  -   -

  KeyView min_key() const noexcept
  {
    return this->blocks_front().min_key();
  }

  KeyView max_key() const noexcept
  {
    return this->blocks_back().max_key();
  }

  SlotSliceSeq slot_slice_seq() const noexcept
  {
    return this->blocks_seq() | batt::seq::map(SlotSliceFromBlock{});
  }

  template <PiecewiseFilterStorageModel<u32> FilterModelT>
  ShardedLiveRanges<FilterModelT> sharded_live_ranges(
      const BasicPiecewiseFilter<u32, FilterModelT>& filter,
      const Interval<LeafItemIndex>& subrange) const noexcept;

  Interval<LeafItemIndex> get_block_aligned_index_range_for_key_range(
      KeyView lower_bound, Optional<KeyView> upper_bound = None) const noexcept;

  Slice<const PackedKeyValueSlotPtr> get_slice_within_block(
      u32 block_index,
      const PackedLeafBlock* block,
      const Interval<LeafItemIndex>& live_item_range) const noexcept;
};

static_assert(sizeof(PackedBlockedLeafPage) == 32);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A view of the header prefix of a PackedBlockedLeafPage.
 */
class PackedBlockedLeafPage::HeaderShardView
{
 public:
  using Self = HeaderShardView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self view_of(const ConstBuffer& buffer) noexcept
  {
    auto* leaf = static_cast<const PackedBlockedLeafPage*>(
        advance_pointer(buffer.data(), sizeof(llfs::PackedPageHeader)));
    BATT_CHECK_EQ(leaf->magic, PackedBlockedLeafPage::kMagic);
    BATT_CHECK_GE(buffer.size(), leaf->min_header_shard_size());

    return Self{*leaf, buffer.size()};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

#if 0
  Seq<PackedKeyValueSlotSlice> load_slices(llfs::PageLoader& loader,
                                           Optional<KeyView> first_key,
                                           Optional<KeyView> last_key,
                                           Optional<u32> first_index,
                                           Optional<u32> last_index,
                                           const PiecewiseFilter<u32>& filter);
#endif

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  explicit HeaderShardView(const PackedBlockedLeafPage& leaf, usize header_shard_size) noexcept
      : leaf_{&leaf}
      , header_shard_size_{header_shard_size}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedBlockedLeafPage* leaf_;
  usize header_shard_size_;
};

}  // namespace turtle_kv