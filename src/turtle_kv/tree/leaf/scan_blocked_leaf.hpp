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
#include "packed_leaf_block.hpp"

#include <turtle_kv/config.hpp>
#include <turtle_kv/core/packed_key_value_slot_slice.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>
#include <turtle_kv/util/piecewise_filter.hpp>
#include <turtle_kv/util/piecewise_filter.ipp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_size.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/sharded_page_view.hpp>

#include <batteries/seq/boxed.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Returns a boxed sequence of PackedKeyValueSlotSlice for a blocked leaf page.
 *
 * If `filter` is nullptr, all items are considered live (a default-constructed PiecewiseFilter is
 * used internally). If `min_key` is provided, scanning begins at the first item >= min_key.
 *
 * On I/O error, the sequence terminates and `status` is set to the error status.
 */
template <PiecewiseFilterStorageModel<u32> FilterModelT = SmallVec<Interval<u32>, 64>>
BoxedSeq<PackedKeyValueSlotSlice> scan_blocked_leaf(
    llfs::PageId page_id,
    usize block_size,
    const BasicPiecewiseFilter<u32, FilterModelT>* filter,
    Optional<KeyView> min_key,
    llfs::PageLoader& page_loader,
    PageSliceStorage& slice_storage,
    llfs::PinPageToJob pin_page_to_job,
    Status& status) noexcept;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
class BlockedLeafScanSeq
{
 public:
  using Item = PackedKeyValueSlotSlice;
  using Filter = BasicPiecewiseFilter<u32, FilterModelT>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BlockedLeafScanSeq(llfs::PageId page_id,
                              usize block_size,
                              const Filter* filter,
                              Optional<KeyView> min_key,
                              llfs::PageLoader& page_loader,
                              PageSliceStorage& slice_storage,
                              llfs::PinPageToJob pin_page_to_job,
                              Status& status) noexcept
      : page_id_{page_id}
      , block_size_{block_size}
      , min_key_{min_key}
      , page_loader_{page_loader}
      , slice_storage_{slice_storage}
      , pin_page_to_job_{pin_page_to_job}
      , status_{status}
      , filter_{filter}
  {
    this->initialize();
  }

  Optional<Item> peek()
  {
    if (this->done_) {
      return None;
    }

    if (!this->pending_slice_) {
      this->advance();
    }

    return this->pending_slice_;
  }

  Optional<Item> next()
  {
    Optional<Item> item = this->peek();
    if (item) {
      this->pending_slice_ = None;
    }
    return item;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  StatusOr<ConstBuffer> load_shard(const Interval<usize>& shard_interval,
                                   llfs::LruPriority lru_priority) noexcept
  {
    llfs::PageCache& page_cache = *this->page_loader_.page_cache();

    Optional<llfs::PageId> shard_page_id =
        page_cache.page_shard_id_for(this->page_id_, shard_interval);

    if (!shard_page_id) {
      return {batt::StatusCode::kUnavailable};
    }

    const llfs::PinnedPage* existing = this->slice_storage_.find_pinned_page(*shard_page_id);
    if (existing) {
      return ConstBuffer{existing->raw_data(), shard_interval.size()};
    }

    BATT_ASSIGN_OK_RESULT(
        llfs::PinnedPage pinned_shard,
        this->page_loader_.load_page(
            *shard_page_id,
            llfs::PageLoadOptions{llfs::ShardedPageView::page_layout_id(),
                                  this->pin_page_to_job_,
                                  llfs::OkIfNotFound{false},
                                  lru_priority}));

    const void* raw_data = pinned_shard.raw_data();
    this->slice_storage_.insert_pinned_page(std::move(pinned_shard));

    return ConstBuffer{raw_data, shard_interval.size()};
  }

  void initialize() noexcept
  {
    StatusOr<ConstBuffer> header_buffer =
        this->load_shard(Interval<usize>{0, this->block_size_},
                         llfs::LruPriority{kTrieIndexLruPriority});

    if (!header_buffer.ok()) {
      this->status_ = header_buffer.status();
      this->done_ = true;
      return;
    }

    this->leaf_ = &PackedBlockedLeafPage::view_of(*header_buffer);

    const usize actual_header_size = this->leaf_->min_header_shard_size();
    if (actual_header_size > this->block_size_) {
      StatusOr<ConstBuffer> second_shard =
          this->load_shard(Interval<usize>{this->block_size_, 2 * this->block_size_},
                           llfs::LruPriority{kTrieIndexLruPriority});

      if (!second_shard.ok()) {
        this->status_ = second_shard.status();
        this->done_ = true;
        return;
      }
    }

    const u32 item_count = BATT_CHECKED_CAST(u32, this->leaf_->item_count());
    u32 first_item = 0;

    if (this->min_key_) {
      const usize start_block_i = this->leaf_->find_block_index_containing_key(*this->min_key_);
      first_item = (*this->leaf_->block_starting_item)[start_block_i];
    }

    if (this->filter_) {
      this->live_ranges_.emplace(
          this->leaf_->sharded_live_ranges(*this->filter_, Interval<u32>{first_item, item_count}));
    } else {
      this->live_ranges_.emplace(
          this->leaf_->sharded_live_ranges(this->pass_through_filter_,
                                           Interval<u32>{first_item, item_count}));
    }
  }

  void advance() noexcept
  {
    for (;;) {
      auto live_pair = this->live_ranges_->next();
      if (!live_pair) {
        this->done_ = true;
        return;
      }

      const auto [block_index, live_item_range] = *live_pair;

      const usize block_offset = this->leaf_->block_page_offset(block_index);
      StatusOr<ConstBuffer> block_buffer =
          this->load_shard(Interval<usize>{block_offset, block_offset + this->block_size_},
                           llfs::LruPriority{kLeafLruPriority});

      if (!block_buffer.ok()) {
        this->status_ = block_buffer.status();
        this->done_ = true;
        return;
      }

      const PackedLeafBlock& block = PackedLeafBlock::view_of(*block_buffer);

      const Interval<u32> block_item_range = this->leaf_->item_index_range_of_block(block_index);
      const usize local_begin = live_item_range.lower_bound - block_item_range.lower_bound;
      const usize local_end = live_item_range.upper_bound - block_item_range.lower_bound;

      const PackedKeyValueSlotPtr* slice_begin = block.items_begin() + local_begin;
      const PackedKeyValueSlotPtr* slice_end = block.items_begin() + local_end;

      if (this->min_key_ && !this->min_key_applied_) {
        this->min_key_applied_ = true;
        slice_begin = std::lower_bound(
            slice_begin, slice_end, *this->min_key_,
            [](const auto& l, const auto& r) {
              return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
            });
      }

      if (slice_begin == slice_end) {
        continue;
      }

      this->pending_slice_ = PackedKeyValueSlotSlice{as_slice(slice_begin, slice_end)};
      return;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageId page_id_;
  usize block_size_;
  Optional<KeyView> min_key_;
  llfs::PageLoader& page_loader_;
  PageSliceStorage& slice_storage_;
  llfs::PinPageToJob pin_page_to_job_;
  Status& status_;

  const Filter* filter_;
  PiecewiseFilter<u32> pass_through_filter_;

  bool done_ = false;
  bool min_key_applied_ = false;

  const PackedBlockedLeafPage* leaf_ = nullptr;

  using LiveRanges = PackedBlockedLeafPage::ShardedLiveRanges<FilterModelT>;
  Optional<LiveRanges> live_ranges_;

  Optional<Item> pending_slice_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <PiecewiseFilterStorageModel<u32> FilterModelT>
BoxedSeq<PackedKeyValueSlotSlice> scan_blocked_leaf(
    llfs::PageId page_id,
    usize block_size,
    const BasicPiecewiseFilter<u32, FilterModelT>* filter,
    Optional<KeyView> min_key,
    llfs::PageLoader& page_loader,
    PageSliceStorage& slice_storage,
    llfs::PinPageToJob pin_page_to_job,
    Status& status) noexcept
{
  return BlockedLeafScanSeq<FilterModelT>{page_id, block_size, filter, min_key,
                                          page_loader, slice_storage, pin_page_to_job, status}
      | seq::boxed();
}

}  // namespace turtle_kv
