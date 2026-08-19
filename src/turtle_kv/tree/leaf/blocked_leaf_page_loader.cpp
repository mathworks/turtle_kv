//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "blocked_leaf_page_loader.hpp"

#include "packed_leaf_block.ipp"

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BlockedLeafPageLoader::BlockedLeafPageLoader(llfs::PageLoader& page_loader,
                                             PageSliceStorage& slice_storage,
                                             llfs::PinPageToJob pin_page_to_job,
                                             usize block_size) noexcept
    : page_loader_{page_loader}
    , slice_storage_{slice_storage}
    , pin_page_to_job_{pin_page_to_job}
    , block_size_{block_size}
    , page_id_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<const PackedBlockedLeafPage*> BlockedLeafPageLoader::set_page(
    llfs::PageId page_id) noexcept
{
  this->page_id_ = page_id;
  this->leaf_ = nullptr;
  this->cache_.clear();

  PageSliceReader slice_reader{this->page_loader_,
                               page_id,
                               llfs::PageSize{BATT_CHECKED_CAST(i32, this->block_size_)}};

  // Load the first shard to read the fixed header fields.
  //
  BATT_ASSIGN_OK_RESULT(ConstBuffer header_buffer,
                        slice_reader.read_slice(Interval<usize>{0, this->block_size_},
                                                this->slice_storage_,
                                                this->pin_page_to_job_,
                                                llfs::LruPriority{kTrieIndexLruPriority}));

  this->leaf_ = &PackedBlockedLeafPage::view_of(header_buffer);

  // Load the full header (block_starting_item + ART) as a contiguous buffer.
  //
  const usize header_size = this->leaf_->min_header_shard_size();

  if (header_size > this->block_size_) {
    BATT_ASSIGN_OK_RESULT(header_buffer,
                          slice_reader.read_slice(Interval<usize>{0, header_size},
                                                  this->slice_storage_,
                                                  this->pin_page_to_job_,
                                                  llfs::LruPriority{kTrieIndexLruPriority}));

    this->leaf_ = &PackedBlockedLeafPage::view_of(header_buffer);
  }

  this->cache_.assign(kCacheSlots, CacheSlot{0, ConstBuffer{}});

  return this->leaf_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<const PackedLeafBlock*> BlockedLeafPageLoader::load_block(u32 block_index) noexcept
{
  BATT_CHECK_NE(this->leaf_, nullptr);
  BATT_CHECK_LT(block_index, this->leaf_->block_count());

  const u32 slot = block_index % kCacheSlots;
  const u32 tag = block_index / kCacheSlots;

  if (this->cache_[slot].buffer.data() && this->cache_[slot].tag == tag) {
    return &PackedLeafBlock::view_of(this->cache_[slot].buffer);
  }

  llfs::PageCache& page_cache = *this->page_loader_.page_cache();

  Optional<llfs::PageId> shard_page_id =
      this->leaf_->page_shard_id_for_block(page_cache, block_index, this->page_id_);

  if (!shard_page_id) {
    return {batt::StatusCode::kUnavailable};
  }

  const llfs::PinnedPage* existing = this->slice_storage_.find_pinned_page(*shard_page_id);
  if (!existing) {
    BATT_ASSIGN_OK_RESULT(
        llfs::PinnedPage pinned_shard,
        this->page_loader_.load_page(*shard_page_id,
                                     llfs::PageLoadOptions{llfs::ShardedPageView::page_layout_id(),
                                                           this->pin_page_to_job_,
                                                           llfs::OkIfNotFound{false},
                                                           llfs::LruPriority{kLeafLruPriority}}));

    this->slice_storage_.insert_pinned_page(std::move(pinned_shard));
    existing = this->slice_storage_.find_pinned_page(*shard_page_id);
  }

  ConstBuffer block_buffer{existing->raw_data(), this->block_size_};
  this->cache_[slot] = CacheSlot{tag, block_buffer};

  return &PackedLeafBlock::view_of(block_buffer);
}

}  // namespace turtle_kv
