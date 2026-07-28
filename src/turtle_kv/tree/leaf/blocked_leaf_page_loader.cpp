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

  // Load the first shard to access the fixed header fields.
  //
  BATT_ASSIGN_OK_RESULT(
      ConstBuffer header_buffer,
      this->load_shard(Interval<usize>{0, this->block_size_},
                       llfs::LruPriority{kTrieIndexLruPriority}));

  this->leaf_ = &PackedBlockedLeafPage::view_of(header_buffer);

  // Load additional shards until the full header metadata is covered.
  //
  const usize header_size = this->leaf_->min_header_shard_size();
  usize loaded = this->block_size_;

  while (loaded < header_size) {
    const usize next_end = std::min(loaded + this->block_size_, header_size);
    BATT_ASSIGN_OK_RESULT(
        ConstBuffer shard_buffer,
        this->load_shard(Interval<usize>{loaded, next_end},
                         llfs::LruPriority{kTrieIndexLruPriority}));
    loaded = next_end;
    (void)shard_buffer;
  }

  this->cache_.resize(this->leaf_->block_count(), None);

  return this->leaf_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<const PackedLeafBlock*> BlockedLeafPageLoader::load_block(u32 block_index) noexcept
{
  BATT_CHECK_NE(this->leaf_, nullptr);
  BATT_CHECK_LT(block_index, this->cache_.size());

  if (this->cache_[block_index]) {
    return &PackedLeafBlock::view_of(*this->cache_[block_index]);
  }

  const usize block_offset = this->leaf_->block_page_offset(block_index);
  BATT_ASSIGN_OK_RESULT(
      ConstBuffer block_buffer,
      this->load_shard(Interval<usize>{block_offset, block_offset + this->block_size_},
                       llfs::LruPriority{kLeafLruPriority}));

  this->cache_[block_index] = block_buffer;

  return &PackedLeafBlock::view_of(block_buffer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> BlockedLeafPageLoader::load_shard(
    const Interval<usize>& shard_interval,
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
    return ConstBuffer{existing->raw_data(), (usize)shard_interval.size()};
  }

  BATT_ASSIGN_OK_RESULT(
      llfs::PinnedPage pinned_shard,
      this->page_loader_.load_page(*shard_page_id,
                                   llfs::PageLoadOptions{llfs::ShardedPageView::page_layout_id(),
                                                         this->pin_page_to_job_,
                                                         llfs::OkIfNotFound{false},
                                                         lru_priority}));

  const void* raw_data = pinned_shard.raw_data();
  this->slice_storage_.insert_pinned_page(std::move(pinned_shard));

  return ConstBuffer{raw_data, (usize)shard_interval.size()};
}

}  // namespace turtle_kv
