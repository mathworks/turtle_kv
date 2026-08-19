//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_LEAF_BLOCKED_LEAF_PAGE_LOADER_HPP

#include "packed_blocked_leaf_page.hpp"
#include "packed_leaf_block.hpp"

#include <turtle_kv/config.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Loads header and blocks from a PackedBlockedLeafPage via sharded page views.
 *
 * Bound to one page at a time. Call `set_page` to move to a new page.
 */
class BlockedLeafPageLoader
{
 public:
  explicit BlockedLeafPageLoader(llfs::PageLoader& page_loader,
                                 PageSliceStorage& slice_storage,
                                 llfs::PinPageToJob pin_page_to_job,
                                 usize block_size) noexcept;

  /** \brief Loads the header shard for the given page, clears the block cache, and returns
   * the validated PackedBlockedLeafPage pointer.
   */
  StatusOr<const PackedBlockedLeafPage*> set_page(llfs::PageId page_id) noexcept;

  /** \brief Returns the current leaf pointer, or nullptr if no page is set.
   */
  const PackedBlockedLeafPage* leaf() const noexcept
  {
    return this->leaf_;
  }

  /** \brief Loads the block at the given index. Returns a cached result on subsequent calls.
   */
  StatusOr<const PackedLeafBlock*> load_block(u32 block_index) noexcept;

 private:
  static constexpr usize kCacheSlots = 256;

  struct CacheSlot {
    u32 tag;
    ConstBuffer buffer;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageLoader& page_loader_;
  PageSliceStorage& slice_storage_;
  llfs::PinPageToJob pin_page_to_job_;
  usize block_size_;

  llfs::PageId page_id_;
  const PackedBlockedLeafPage* leaf_ = nullptr;
  SmallVec<CacheSlot, kCacheSlots> cache_;
};

}  // namespace turtle_kv
