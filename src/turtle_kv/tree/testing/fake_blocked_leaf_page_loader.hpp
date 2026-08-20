//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>
#include <turtle_kv/tree/leaf/packed_leaf_block.hpp>
#include <turtle_kv/tree/testing/fake_page_loader.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id.hpp>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakeBlockedLeafPageLoader
{
 public:
  explicit FakeBlockedLeafPageLoader(const PackedBlockedLeafPage* leaf) noexcept : leaf_{leaf}
  {
  }

  explicit FakeBlockedLeafPageLoader(FakePageLoader& page_loader) noexcept
      : leaf_{nullptr}
      , page_loader_{&page_loader}
  {
  }

  StatusOr<const PackedBlockedLeafPage*> set_page(llfs::PageId page_id) noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->page_loader_);

    BATT_ASSIGN_OK_RESULT(auto pinned_page,
                          this->page_loader_->load_page(page_id, llfs::PageLoadOptions{}));
    this->pinned_page_ = std::move(pinned_page);
    this->leaf_ = PackedBlockedLeafPage::view_of(this->pinned_page_);
    return this->leaf_;
  }

  StatusOr<const PackedLeafBlock*> load_block(u32 block_index) noexcept
  {
    BATT_CHECK_NOT_NULLPTR(this->leaf_);
    return &*(this->leaf_->blocks_begin() + block_index);
  }

 private:
  const PackedBlockedLeafPage* leaf_;
  FakePageLoader* page_loader_ = nullptr;
  FakePinnedPage pinned_page_;
};

}  // namespace testing
}  // namespace turtle_kv