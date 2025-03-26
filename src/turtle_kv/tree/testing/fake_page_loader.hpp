#pragma once

#include <turtle_kv/tree/testing/fake_pinned_page.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_size.hpp>

#include <memory>
#include <unordered_map>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakePageLoader : public llfs::BasicPageLoader<FakePinnedPage>
{
 public:
  explicit FakePageLoader(llfs::PageSize page_size) noexcept : page_size_{page_size}
  {
  }

  FakePageLoader(const FakePageLoader&) = delete;
  FakePageLoader& operator=(const FakePageLoader&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageSize get_page_size() const
  {
    return this->page_size_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void prefetch_hint(llfs::PageId page_id [[maybe_unused]]) override
  {
    // nothing to do.
  }

  StatusOr<FakePinnedPage> get_page_with_layout_in_job(
      llfs::PageId page_id,
      const Optional<llfs::PageLayoutId>& required_layout,
      llfs::PinPageToJob pin_page_to_job [[maybe_unused]],
      llfs::OkIfNotFound ok_if_not_found [[maybe_unused]]) override
  {
    std::shared_ptr<llfs::PageBuffer>& page_buffer = this->pages_[page_id.int_value()];

    if (!page_buffer) {
      page_buffer = llfs::PageBuffer::allocate(this->page_size_, page_id);
      if (required_layout) {
        llfs::mutable_page_header(page_buffer.get())->layout_id = *required_layout;
      }
    }

    if (required_layout) {
      auto existing_layout_id = llfs::get_page_header(*page_buffer).layout_id;
      if (existing_layout_id != *required_layout) {
        return {batt::StatusCode::kFailedPrecondition};
      }
    }

    return FakePinnedPage{batt::make_copy(page_buffer)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageSize page_size_;
  std::unordered_map<u64, std::shared_ptr<llfs::PageBuffer>> pages_;
};

}  // namespace testing
}  // namespace turtle_kv
