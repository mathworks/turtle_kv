#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_page_header.hpp>
#include <llfs/page_loader.hpp>

#include <absl/container/flat_hash_map.h>

namespace turtle_kv {

class PinningPageLoader : public llfs::PageLoader
{
 public:
  explicit PinningPageLoader(llfs::PageLoader& base_loader) noexcept : base_loader_{base_loader}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // llfs::PageLoader interface

  void prefetch_hint(llfs::PageId page_id) override
  {
    this->base_loader_.prefetch_hint(page_id);
  }

  StatusOr<llfs::PinnedPage> get_page_with_layout_in_job(
      llfs::PageId page_id,
      const Optional<llfs::PageLayoutId>& required_layout,
      llfs::PinPageToJob pin_page_to_job,
      llfs::OkIfNotFound ok_if_not_found) override
  {
    auto iter = this->pinned_pages_.find(page_id.int_value());
    if (iter != this->pinned_pages_.end()) {
      BATT_REQUIRE_OK(llfs::require_page_layout(iter->second.page_buffer(), required_layout));
      return iter->second;
    }

    BATT_ASSIGN_OK_RESULT(llfs::PinnedPage pinned_page,
                          this->base_loader_.get_page_with_layout_in_job(page_id,
                                                                         required_layout,
                                                                         pin_page_to_job,
                                                                         ok_if_not_found));

    if (pin_page_to_job != llfs::PinPageToJob::kFalse) {
      this->pinned_pages_.emplace(page_id.int_value(), pinned_page);
    }

    return pinned_page;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& base_loader_;
  absl::flat_hash_map<u64, llfs::PinnedPage> pinned_pages_;
};

}  // namespace turtle_kv
