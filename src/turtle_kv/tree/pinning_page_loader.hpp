#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_page_header.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/sharded_page_view.hpp>

#include <absl/container/flat_hash_map.h>

namespace turtle_kv {

class PinningPageLoader : public llfs::PageLoader
{
 public:
  using Self = PinningPageLoader;

  struct Metrics {
    LatencyMetric prefetch_hint_latency;
    LatencyMetric hash_map_lookup_latency;
    LatencyMetric get_page_from_cache_latency;
    FastCountMetric<u64> hash_map_miss_count;
    FastCountMetric<u64> get_page_count;
  };

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PinningPageLoader(llfs::PageLoader& base_loader) noexcept : base_loader_{base_loader}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  StatusOr<llfs::PinnedPage> hash_map_lookup(
      llfs::PageId page_id,
      const Optional<llfs::PageLayoutId>& required_layout) const
  {
    auto iter = BATT_COLLECT_LATENCY_SAMPLE(Every2ToTheConst<16>{},
                                            this->metrics_.hash_map_lookup_latency,
                                            this->pinned_pages_.find(page_id.int_value()));
    if (iter != this->pinned_pages_.end()) {
      if (required_layout && *required_layout != llfs::ShardedPageView::page_layout_id()) {
        BATT_REQUIRE_OK(llfs::require_page_layout(iter->second.page_buffer(), required_layout));
      }
      return {iter->second};
    }
    this->metrics_.hash_map_miss_count.add(1);

    return {batt::StatusCode::kUnavailable};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // llfs::PageLoader interface

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  llfs::PageCache* page_cache() const override
  {
    return this->base_loader_.page_cache();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void prefetch_hint(llfs::PageId page_id) override
  {
    LatencyTimer timer{Every2ToTheConst<16>{}, this->metrics_.prefetch_hint_latency};
    this->base_loader_.prefetch_hint(page_id);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  StatusOr<llfs::PinnedPage> try_pin_cached_page(
      llfs::PageId page_id,
      const Optional<llfs::PageLayoutId>& required_layout,
      llfs::PinPageToJob pin_page_to_job) override
  {
    StatusOr<llfs::PinnedPage> found_in_hash_map = this->hash_map_lookup(page_id, required_layout);
    if (found_in_hash_map.ok()) {
      if (pin_page_to_job != llfs::PinPageToJob::kFalse) {
        this->pinned_pages_.emplace(page_id.int_value(), *found_in_hash_map);
      }
      return found_in_hash_map;
    }

    return this->base_loader_.try_pin_cached_page(page_id, required_layout, pin_page_to_job);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  StatusOr<llfs::PinnedPage> get_page_with_layout_in_job(
      llfs::PageId page_id,
      const Optional<llfs::PageLayoutId>& required_layout,
      llfs::PinPageToJob pin_page_to_job,
      llfs::OkIfNotFound ok_if_not_found) override
  {
    this->metrics_.get_page_count.add(1);

    StatusOr<llfs::PinnedPage> found_in_hash_map = this->hash_map_lookup(page_id, required_layout);
    if (found_in_hash_map.ok()) {
      return found_in_hash_map;
    }

    BATT_ASSIGN_OK_RESULT(llfs::PinnedPage pinned_page,
                          BATT_COLLECT_LATENCY_SAMPLE(
                              Every2ToTheConst<16>{},
                              this->metrics_.get_page_from_cache_latency,
                              this->base_loader_.get_page_with_layout_in_job(page_id,
                                                                             required_layout,
                                                                             pin_page_to_job,
                                                                             ok_if_not_found)));

    if (pin_page_to_job != llfs::PinPageToJob::kFalse) {
      this->pinned_pages_.emplace(page_id.int_value(), pinned_page);
    }

    return pinned_page;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& base_loader_;
  absl::flat_hash_map<u64, llfs::PinnedPage> pinned_pages_;
  Metrics& metrics_ = Self::metrics();
};

}  // namespace turtle_kv
