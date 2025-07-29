#pragma once

#include <turtle_kv/config.hpp>

#include <turtle_kv/vqf_filter_page_view.hpp>

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>

#include <turtle_kv/import/bool_status.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/bloom_filter.hpp>
#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/utility.hpp>

namespace turtle_kv {

struct KeyQuery {
  using Self = KeyQuery;

  struct Metrics {
    FastCountMetric<u64> total_filter_query_count;
    FastCountMetric<u64> no_filter_page_count;
    FastCountMetric<u64> filter_page_load_failed_count;
    FastCountMetric<u64> page_id_mismatch_count;
    FastCountMetric<u64> filter_reject_count;
    FastCountMetric<u64> try_pin_leaf_count;
    FastCountMetric<u64> try_pin_leaf_success_count;
    FastCountMetric<u64> sharded_view_find_count;
    FastCountMetric<u64> sharded_view_find_success_count;
    FastCountMetric<u64> filter_positive_count;
    FastCountMetric<u64> filter_false_positive_count;
    LatencyMetric reject_page_latency;
    LatencyMetric filter_lookup_latency;

    double filter_false_positive_rate() const noexcept
    {
      const double positives = filter_positive_count.get();
      if (positives == 0) {
        return -1;
      }
      const double false_positives = filter_false_positive_count.get();
      return false_positives / positives;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageLoader* page_loader;
  llfs::PageCache* page_cache;
  PageSliceStorage* page_slice_storage;
  const TreeOptions* tree_options;

#if TURTLE_KV_USE_BLOOM_FILTER
  llfs::BloomFilterQuery<KeyView> bloom_filter_query;
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
  KeyView key_;
  u64 hash_val = vqf_hash_val(this->key_);
#endif

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit KeyQuery(llfs::PageLoader& loader,
                    PageSliceStorage& slice_storage,
                    const TreeOptions& tree_opts,
                    const KeyView& key_arg) noexcept
      : page_loader{std::addressof(loader)}
      , page_cache{this->page_loader->page_cache()}
      , page_slice_storage{std::addressof(slice_storage)}
      , tree_options{std::addressof(tree_opts)}

#if TURTLE_KV_USE_BLOOM_FILTER
      , bloom_filter_query{key_arg}
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
      , key_{key_arg}
#endif
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the key being queried.
   */
  BATT_ALWAYS_INLINE const KeyView& key() const
  {
#if TURTLE_KV_USE_BLOOM_FILTER
    return this->bloom_filter_query.key;
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
    return this->key_;
#endif
  }

  /** \brief Returns the PageId of the filter page for the given `page_id`, if available; otherwise
   * return None.
   */
  BATT_ALWAYS_INLINE Optional<llfs::PageId> filter_page_id_for(llfs::PageId page_id) const
  {
    return this->page_cache->filter_page_id_for(page_id);
  }

  /** \brief If there is a filter page available for `page_id`, prefetches that page in
   * `this->page_cache` and returns the filter page id; otherwise return None.
   */
  Optional<llfs::PageId> find_and_prefetch_filter(llfs::PageId page_id)
  {
    Optional<llfs::PageId> filter_page_id = this->filter_page_id_for(page_id);

    if (filter_page_id) {
      this->page_cache->prefetch_hint(*filter_page_id);
    }

    return filter_page_id;
  }

  /** \brief If a filter page is available for `page_id_to_reject`, load that page through the cache
   * and look up this->key() in the filter; return true iff the filter returns false (definitely not
   * present).
   *
   * If the passed `filter_page_id` is None, returns false.
   *
   * If the passed page could not be loaded, returns false.
   */
  BoolStatus reject_page(llfs::PageId page_id_to_reject [[maybe_unused]],
                         const Optional<llfs::PageId>& filter_page_id)
  {
    LatencyTimer timer{Every2ToTheConst<16>{}, Self::metrics().reject_page_latency};

    Self::metrics().total_filter_query_count.add(1);

    if (!filter_page_id) {
      Self::metrics().no_filter_page_count.add(1);
      return BoolStatus::kUnknown;
    }

    StatusOr<llfs::PinnedPage> filter_pinned_page = this->page_loader->load_page(  //
        *filter_page_id,
        llfs::PageLoadOptions{
#if TURTLE_KV_USE_BLOOM_FILTER
            llfs::BloomFilterPageView::page_layout_id(),

#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
            VqfFilterPageView::page_layout_id(),
#endif
            llfs::PinPageToJob::kDefault,
            llfs::OkIfNotFound{true},
            llfs::LruPriority{kFilterLruPriority},
        });

    // Failed to load the page; can't reject.
    //
    if (!filter_pinned_page.ok()) {
      Self::metrics().filter_page_load_failed_count.add(1);
      return BoolStatus::kUnknown;
    }

    llfs::PinnedPage& pinned_filter_page = *filter_pinned_page;
    const llfs::PageView& page_view = *pinned_filter_page;
    const auto& filter_page_view =
#if TURTLE_KV_USE_BLOOM_FILTER
        static_cast<const llfs::BloomFilterPageView&>(page_view)
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
            static_cast<const VqfFilterPageView&>(page_view)
#endif
        ;

#if TURTLE_KV_USE_BLOOM_FILTER
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // PARAOID CHECK - TODO [tastolfi 2025-04-04] remove once debugged.
    //
    if (false) {
      filter_page_view.check_integrity();
    }
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // If we loaded the filter page, but it says it is for a different (src or leaf) page, we can't
    // reject.
    //
    if (filter_page_view.src_page_id() != page_id_to_reject) {
      Self::metrics().page_id_mismatch_count.add(1);
      return BoolStatus::kUnknown;
    }

    // If the filter says yes, the query key might be in the set; can't reject.
    //
    const bool reject = BATT_COLLECT_LATENCY_SAMPLE(
        Every2ToTheConst<16>{},
        Self::metrics().filter_lookup_latency,
        (filter_page_view.bloom_filter().query(this->bloom_filter_query) == false));
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
    const bool reject =
        TURTLE_KV_COLLECT_LATENCY_SAMPLE(Every2ToTheConst<16>{},
                                         Self::metrics().filter_lookup_latency,
                                         (filter_page_view.is_present(this->hash_val) == false));
#endif

    if (reject) {
      Self::metrics().filter_reject_count.add(1);
    }

    return bool_status_from(reject);
  }

  BoolStatus reject_page(llfs::PageId page_id_to_reject)
  {
    return this->reject_page(page_id_to_reject, this->filter_page_id_for(page_id_to_reject));
  }
};

StatusOr<ValueView> find_key_in_leaf(llfs::PageId leaf_page_id,
                                     KeyQuery& query,
                                     usize& item_index_out);

StatusOr<ValueView> find_key_in_leaf(const llfs::PageIdSlot& leaf_page_id,
                                     KeyQuery& query,
                                     usize& item_index_out);

}  // namespace turtle_kv
