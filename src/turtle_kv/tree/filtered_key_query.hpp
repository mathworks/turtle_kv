#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/bloom_filter.hpp>
#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/utility.hpp>

namespace turtle_kv {

struct FilteredKeyQuery {
  using Self = FilteredKeyQuery;

  struct Metrics {
    CountMetric<u64> total_filter_query_count;
    CountMetric<u64> no_filter_page_count;
    CountMetric<u64> filter_page_load_failed_count;
    CountMetric<u64> page_id_mismatch_count;
    CountMetric<u64> filter_reject_count;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageCache* page_cache;
  llfs::PageLoader* page_loader;
  llfs::PinnedPage* pinned_page_out;
  llfs::BloomFilterQuery<KeyView> bloom_filter_query;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FilteredKeyQuery(llfs::PageCache& cache,
                            llfs::PageLoader& loader,
                            llfs::PinnedPage& page_out,
                            const KeyView& key) noexcept
      : page_cache{std::addressof(cache)}
      , page_loader{std::addressof(loader)}
      , pinned_page_out{std::addressof(page_out)}
      , bloom_filter_query{key}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the key being queried.
   */
  BATT_ALWAYS_INLINE const KeyView& key() const
  {
    return this->bloom_filter_query.key;
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
  bool reject_page(llfs::PageId page_id_to_reject, const Optional<llfs::PageId>& filter_page_id)
  {
    Self::metrics().total_filter_query_count.add(1);

    if (!filter_page_id) {
      Self::metrics().no_filter_page_count.add(1);
      return false;
    }

    StatusOr<llfs::PinnedPage> filter_pinned_page =       //
        this->page_loader->get_page_with_layout_in_job(   //
            *filter_page_id,                              //
            llfs::BloomFilterPageView::page_layout_id(),  //
            llfs::PinPageToJob::kDefault,                 //
            llfs::OkIfNotFound{true});

    // Failed to load the page; can't reject.
    //
    if (!filter_pinned_page.ok()) {
      Self::metrics().filter_page_load_failed_count.add(1);
      return false;
    }

    llfs::PinnedPage& pinned_filter_page = *filter_pinned_page;
    const llfs::PageView& page_view = *pinned_filter_page;
    const auto& filter_page_view = static_cast<const llfs::BloomFilterPageView&>(page_view);

    // If we loaded the filter page, but it says it is for a different (src or leaf) page, we can't
    // reject.
    //
    if (filter_page_view.src_page_id() != page_id_to_reject) {
      Self::metrics().page_id_mismatch_count.add(1);
      return false;
    }

    // If the filter says yes, the query key might be in the set; can't reject.
    //
    const bool reject = (filter_page_view.bloom_filter().query(this->bloom_filter_query) == false);

    if (reject) {
      Self::metrics().filter_reject_count.add(1);
    }

    return reject;
  }

  bool reject_page(llfs::PageId page_id_to_reject)
  {
    return this->reject_page(page_id_to_reject, this->filter_page_id_for(page_id_to_reject));
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename SubtreeT>
struct FilteredQuerySubtreeWrapper {
  SubtreeT subtree_;
  FilteredKeyQuery& query_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename SubtreeArgT>
  explicit FilteredQuerySubtreeWrapper(SubtreeArgT&& subtree_arg, FilteredKeyQuery& query) noexcept
      : subtree_{BATT_FORWARD(subtree_arg)}
      , query_{query}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename PageLoaderT, typename PinnedPageT>
  StatusOr<ValueView> find_key(PageLoaderT& page_loader [[maybe_unused]],
                               PinnedPageT& subtree_pinned_page,
                               const KeyView& key) const
  {
    BATT_CHECK_EQ(std::addressof(this->query_.key()), std::addressof(key));

    PinnedPageT* saved_page_out = std::addressof(subtree_pinned_page);
    std::swap(saved_page_out, query_.pinned_page_out);
    auto on_scope_exit = batt::finally([&] {
      std::swap(saved_page_out, query_.pinned_page_out);
    });

    return this->subtree_.find_key_filtered(this->query_);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename NodeT>
struct FilteredQueryNodeWrapper {
  NodeT& node_;
  FilteredKeyQuery& query_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FilteredQueryNodeWrapper(NodeT& node, FilteredKeyQuery& query) noexcept
      : node_{node}
      , query_{query}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BATT_ALWAYS_INLINE decltype(auto) get_pivot_keys() const
  {
    return this->node_.get_pivot_keys();
  }

  BATT_ALWAYS_INLINE decltype(auto) get_level_count() const
  {
    return this->node_.get_level_count();
  }

  BATT_ALWAYS_INLINE auto get_child(i32 key_pivot_i) const
  {
    using SubtreeT = decltype(this->node_.get_child(key_pivot_i));

    return FilteredQuerySubtreeWrapper<SubtreeT>{this->node_.get_child(key_pivot_i), this->query_};
  }

  template <typename PageLoaderT, typename PinnedPageT>
  StatusOr<ValueView> find_key_in_level(usize level_i,
                                        PageLoaderT& page_loader [[maybe_unused]],
                                        PinnedPageT& pinned_page_out,
                                        i32 key_pivot_i,
                                        const KeyView& key) const
  {
    BATT_CHECK_EQ(std::addressof(this->query_.key()), std::addressof(key));

    PinnedPageT* saved_page_out = std::addressof(pinned_page_out);
    std::swap(saved_page_out, query_.pinned_page_out);
    auto on_scope_exit = batt::finally([&] {
      std::swap(saved_page_out, query_.pinned_page_out);
    });

    return this->node_.find_key_in_level_filtered(level_i, key_pivot_i, this->query_);
  }
};

}  // namespace turtle_kv
