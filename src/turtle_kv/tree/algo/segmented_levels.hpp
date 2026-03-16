#pragma once

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segments.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/assert.hpp>

#include <algorithm>
#include <type_traits>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline i32 get_first_active_pivot(i32 pivot_i)
{
  return pivot_i;
}

inline i32 get_last_active_pivot(i32 pivot_i)
{
  return pivot_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(usize pivot_i)
{
  return pivot_i;
}

inline i32 get_last_active_pivot(usize pivot_i)
{
  return pivot_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const Interval<i32>& pivot_range)
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const Interval<i32>& pivot_range)
{
  return pivot_range.upper_bound - 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const CInterval<i32>& pivot_range)
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const CInterval<i32>& pivot_range)
{
  return pivot_range.upper_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const Interval<usize>& pivot_range)
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const Interval<usize>& pivot_range)
{
  return pivot_range.upper_bound - 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const CInterval<usize>& pivot_range)
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const CInterval<usize>& pivot_range)
{
  return pivot_range.upper_bound;
}

//----- --- -- -  -  -   -

template <HasConstActivePivotsSet T>
inline i32 get_first_active_pivot(const T& segment)
{
  return segment.get_active_pivots().first();
}

template <HasConstActivePivotsSet T>
inline i32 get_last_active_pivot(const T& segment)
{
  return segment.get_active_pivots().last();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct SegmentActivePivotOrder {
  template <typename L, typename R>
  bool operator()(L&& l, R&& r) const noexcept
  {
    return get_last_active_pivot(BATT_FORWARD(l)) < get_first_active_pivot(BATT_FORWARD(r));
  }
};

struct NodeUnavailable {
};

struct PageLoaderUnavailable {
  using PinnedPageT = PageLoaderUnavailable;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
struct SegmentedLevelAlgorithms {
  using SegmentT = typename LevelT::Segment;
  using PinnedPageT = typename PageLoaderT::PinnedPageT;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr bool node_available()
  {
    return !std::is_same_v<std::decay<NodeT>, NodeUnavailable>;
  }

  static constexpr bool page_loader_available()
  {
    return !std::is_same_v<std::decay<PageLoaderT>, PageLoaderUnavailable>;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  NodeT& node_;
  LevelT& level_;
  PageLoaderT& page_loader_;
  llfs::PageCacheOvercommit& overcommit_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SegmentedLevelAlgorithms(NodeT& node,
                                    LevelT& level,
                                    PageLoaderT& page_loader,
                                    llfs::PageCacheOvercommit& overcommit) noexcept
      : node_{node}
      , level_{level}
      , page_loader_{page_loader}
      , overcommit_{overcommit}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Filters out the key range in each segment in this level.
   */
  Status drop_key_range(usize pivot_i, const KeyView& max_key)
  {
    static_assert(node_available());
    static_assert(page_loader_available());

    KeyView pivot_lower_bound_key = this->node_.get_pivot_key(pivot_i);
    KeyView pivot_upper_bound_key = this->node_.get_pivot_key(pivot_i + 1);

    for (usize segment_i = 0; segment_i < this->level_.segment_count();) {
      SegmentT& segment = this->level_.get_segment(segment_i);

      // Skip this segment if the pivot is not active.
      //
      if (!segment.is_pivot_active(pivot_i)) {
        ++segment_i;
        continue;
      }

      BATT_ASSIGN_OK_RESULT(PinnedPageT pinned_page,
                            segment.load_leaf_page(this->page_loader_,
                                                   llfs::PinPageToJob::kDefault,
                                                   this->overcommit_));

      const PackedLeafPage& leaf = PackedLeafPage::view_of(pinned_page.get_page_buffer());

      auto flushed_last = leaf.lower_bound(max_key);
      if (flushed_last != leaf.items_end() && get_key(*flushed_last) == max_key) {
        ++flushed_last;
      }

      CInterval<KeyView> flush_key_crange{pivot_lower_bound_key, max_key};
      segment.drop_key_range(flush_key_crange, leaf.items_slice());

      if (flushed_last == leaf.items_end() || get_key(*flushed_last) >= pivot_upper_bound_key) {
        segment.set_pivot_active(pivot_i, false);
      }

      // Drop the segment if it has become inactive due to the flush.
      //
      if (segment.is_inactive()) {
        this->level_.drop_segment(segment_i);
      } else {
        ++segment_i;
      }
    }

    return OkStatus();
  }

  /** \brief Inserts a new pivot *after* `pivot_i`.
   *
   * \param pivot_i The pivot being split; the new sibling is right after this one
   * \param old_pivot_key_range The key range of the pivot prior to the split
   * \param split_key The minimum actual key in the upper half of the split
   */
  Status split_pivot(i32 pivot_i,
                     const Interval<KeyView>& old_pivot_key_range,
                     const KeyView& split_key)
  {
    static_assert(node_available());
    static_assert(page_loader_available());

    VLOG(1) << "split_pivot(pivot=" << pivot_i << ", key_range=["
            << batt::c_str_literal(old_pivot_key_range.lower_bound) << ".."
            << batt::c_str_literal(old_pivot_key_range.upper_bound)
            << "), key=" << batt::c_str_literal(split_key) << ")";

    BATT_CHECK_LT(this->node_.pivot_count(), InMemoryNode::kMaxTempPivots);

    const KeyView pivot_key = old_pivot_key_range.lower_bound;
    const usize segment_count = this->level_.segment_count();

    BATT_CHECK_LE(pivot_key, split_key);
    BATT_CHECK_LT(split_key, old_pivot_key_range.upper_bound);

    for (usize segment_i = 0; segment_i < segment_count; ++segment_i) {
      SegmentT& segment = this->level_.get_segment(segment_i);

      // If we can split the pivot without loading the leaf, great!
      //
      if (in_segment(segment).split_pivot(pivot_i, /*split_indices=*/None, this->level_)) {
        continue;
      }

      // Else we can't split without knowing the item offset of the split point.
      //
      BATT_ASSIGN_OK_RESULT(PinnedPageT segment_pinned_leaf,
                            segment.load_leaf_page(this->page_loader_,
                                                   llfs::PinPageToJob::kFalse,
                                                   this->overcommit_));

      const PackedLeafPage& leaf_page = PackedLeafPage::view_of(segment_pinned_leaf);

      const auto first_item_in_leaf = leaf_page.items_begin();

      const usize pivot_begin_in_leaf =
          std::distance(first_item_in_leaf, leaf_page.lower_bound(pivot_key));

      const usize split_offset_in_leaf =
          std::distance(first_item_in_leaf, leaf_page.lower_bound(split_key));

      const usize pivot_end_in_leaf =
          std::distance(first_item_in_leaf, leaf_page.lower_bound(old_pivot_key_range.upper_bound));

      VLOG(1) << " --" << BATT_INSPECT(split_offset_in_leaf) << BATT_INSPECT(pivot_begin_in_leaf);

      BATT_CHECK_LE(pivot_begin_in_leaf, split_offset_in_leaf);
      BATT_CHECK_LE(split_offset_in_leaf, pivot_end_in_leaf);

      BATT_CHECK(in_segment(segment).split_pivot(pivot_i,
                                                 SegmentPivotSplitIndices{
                                                     pivot_begin_in_leaf,
                                                     split_offset_in_leaf,
                                                     pivot_end_in_leaf,
                                                 },
                                                 this->level_));
    }

    return OkStatus();
  }

  /** \brief Invokes `fn` for each SegmentT& selected by `pivot_selector`.
   *
   * `pivot_selector` can be:
   *   - i32: the pivot index
   *   - Interval<i32>: a half-open interval range of pivot indices
   *   - CInterval<i32>: a closed interval range of pivot indices
   */
  template <typename PivotSelector,
            typename Fn,
            typename = std::enable_if_t<!std::is_convertible_v<std::decay_t<PivotSelector>, i32>>>
  void for_each_active_segment_in(const PivotSelector& pivot_selector, Fn&& fn)
  {
    // Get a slice view of all segments for this level.
    //
    const auto& all_segments = this->level_.get_segments_slice();

    // Use binary search to narrow down the segments to only those whose active pivot range includes
    // the search key's pivot.  Note: this does *not* mean all segments which are actually active
    // for key_pivot_i.  (Example: key_pivot_i = 7, segment active pivots = {4, 5, 8})
    //
    const auto matching_segments = std::equal_range(all_segments.begin(),
                                                    all_segments.end(),
                                                    pivot_selector,
                                                    SegmentActivePivotOrder{});

    // Iterate through the matching segments to try to find the query key.
    //
    for (const SegmentT& segment : as_slice(matching_segments.first, matching_segments.second)) {
      BATT_INVOKE_LOOP_FN((fn, segment));
    }
  }

  /** \brief Invokes `fn` for each SegmentT& which is active for `pivot_i`.
   */
  template <typename Fn>
  void for_each_active_segment_in(i32 pivot_i, Fn&& fn)
  {
    this->for_each_active_segment_in(  //
        CInterval<i32>{pivot_i, pivot_i},
        [&](const SegmentT& segment) -> Optional<batt::seq::LoopControl> {
          // If the active bit is _not_ set for the pivot, then skip this segment.
          //
          if (!segment.is_pivot_active(pivot_i)) {
            return batt::seq::LoopControl::kContinue;
          }

          return batt::seq::invoke_loop_fn(fn, segment);
        });
  }

  StatusOr<ValueView> find_key(i32 key_pivot_i, KeyQuery& query)
  {
    StatusOr<ValueView> result{Status{batt::StatusCode::kNotFound}};

    this->for_each_active_segment_in(
        key_pivot_i,
        [&](const SegmentT& segment) -> Optional<batt::seq::LoopControl> {
          return in_segment(segment).find_key(this->level_, key_pivot_i, query, &result);
        });

    return result;
  }
};

/** \brief Access algorithms for segmented update buffer level.
 */
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline SegmentedLevelAlgorithms<NodeT, LevelT, PageLoaderT> in_segmented_level(
    NodeT& node,
    LevelT& level,
    PageLoaderT& page_loader,
    llfs::PageCacheOvercommit& overcommit)
{
  return SegmentedLevelAlgorithms<NodeT, LevelT, PageLoaderT>{
      node,
      level,
      page_loader,
      overcommit,
  };
}

/** \brief Access algorithms for segmented update buffer level; only provides access to algorithms
 * which do NOT require access to the node or a page loader.
 */
template <typename LevelT>
inline SegmentedLevelAlgorithms<NodeUnavailable, LevelT, PageLoaderUnavailable> in_segmented_level(
    LevelT& level)
{
  return SegmentedLevelAlgorithms<NodeUnavailable, LevelT, PageLoaderUnavailable>{
      NodeUnavailable{},
      level,
      PageLoaderUnavailable{},
      llfs::PageCacheOvercommit::not_allowed(),
  };
}

}  // namespace turtle_kv
