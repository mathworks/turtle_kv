#pragma once

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segments.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/util/bit_ops.hpp>

#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/assert.hpp>

#include <algorithm>
#include <type_traits>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline i32 get_first_active_pivot(i32 pivot_i) noexcept
{
  return pivot_i;
}

inline i32 get_last_active_pivot(i32 pivot_i) noexcept
{
  return pivot_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(usize pivot_i) noexcept
{
  return pivot_i;
}

inline i32 get_last_active_pivot(usize pivot_i) noexcept
{
  return pivot_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const Interval<i32>& pivot_range) noexcept
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const Interval<i32>& pivot_range) noexcept
{
  return pivot_range.upper_bound - 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const CInterval<i32>& pivot_range) noexcept
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const CInterval<i32>& pivot_range) noexcept
{
  return pivot_range.upper_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const Interval<usize>& pivot_range) noexcept
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const Interval<usize>& pivot_range) noexcept
{
  return pivot_range.upper_bound - 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline i32 get_first_active_pivot(const CInterval<usize>& pivot_range) noexcept
{
  return pivot_range.lower_bound;
}

inline i32 get_last_active_pivot(const CInterval<usize>& pivot_range) noexcept
{
  return pivot_range.upper_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename T>
using EnableIfHasActivePivotsBitset =
    std::enable_if_t<std::is_convertible_v<decltype(std::declval<T&&>().get_active_pivots()), u64>>;

//----- --- -- -  -  -   -

template <typename T, typename = EnableIfHasActivePivotsBitset<T>>
inline i32 get_first_active_pivot(T&& segment) noexcept
{
  return first_bit(segment.get_active_pivots());
}

template <typename T, typename = EnableIfHasActivePivotsBitset<T>>
inline i32 get_last_active_pivot(T&& segment) noexcept
{
  return last_bit(segment.get_active_pivots());
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

  static constexpr bool node_available() noexcept
  {
    return !std::is_same_v<std::decay<NodeT>, NodeUnavailable>;
  }

  static constexpr bool page_loader_available() noexcept
  {
    return !std::is_same_v<std::decay<PageLoaderT>, PageLoaderUnavailable>;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  NodeT& node_;
  LevelT& level_;
  PageLoaderT& page_loader_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SegmentedLevelAlgorithms(NodeT& node, LevelT& level, PageLoaderT& page_loader) noexcept
      : node_{node}
      , level_{level}
      , page_loader_{page_loader}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Marks all items in `pivot_i` with keys less than or equal to `max_key` as flushed.
   */
  Status flush_pivot_up_to_key(usize pivot_i, const KeyView& max_key) noexcept
  {
    static_assert(node_available());
    static_assert(page_loader_available());

    VLOG(1) << "flush_pivot_up_to_key(pivot=" << pivot_i << ", " << batt::c_str_literal(max_key)
            << ")";

    KeyView pivot_lower_bound_key = this->node_.get_pivot_key(pivot_i);
    KeyView pivot_upper_bound_key = this->node_.get_pivot_key(pivot_i + 1);

    if (max_key < pivot_lower_bound_key) {
      return batt::StatusCode::kInvalidArgument;
    }

    for (usize segment_i = 0; segment_i < this->level_.segment_count();) {
      SegmentT& segment = this->level_.get_segment(segment_i);
      const u64 active_pivots = segment.get_active_pivots();
      if (!get_bit(active_pivots, pivot_i)) {
        ++segment_i;
        continue;
      }

      BATT_ASSIGN_OK_RESULT(
          PinnedPageT pinned_page,
          segment.load_leaf_page(this->page_loader_, llfs::PinPageToJob::kDefault));

      const PackedLeafPage& leaf_view = PackedLeafPage::view_of(pinned_page.get_page_buffer());

      auto pivot_first = leaf_view.lower_bound(pivot_lower_bound_key);
      auto pivot_last = leaf_view.lower_bound(pivot_upper_bound_key);
      auto flushed_last = leaf_view.lower_bound(max_key);

#if 0
      //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
      BATT_DEBUG_INFO(([&leaf_view, &pivot_last, &flushed_last](std::ostream& out) {
                        out << " flushed_max=";
                        if (flushed_last == leaf_view.items_begin()) {
                          out << "(begin)";
                        } else {
                          out << batt::c_str_literal(get_key(*std::prev(flushed_last)));
                        }
                        out << " flushed_last=";
                        if (flushed_last != leaf_view.items_end()) {
                          out << batt::c_str_literal(get_key(*flushed_last));
                        } else {
                          out << "(end)";
                        }
                        out << " pivot_last=";
                        if (pivot_last != leaf_view.items_end()) {
                          out << batt::c_str_literal(get_key(*pivot_last));
                        } else {
                          out << "(end)";
                        }
                      })
                      << std::endl                                                          //
                      << BATT_INSPECT(pivot_first == flushed_last) << std::endl             //
                      << BATT_INSPECT_STR(max_key)                                          //
                      << BATT_INSPECT_STR(pivot_lower_bound_key)                            //
                      << BATT_INSPECT_STR(pivot_upper_bound_key) << std::endl               //
                      << BATT_INSPECT(std::distance(pivot_first, pivot_last))               //
                      << BATT_INSPECT(std::distance(leaf_view.items_begin(), pivot_first))  //
                      << BATT_INSPECT(std::distance(leaf_view.items_begin(), pivot_last))   //
      );
      //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#endif

      if (flushed_last != leaf_view.items_end() && get_key(*flushed_last) <= max_key) {
        ++flushed_last;
      }
      BATT_CHECK((flushed_last == leaf_view.items_end()) || (get_key(*flushed_last) > max_key));

      const u32 prior_flushed_upper_bound =
          segment.get_flushed_item_upper_bound(this->level_, pivot_i);

      // Check to see whether we are flushing all keys in the pivot range.
      //
      if (flushed_last == pivot_last) {
        segment.set_flushed_item_upper_bound(pivot_i, 0);
        segment.set_pivot_active(pivot_i, false);
      }
      // If the flushed upper bound for this segment is at the start of this pivot's key range,
      // (i.e. flushed_last == pivot_first), then this flush doesn't affect us.
      //
      else if (flushed_last != pivot_first) {
        // The general case; update the flushed upper bound for this pivot in this segment.
        //
        BATT_CHECK_LT(flushed_last, pivot_last);

        const usize new_flushed_upper_bound = std::distance(leaf_view.items_begin(), flushed_last);

        // Make sure we never "unflush" any items!
        //
        if (new_flushed_upper_bound > prior_flushed_upper_bound) {
          segment.set_flushed_item_upper_bound(pivot_i, new_flushed_upper_bound);

          // Sanity check; make sure the new upper bound is correct.
          //
          BATT_CHECK_EQ(segment.get_flushed_item_upper_bound(this->level_, pivot_i),
                        new_flushed_upper_bound);
        }
      }
      // At this point, the flushed upper bound and active pivots set have been updated.
      //----- --- -- -  -  -   -

      // If this segment becomes inactive by flushing the last item in the last active pivot, then
      // remove it from the level.
      //
      if (segment.get_active_pivots() == 0) {
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
                     const KeyView& split_key) noexcept
  {
    static_assert(node_available());
    static_assert(page_loader_available());

    VLOG(1) << "split_pivot(pivot=" << pivot_i << ", key_range=["
            << batt::c_str_literal(old_pivot_key_range.lower_bound) << ".."
            << batt::c_str_literal(old_pivot_key_range.upper_bound)
            << "), key=" << batt::c_str_literal(split_key) << ")";

    BATT_CHECK_LT(this->node_.pivot_count(), 64);

    const KeyView pivot_key = old_pivot_key_range.lower_bound;
    const usize segment_count = this->level_.segment_count();

    BATT_CHECK_LE(pivot_key, split_key);
    BATT_CHECK_LT(split_key, old_pivot_key_range.upper_bound);

    for (usize segment_i = 0; segment_i < segment_count; ++segment_i) {
      SegmentT& segment = this->level_.get_segment(segment_i);

      // If we can split the pivot without loading the leaf, great!
      //
      if (in_segment(segment).split_pivot(pivot_i, None, this->level_)) {
        continue;
      }

      // Else we can't split without knowing the item offset of the split point.
      //
      BATT_ASSIGN_OK_RESULT(PinnedPageT segment_pinned_leaf,
                            segment.load_leaf_page(this->page_loader_, llfs::PinPageToJob::kFalse));

      const PackedLeafPage& leaf_page = PackedLeafPage::view_of(segment_pinned_leaf);

      const usize pivot_offset_in_leaf =
          std::distance(leaf_page.items_begin(), leaf_page.lower_bound(pivot_key));

      const usize split_offset_in_leaf =
          std::distance(leaf_page.items_begin(), leaf_page.lower_bound(split_key));

      VLOG(1) << " --" << BATT_INSPECT(split_offset_in_leaf) << BATT_INSPECT(pivot_offset_in_leaf);

      BATT_CHECK_LE(pivot_offset_in_leaf, split_offset_in_leaf);

      BATT_CHECK(in_segment(segment).split_pivot(pivot_i, split_offset_in_leaf, this->level_));
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
  void for_each_active_segment_in(const PivotSelector& pivot_selector, Fn&& fn) noexcept
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
  void for_each_active_segment_in(i32 pivot_i, Fn&& fn) noexcept
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

  /** \brief Finds the given key in the segments of this level.
   */
  StatusOr<ValueView> find_key(PinnedPageT& pinned_page_out,
                               i32 key_pivot_i,
                               const KeyView& key) noexcept
  {
    static_assert(page_loader_available());

    StatusOr<ValueView> result{Status{batt::StatusCode::kNotFound}};

    this->for_each_active_segment_in(
        key_pivot_i,
        [&](const SegmentT& segment) -> Optional<batt::seq::LoopControl> {
          // We need the actual page at this point to go proceed.
          //
          StatusOr<PinnedPageT> pinned_leaf_page =
              segment.load_leaf_page(this->page_loader_, llfs::PinPageToJob::kDefault);

          if (!pinned_leaf_page.ok()) {
            result = pinned_leaf_page.status();
            return batt::seq::LoopControl::kBreak;
          }
          pinned_page_out = std::move(*pinned_leaf_page);

          // Get the structured view of the page data.
          //
          const PackedLeafPage& leaf_page = PackedLeafPage::view_of(pinned_page_out);

          // Do a point query inside the page for the search key.
          //
          const PackedKeyValue* found = leaf_page.find_key(key);
          if (!found) {
            //
            // The key is not in this segment!  Keep searching...
            //
            return batt::seq::LoopControl::kContinue;
          }

          // At this point we know the key *is* present in this segment, but it may have
          // been flushed out of the level.  Calculate the found key index and compare
          // against the flushed item upper bound for our pivot.
          //
          const usize key_index_in_leaf = std::distance(leaf_page.items_begin(), found);
          const usize flushed_upper_bound =
              segment.get_flushed_item_upper_bound(this->level_, key_pivot_i);

          if (key_index_in_leaf < flushed_upper_bound) {
            //
            // Key was found, but it has been flushed from this segment.  Since keys are
            // unique within a level, we can stop at this point and return kNotFound.
            //
            return batt::seq::LoopControl::kBreak;
          }

          // Found!
          //
          result = get_value(*found);
          return batt::seq::LoopControl::kBreak;
        });

    return result;
  }
};

/** \brief Access algorithms for segmented update buffer level.
 */
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline SegmentedLevelAlgorithms<NodeT, LevelT, PageLoaderT>
in_segmented_level(NodeT& node, LevelT& level, PageLoaderT& page_loader) noexcept
{
  return SegmentedLevelAlgorithms<NodeT, LevelT, PageLoaderT>{
      node,
      level,
      page_loader,
  };
}

/** \brief Access algorithms for segmented update buffer level; only provides access to algorithms
 * which do NOT require access to the node or a page loader.
 */
template <typename LevelT>
inline SegmentedLevelAlgorithms<NodeUnavailable, LevelT, PageLoaderUnavailable> in_segmented_level(
    LevelT& level) noexcept
{
  return SegmentedLevelAlgorithms<NodeUnavailable, LevelT, PageLoaderUnavailable>{
      NodeUnavailable{},
      level,
      PageLoaderUnavailable{},
  };
}

}  // namespace turtle_kv
