#pragma once

#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <batteries/assert.hpp>
#include <batteries/bool_status.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/seq/loop_control.hpp>

#include <algorithm>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief The set of indices involved in a pivot split; each index is a 0-based item index within
 * the leaf page.
 */
struct SegmentPivotSplitIndices {
  /** \brief The lower bound (inclusive) of the lower-half of the split.  This is also the lower
   * bound of the pre-split region.
   */
  u32 lower_bound;

  /** \brief The index of the first item belonging to the right-hand-side after the split.
   * This is the upper bound (non-inclusive) of the new lower-half, and the lower bound
   * (inclusive) of the new upper-half.
   */
  u32 split_point;

  /** \brief The uppwer bound (non-inclusive) of the upper-half of the split.  This is also the
   * upper bound of the pre-split region.
   */
  u32 upper_bound;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SegmentPivotSplitIndices() = delete;

  explicit SegmentPivotSplitIndices(u32 lower, u32 middle, u32 upper) noexcept
      : lower_bound{lower}
      , split_point{middle}
      , upper_bound{upper}
  {
  }

  explicit SegmentPivotSplitIndices(usize lower, usize middle, usize upper) noexcept
      : SegmentPivotSplitIndices{
            BATT_CHECKED_CAST(u32, lower),
            BATT_CHECKED_CAST(u32, middle),
            BATT_CHECKED_CAST(u32, upper),
        }
  {
  }

  /** \brief Returns the lower half index range for the split.
   */
  Interval<u32> lower_range() const
  {
    return {this->lower_bound, this->split_point};
  }

  /** \brief Returns the upper half index range for the split.
   */
  Interval<u32> upper_range() const
  {
    return {this->split_point, this->upper_bound};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename SegmentT>
struct SegmentAlgorithms {
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SegmentT& segment_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Updates the segment to reflect the splitting of a pivot; inserts a new pivot right
   * after `pivot_i`.
   *
   * Depending on the current state of the segment, it may be possible to do this operation
   * _without_ needing to know the exact value of `split_offset_within_pivot`.  Callers may want to
   * try calling this function using `None` as the second argument, and then if `false` is returned,
   * pay the cost to find the value to the second parameter and retry.
   *
   * \return true iff the split was performed; if false is returned, caller should try again with
   * `split_offset_in_leaf` set to non-None.
   */
  template <typename LevelT>
  [[nodiscard]] bool split_pivot(i32 pivot_i,
                                 Optional<SegmentPivotSplitIndices> split_indices,
                                 const LevelT& level) const
  {
    using batt::BoolStatus;

    this->segment_.check_invariants(__FILE__, __LINE__);
    auto on_scope_exit = batt::finally([&] {
      this->segment_.check_invariants(__FILE__, __LINE__);
    });

    BATT_CHECK_LT(pivot_i, (i32)InMemoryNode::kMaxTempPivots - 1);

    // Simplest case: pivot not active for this segment.
    //
    if (!this->segment_.is_pivot_active(pivot_i)) {
      this->segment_.insert_pivot(pivot_i + 1, false);
      return true;
    }

    // If the pivot we are splitting is currently active, then we need to know the split_indices
    // before we can accurately compute whether the lower/upper ranges of the split are active.
    //
    if (!split_indices) {
      return false;
    }

    // Ask the segment filter whether the lower/upper ranges of the split have live items.
    //
    const Interval<u32> lower_live_range =
        this->segment_.get_live_item_range(level, split_indices->lower_range());

    const Interval<u32> upper_live_range =
        this->segment_.get_live_item_range(level, split_indices->upper_range());

    const bool lower_pivot_active = !lower_live_range.empty();
    const bool upper_pivot_active = !upper_live_range.empty();

    this->segment_.set_pivot_active(pivot_i, lower_pivot_active);
    this->segment_.insert_pivot(pivot_i + 1, upper_pivot_active);

    return true;
  }

  /** \brief Invokes the speficied `fn` for each active pivot in the specified range, passing a
   * reference to the segment and the pivot index (i32).
   */
  template <typename Fn /* = void(SegmentT& segment, i32 pivot_i) */>
  void for_each_active_pivot_in(const Interval<i32>& pivot_range, Fn&& fn)
  {
    // IMPORTANT: we capture a copy of the entire active bitset so that we can iterate the pivots as
    // they were when this function was entered, regardless of what `fn` may do to change the state
    // of the segment.
    //
    const auto observed_active_pivots = this->segment_.get_active_pivots();

    const i32 first_pivot_i = std::max<i32>(pivot_range.lower_bound,  //
                                            observed_active_pivots.first());

    for (i32 pivot_i = first_pivot_i; pivot_i < pivot_range.upper_bound;
         pivot_i = observed_active_pivots.next(pivot_i)) {
      BATT_INVOKE_LOOP_FN((fn, this->segment_, pivot_i));
    }
  }

  /** \brief Invokes the speficied `fn` for each active pivot, passing a reference to the segment
   * and the pivot index (i32).
   */
  template <typename Fn /* = void(SegmentT& segment, i32 pivot_i) */>
  void for_each_active_pivot(Fn&& fn)
  {
    // Call the general version with the full pivot range.
    //
    this->for_each_active_pivot_in(Interval<i32>{0, 64}, BATT_FORWARD(fn));
  }

  /** \brief Drops all pivots within the specified `drop_range` from the segment.
   */
  // TODO [tastolfi 2025-03-26] rename deactivate_pivot_range.
  Status drop_pivot_range(const Interval<i32>& drop_i_range,
                          const Interval<KeyView>& drop_key_range,
                          llfs::PageLoader& page_loader,
                          const TreeOptions& tree_options)
  {
    // First, drop the key range from the segment corresponding to the pivot range. To do this,
    // use sharded queries to find item indexes for the lower and upper bound of the pivot range.
    //
    PageSliceStorage page_slice_storage;

    KeyQuery key_query{page_loader, page_slice_storage, tree_options, drop_key_range.lower_bound};

    BATT_ASSIGN_OK_RESULT(u32 start_item_index,
                          find_key_lower_bound_index(this->segment_.get_leaf_page_id(), key_query));

    key_query = KeyQuery{page_loader, page_slice_storage, tree_options, drop_key_range.upper_bound};

    BATT_ASSIGN_OK_RESULT(u32 end_item_index,
                          find_key_lower_bound_index(this->segment_.get_leaf_page_id(), key_query));

    this->segment_.drop_index_range(Interval<u32>{start_item_index, end_item_index});

    // Then, iterate through the pivots to set the active bit per pivot to 0.
    //
    this->for_each_active_pivot_in(  //
        drop_i_range,                //
        [&drop_i_range](SegmentT& segment, i32 pivot_i) {
          BATT_CHECK(drop_i_range.contains(pivot_i));
          segment.set_pivot_active(pivot_i, false);
        });

    return OkStatus();
  }

  /** \brief Searches the segment for the given key, returning its value if found.
   */
  template <typename LevelT>
  batt::seq::LoopControl find_key(LevelT& level,
                                  i32 key_pivot_i [[maybe_unused]],
                                  KeyQuery& query,
                                  StatusOr<ValueView>* value_out)
  {
    usize key_index_in_leaf = ~usize{0};

    StatusOr<ValueView> found =
        find_key_in_leaf(this->segment_.get_leaf_page_id(), query, key_index_in_leaf);

    if (!found.ok()) {
      return batt::seq::LoopControl::kContinue;
    }

    BATT_CHECK_NE(key_index_in_leaf, ~usize{0});

    // At this point we know the key *is* present in this segment, but it may have
    // been flushed out of the level. Check the segment filter to see if it has been flushed.
    //
    if (this->segment_.is_index_filtered(level, key_index_in_leaf)) {
      //
      // Key was found, but it has been flushed from this segment.  Since keys are
      // unique within a level, we can stop at this point and return kNotFound.
      //
      VLOG(1) << "Key was found in buffer segment, but has been flushed";

      return batt::seq::LoopControl::kBreak;
    }

    // Found!
    //
    *value_out = found;
    return batt::seq::LoopControl::kBreak;
  }
};  // namespace turtle_kv

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename SegmentT>
SegmentAlgorithms<SegmentT> in_segment(SegmentT& segment)
{
  return SegmentAlgorithms<SegmentT>{segment};
}

}  // namespace turtle_kv
