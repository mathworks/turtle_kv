#pragma once

#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/util/bit_ops.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <batteries/assert.hpp>
#include <batteries/bool_status.hpp>
#include <batteries/seq/loop_control.hpp>

#include <algorithm>

namespace turtle_kv {

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
                                 Optional<usize> split_offset_in_leaf,
                                 const LevelT& level) const
  {
    using batt::BoolStatus;

    this->segment_.check_invariants(__FILE__, __LINE__);
    auto on_scope_exit = batt::finally([&] {
      this->segment_.check_invariants(__FILE__, __LINE__);
    });

    BATT_CHECK_LT(pivot_i, 63);

    const usize pivot_flush_upper_bound =
        this->segment_.get_flushed_item_upper_bound(level, pivot_i);

    // Simplest case: pivot not active for this segment.
    //
    if (!this->segment_.is_pivot_active(pivot_i)) {
      BATT_CHECK_EQ(pivot_flush_upper_bound, 0)
          << "Sanity check failed: segment can not be inactive for a given pivot and also have a "
             "nonzero flushed item upper bound!";

      this->segment_.insert_pivot(pivot_i + 1, false);
      return true;
    }

    const BoolStatus old_pivot_becomes_inactive = [&] {
      if (pivot_flush_upper_bound == 0) {
        return BoolStatus::kFalse;
      }
      if (!split_offset_in_leaf) {
        return BoolStatus::kUnknown;
      }
      return batt::bool_status_from(*split_offset_in_leaf <= pivot_flush_upper_bound);
    }();

    const BoolStatus new_pivot_has_flushed_items = [&] {
      if (old_pivot_becomes_inactive == BoolStatus::kUnknown) {
        return BoolStatus::kUnknown;
      }
      return batt::bool_status_from(old_pivot_becomes_inactive == BoolStatus::kTrue &&
                                    *split_offset_in_leaf < pivot_flush_upper_bound);
    }();

    // Next simplest: pivot active, but flush count is zero for pivot; flush counts stay the same.
    //
    if (old_pivot_becomes_inactive == BoolStatus::kFalse) {
      BATT_CHECK_EQ(new_pivot_has_flushed_items, BoolStatus::kFalse);
      this->segment_.insert_pivot(pivot_i + 1, true);
      return true;
    }

    // At this point we can only proceed if we know the item count of the split position relative to
    // the pivot key range start.
    //
    if (old_pivot_becomes_inactive == BoolStatus::kUnknown ||
        new_pivot_has_flushed_items == BoolStatus::kUnknown) {
      return false;
    }

    BATT_CHECK_EQ(old_pivot_becomes_inactive, BoolStatus::kTrue);
    BATT_CHECK(split_offset_in_leaf);
    BATT_CHECK_GE(pivot_flush_upper_bound, *split_offset_in_leaf);

    // If the split is not after the last flushed item, then the lower pivot (in the split) is now
    // inactive and the upper one is active, possibly with some flushed items.
    //
    this->segment_.set_flushed_item_upper_bound(pivot_i, 0);
    this->segment_.set_pivot_active(pivot_i, false);
    this->segment_.insert_pivot(pivot_i + 1, true);

    if (new_pivot_has_flushed_items == BoolStatus::kTrue) {
      this->segment_.set_flushed_item_upper_bound(pivot_i + 1, pivot_flush_upper_bound);
    } else {
      this->segment_.set_flushed_item_upper_bound(pivot_i + 1, 0);
    }
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
    const u64 observed_active_pivots = this->segment_.get_active_pivots();

    const i32 first_pivot_i = std::max<i32>(pivot_range.lower_bound,  //
                                            first_bit(observed_active_pivots));

    for (i32 pivot_i = first_pivot_i; pivot_i < pivot_range.upper_bound;
         pivot_i = next_bit(observed_active_pivots, pivot_i)) {
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
  void drop_pivot_range(const Interval<i32>& drop_range)
  {
    this->for_each_active_pivot_in(  //
        drop_range,                  //
        [&drop_range](SegmentT& segment, i32 pivot_i) {
          BATT_CHECK(drop_range.contains(pivot_i));

          segment.set_flushed_item_upper_bound(pivot_i, 0);
          segment.set_pivot_active(pivot_i, false);
        });
  }

  /** \brief Searches the segment for the given key, returning its value if found.
   */
  template <typename PageLoaderT, typename PinnedPageT, typename LevelT>
  batt::seq::LoopControl find_key(PageLoaderT& page_loader,
                                  PinnedPageT& pinned_page_out,
                                  LevelT& level,
                                  const KeyView& key,
                                  i32 key_pivot_i,
                                  StatusOr<ValueView>* value_out)
  {
    // We need the actual page at this point to go proceed.
    //
    StatusOr<PinnedPageT> pinned_leaf_page =
        this->segment_.load_leaf_page(page_loader, llfs::PinPageToJob::kDefault);

    if (!pinned_leaf_page.ok()) {
      *value_out = pinned_leaf_page.status();
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
        this->segment_.get_flushed_item_upper_bound(level, key_pivot_i);

    if (key_index_in_leaf < flushed_upper_bound) {
      //
      // Key was found, but it has been flushed from this segment.  Since keys are
      // unique within a level, we can stop at this point and return kNotFound.
      //
      return batt::seq::LoopControl::kBreak;
    }

    // Found!
    //
    *value_out = get_value(*found);
    return batt::seq::LoopControl::kBreak;
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename SegmentT>
SegmentAlgorithms<SegmentT> in_segment(SegmentT& segment)
{
  return SegmentAlgorithms<SegmentT>{segment};
}

}  // namespace turtle_kv
