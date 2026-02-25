#pragma once

#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/bit_ops.hpp>
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

    BATT_CHECK_LT(pivot_i, InMemoryNode::kMaxTempPivots - 1);

    // Simplest case: pivot not active for this segment.
    //
    if (!this->segment_.is_pivot_active(pivot_i)) {
      this->segment_.insert_pivot(pivot_i + 1, false);
      return true;
    }

    const BoolStatus old_pivot_becomes_inactive = [&] {
      if (!split_offset_in_leaf) {
        return BoolStatus::kUnknown;
      }

      if (*split_offset_in_leaf == 0) {
        return BoolStatus::kTrue;
      }

      return batt::bool_status_from(
          this->segment_.is_index_filtered(level, *split_offset_in_leaf - 1));
    }();

    const BoolStatus new_pivot_has_flushed_items = [&] {
      if (old_pivot_becomes_inactive == BoolStatus::kUnknown) {
        return BoolStatus::kUnknown;
      }

      return batt::bool_status_from(old_pivot_becomes_inactive == BoolStatus::kTrue &&
                                    this->segment_.is_index_filtered(level, *split_offset_in_leaf));
    }();

    // Next simplest: pivot active, but flush count is zero for pivot.
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

    // If the split is not after the last flushed item, then the lower pivot (in the split) is now
    // inactive and the upper one is active, possibly with some flushed items.
    //
    this->segment_.set_pivot_active(pivot_i, false);
    this->segment_.insert_pivot(pivot_i + 1, true);

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
    const std::pair<u64, u64> observed_active_pivots =
        this->segment_.get_active_pivots_with_overflow();

    const u64 first_active_bit = first_bit(observed_active_pivots.first);
    const u64 first_active_overflow_bit = first_bit(observed_active_pivots.second);
    const u64 first_active_pivot =
        (first_active_bit != 64) ? first_active_bit : (first_active_overflow_bit + 64);

    const i32 first_pivot_i = std::max<i32>(pivot_range.lower_bound,  //
                                            first_active_pivot);

    for (i32 pivot_i = first_pivot_i; pivot_i < pivot_range.upper_bound;) {
      BATT_INVOKE_LOOP_FN((fn, this->segment_, pivot_i));

      if (pivot_i < 64) {
        pivot_i = next_bit(observed_active_pivots.first, pivot_i);
        if (pivot_i == 64) {
          pivot_i = first_bit(observed_active_pivots.second) + 64;
        }
      } else {
        const i32 overflow_i = pivot_i - 64;
        if (overflow_i >= 64) {
          break;
        }
        pivot_i = next_bit(observed_active_pivots.second, overflow_i) + 64;
      }
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
