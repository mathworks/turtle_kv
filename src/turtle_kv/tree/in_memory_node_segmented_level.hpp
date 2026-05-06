#pragma once

#include <turtle_kv/tree/active_pivots_set.hpp>
#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/update_buffer_levels.hpp>

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>

#include <llfs/page_cache_job.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/pinned_page.hpp>

namespace turtle_kv {

struct InMemoryNode;
struct BatchUpdateContext;
class KeyQuery;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Mutable, in-memory representation of a serialized buffer segment (one leaf page).
 */
struct InMemoryNodeSegment {
  using Self = InMemoryNodeSegment;

  /** \brief The id of the leaf page for this segment.
   */
  llfs::PageIdSlot page_id_slot;

  /** \brief A bit set of pivots in whose key range this segment contains items.
   */
  ActivePivotsSet128 active_pivots;

  /** \brief A filter over the flushed items in this segment.
   */
  PiecewiseFilter</*OffsetT=*/u32> filter;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a reference to the PageId of this segment's leaf page, plus weak reference
   * to its cache slot (if known).
   */
  const llfs::PageIdSlot& get_leaf_page_id() const
  {
    return this->page_id_slot;
  }

  /** \brief Returns the active pivots bit set.
   */
  ActivePivotsSet128 get_active_pivots() const
  {
    return this->active_pivots;
  }

  /** \brief Marks this segment as containing (or not) active keys addressed to `pivot_i`.
   */
  void set_pivot_active(i32 pivot_i, bool active)
  {
    this->active_pivots.set(pivot_i, active);
  }

  /** \brief Returns true iff this segment has active keys addressed to `pivot_i`.
   */
  bool is_pivot_active(i32 pivot_i) const
  {
    return this->active_pivots.get(pivot_i);
  }

  template <typename Traits>
  Interval<u32> drop_key_range(const BasicInterval<Traits>& key_range,
                               const Slice<const PackedKeyValue>& items)
  {
    return drop_item_range(this->filter, items, key_range, llfs::KeyRangeOrder{});
  }

  void drop_index_range(Interval<u32> i)
  {
    this->filter.drop_index_range(i);
  }

  bool is_index_filtered(const InMemoryNodeSegmentedLevel&, u32 index) const
  {
    return !this->filter.live_at_index(index);
  }

  u32 live_lower_bound(const InMemoryNodeSegmentedLevel&, u32 item_i) const
  {
    return this->filter.live_lower_bound(item_i);
  }

  Interval<u32> get_live_item_range(const InMemoryNodeSegmentedLevel&, Interval<u32> i) const
  {
    return this->filter.find_live_range(i);
  }

  usize num_cut_points() const
  {
    Slice<const Interval<u32>> live_ranges = this->filter.live();

    // If the first element is live, don't include 0 as a cut point, only include the
    // upper bound.
    //
    bool first_element_live = !live_ranges.empty() && live_ranges.front().lower_bound ==
                                                          PiecewiseFilter<u32>::kMinLowerBound;
    bool ends_at_max = !live_ranges.empty() &&
                       live_ranges.back().upper_bound == PiecewiseFilter<u32>::kMaxUpperBound;

    return live_ranges.size() * 2 - (first_element_live ? 1 : 0) - (ends_at_max ? 1 : 0);
  }

  void deduplicate(const InMemoryNodeSegment& other)
  {
    this->active_pivots |= other.active_pivots;
    this->filter.merge(other.filter);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Panic if filter invariants are not satisfied.
   */
  void check_invariants(const char* file, int line) const;

  /** \brief Inserts a new pivot bit in this->active_pivots at position
   * `pivot_i`.  This is called when a child subtree needs to be split.
   */
  void insert_pivot(i32 pivot_i, bool is_active);

  /** \brief Removes a bit in the active pivots bit set at position `pivot_i`.
   */
  void remove_pivot(i32 pivot_i);

  /** \brief Removes the specified number (`count`) pivots from the front of this segment.  This
   * is used while splitting a node's update buffer.
   */
  void pop_front_pivots(i32 count);

  /** \brief Returns true iff this segment is not active for any pivots.
   */
  bool is_inactive() const;

  /** \brief Loads the leaf page for this Segment, returning the resulting llfs::PinnedPage.
   */
  StatusOr<llfs::PinnedPage> load_leaf_page(llfs::PageLoader& page_loader,
                                            llfs::PinPageToJob pin_page_to_job,
                                            llfs::PageCacheOvercommit& overcommit) const;

  /** \brief Prints a human-readable representation of this Segment.
   */
  SmallFn<void(std::ostream&)> dump(bool multi_line = true) const;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Mutable, in-memory representation of a non-empty serialized update buffer level.
 */
struct InMemoryNodeSegmentedLevel {
  using Self = InMemoryNodeSegmentedLevel;
  using Segment = InMemoryNodeSegment;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SmallVec<Segment, 32> segments;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const
  {
    return this->segments.empty();
  }

  usize segment_count() const
  {
    return this->segments.size();
  }

  Segment& get_segment(usize i)
  {
    return this->segments[i];
  }

  const Segment& get_segment(usize i) const
  {
    return this->segments[i];
  }

  Slice<const Segment> get_segments_slice() const
  {
    return as_const_slice(this->segments);
  }

  InMemoryNodeSegment* front()
  {
    if (this->segments.empty()) {
      return nullptr;
    }

    return &this->segments.front();
  }

  InMemoryNodeSegment* back()
  {
        if (this->segments.empty()) {
      return nullptr;
    }

    return &this->segments.back();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Removes the specified segment from this level.
   *
   * Should only be called by SegmentedLevelAlgorithms::flush_pivot_up_to_key.
   */
  void drop_segment(usize i);

  /** \brief Removes the specified set of pivots from this level.
   *
   * Used to implement node splits.
   */
  void drop_pivot_range(const Interval<i32>& pivot_i_range,
                        const Interval<KeyView>& pivot_key_range,
                        llfs::PageLoader& page_loader,
                        const TreeOptions& tree_options);

  /** \brief Drops all pivots before (but not including) the specified pivot.
   *
   */
  void drop_before_pivot(i32 pivot_i,
                         const KeyView& pivot_key,
                         llfs::PageLoader& page_loader,
                         const TreeOptions& tree_options);

  /** \brief Drops all pivots after (and including) the specified pivot.
   *
   */
  void drop_after_pivot(i32 pivot_i,
                        const KeyView& pivot_key,
                        llfs::PageLoader& page_loader,
                        const TreeOptions& tree_options);

  /** \brief Returns true iff the specified pivot is active for any of the Segments in this
   * level.
   */
  bool is_pivot_active(i32 pivot_i) const;

  /** \brief Verifies that all items appear in this level in key-sorted order; panic if this is
   * not the case.
   *
   * Warning: This is a very expensive operation!  Do not call it on a performance-critical code
   * path.
   */
  void check_items_sorted(const InMemoryNode& node, llfs::PageLoader& page_loader) const;

  /** \brief Converts the unflushed items in this level to a boxed sequence.
   */
  BoxedSeq<EditSlice> to_boxed_seq(const InMemoryNode& node,
                                   BatchUpdateContext& update_context,
                                   Status& segment_load_status,
                                   i32 min_pivot_i,
                                   bool only_pivot,
                                   Optional<KeyView> min_key) const;

  /** \brief Marks the items contained in `flush_key_crange` that are addressed to `pivot_i`
   * as flushed within this level.
   */
  bool set_pivot_items_flushed(const InMemoryNode& node,
                               BatchUpdateContext& update_context,
                               usize pivot_i,
                               const CInterval<KeyView>& flush_key_crange,
                               Status& segment_load_status);

  /** \brief Marks the pivot `pivot_i` as completely flushed within this level.
   */
  bool set_pivot_completely_flushed(usize pivot_i);

  /** \brief Calculates the number of filter cut points needed for this level when it will
   * be serialized.
   */
  usize segment_filter_cut_points() const;

  void push_front_pivots(usize node_pivot_count);

  StatusOr<ValueView> find_key(const InMemoryNode& node, KeyQuery& query, i32 key_pivot_i) const;

  /** \brief Merges this level with a "sibling" level from another node.
   *
   * This function is called when two nodes are being merged and their update buffers are
   * being merged as well. In this function, this level is the "left" level (i.e., the level
   * comes from the left node in the merge) and `sibling_level` is the "right" level.
   *
   * `node_pivot_count` is the number of pivots in the left node (i.e., the node that this
   * level exists in).
   */
  InMemoryNodeLevel merge(InMemoryNodeLevel&& sibling_level, usize node_pivot_count) &&;

  /** \brief Prints a human-readable representation of the level.
   */
  SmallFn<void(std::ostream&)> dump() const;
};

}  // namespace turtle_kv
