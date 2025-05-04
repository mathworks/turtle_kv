#pragma once

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/core/edit_slice.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

namespace turtle_kv {

class SegmentedLevelScannerBase
{
 protected:
  Status self_contained_status_;
};

/** \brief
 *
 * Node concept:
 *
 *  node->pivot_count() -> usize
 *  node->get_pivot_key(usize i) -> KeyView
 *
 * Level concept:
 *
 *  typename Level::Segment
 *  level->segment_count() -> usize
 *  level->get_segment(usize i) -> LevelT::Segment&
 *
 * Segment concept:
 *
 *  segment->load_leaf_page(PageLoaderT&, llfs::PinPageToJob) -> StatusOr<PinnedPageT>
 *  segment->get_active_pivots() -> u64
 *    - Returns a bitset identifying the pivots for which this segment contains active edits
 *  segment->get_flushed_pivots() -> u64
 *    - Returns a bitset identifying the pivots for which this segment contains active edits
 *  segment->get_flushed_item_upper_bound(Level& level, usize pivot_i) -> usize
 *    - Returns the position one past the last flushed item for pivot_i, from the leaf start
 */
template <typename NodeT, typename LevelT, typename PageLoaderT>
class SegmentedLevelScanner : private SegmentedLevelScannerBase
{
 public:
  using Self = SegmentedLevelScanner;
  using Super = SegmentedLevelScannerBase;
  using Node = NodeT;
  using Level = LevelT;
  using PageLoader = PageLoaderT;
  using PinnedPageT = typename PageLoader::PinnedPageT;
  using Segment = typename Level::Segment;

  using Item = EditSlice;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SegmentedLevelScanner(Node& node,
                                 Level& level,
                                 PageLoader& loader,
                                 llfs::PinPageToJob pin_pages_to_job,
                                 Status& status,
                                 i32 min_pivot_i = 0) noexcept;

  explicit SegmentedLevelScanner(Node& node,
                                 Level& level,
                                 PageLoader& loader,
                                 llfs::PinPageToJob pin_pages_to_job,
                                 i32 min_pivot_i = 0) noexcept
      : SegmentedLevelScanner{node,
                              level,
                              loader,
                              pin_pages_to_job,
                              this->Super::self_contained_status_,
                              min_pivot_i}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Seq methods

  Optional<Item> peek();

  Optional<Item> next();

  Status status() const
  {
    return this->status_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Methods to access internal state

  usize get_pivot_index() const
  {
    return this->pivot_i_;
  }

  usize get_segment_index() const
  {
    return this->segment_i_;
  }

  /** \brief Returns the index of the next unconsumed item within the current segment; this may not
   * be accurate if called after this->next(); instead, call this->peek() and then this function.
   */
  usize get_item_index() const
  {
    return this->item_i_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Optional<Item> peek_next_impl(bool advance);

  void advance_segment();

  void advance_to_pivot(usize target_pivot_i,
                        const Segment& segment,
                        const PackedLeafPage& leaf_page);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  Node* node_;
  Level* level_;
  PageLoader* loader_;
  llfs::PinPageToJob pin_pages_to_job_;
  Status& status_;
  PinnedPageT pinned_leaf_;
  usize segment_i_;
  i32 min_pivot_i_;
  i32 pivot_i_;
  usize item_i_;
  bool needs_load_;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline /*explicit*/ SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::SegmentedLevelScanner(
    Node& node,
    Level& level,
    PageLoader& loader,
    llfs::PinPageToJob pin_pages_to_job,
    Status& status,
    i32 min_pivot_i) noexcept
    : node_{std::addressof(node)}
    , level_{std::addressof(level)}
    , loader_{std::addressof(loader)}
    , pin_pages_to_job_{pin_pages_to_job}
    , status_{status}
    , pinned_leaf_{}
    , segment_i_{0}
    , min_pivot_i_{min_pivot_i}
    , pivot_i_{0}
    , item_i_{0}
    , needs_load_{true}
{
  // TODO [tastolfi 2025-03-10] prefetch_hint the first leaf?
  // TODO [tastolfi 2025-03-15] binary search to first segment active for the new minimum pivot?
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::peek() -> Optional<Item>
{
  return this->peek_next_impl(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::next() -> Optional<Item>
{
  return this->peek_next_impl(true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::peek_next_impl(bool advance)
    -> Optional<Item>
{
  // Errors are final; check the current status.
  //
  if (!this->status_.ok()) {
    return None;
  }

  // If the current segment is past the end, return None.
  //
  if (this->segment_i_ == this->level_->segment_count()) {
    return None;
  }

  const Segment* segment = std::addressof(this->level_->get_segment(this->segment_i_));

  u64 active_pivots = segment->get_active_pivots();
  BATT_CHECK_NE(active_pivots, 0) << "This segment should have been dropped!";

  // Make sure we have a leaf page loaded.
  //
  if (this->needs_load_) {
    this->needs_load_ = false;

    // Skip ahead to the next segment that is active at or past the minimum pivot.
    //
    while (last_bit(active_pivots) < this->min_pivot_i_) {
      ++this->segment_i_;
      if (this->segment_i_ == this->level_->segment_count()) {
        return None;
      }
      segment = std::addressof(this->level_->get_segment(this->segment_i_));
      active_pivots = segment->get_active_pivots();
    }

    // Try to load the page for this segment.
    //
    StatusOr<PinnedPageT> loaded_page =
        segment->load_leaf_page(*this->loader_, this->pin_pages_to_job_);

    if (!loaded_page.ok()) {
      this->status_ = loaded_page.status();
      VLOG(1) << "Failed to load page: " << BATT_INSPECT(loaded_page.status())
              << BATT_INSPECT((int)this->pin_pages_to_job_);
      return None;
    }

    this->pinned_leaf_ = std::move(*loaded_page);

    i32 target_pivot_i = std::max(first_bit(active_pivots), this->min_pivot_i_);
    while (target_pivot_i < (i32)this->node_->pivot_count() &&
           !get_bit(active_pivots, target_pivot_i)) {
      ++target_pivot_i;
    }

    this->advance_to_pivot(target_pivot_i,
                           *segment,
                           PackedLeafPage::view_of(this->pinned_leaf_.get_page_buffer()));
  }

  //----- --- -- -  -  -   -
  // Return the slice containing all items up to the next gap.

  const PackedLeafPage& leaf_page = PackedLeafPage::view_of(this->pinned_leaf_.get_page_buffer());

  const i32 next_inactive_pivot_i = next_bit(~active_pivots, this->pivot_i_);
  const i32 next_flushed_pivot_i = next_bit(segment->get_flushed_pivots(), this->pivot_i_);
  const i32 next_gap_pivot_i = std::min(next_inactive_pivot_i, next_flushed_pivot_i);

  const usize begin_i = this->item_i_;

  const usize end_i = [&]() -> usize {
    const KeyView gap_pivot_key = this->node_->get_pivot_key(next_gap_pivot_i);

    // The end of the next slice is the position of the gap pivot key's lower bound.
    //
    return std::distance(leaf_page.items_begin(),  //
                         leaf_page.lower_bound(gap_pivot_key));
  }();

  if (advance) {
    if (next_gap_pivot_i == next_inactive_pivot_i) {
      const usize next_pivot_i =
          (next_inactive_pivot_i < 64) ? next_bit(active_pivots, next_inactive_pivot_i) : 64;

      if (next_pivot_i < this->node_->pivot_count()) {
        this->advance_to_pivot(next_pivot_i, *segment, leaf_page);
      } else {
        this->advance_segment();
      }
    } else {
      this->pivot_i_ = next_flushed_pivot_i;

      BATT_CHECK_LT(this->pivot_i_, this->node_->pivot_count())
          << BATT_INSPECT(next_inactive_pivot_i) << BATT_INSPECT(next_gap_pivot_i);

      this->item_i_ = segment->get_flushed_item_upper_bound(*this->level_, next_flushed_pivot_i);

      BATT_CHECK_LT(this->item_i_, leaf_page.key_count);
    }
  }

  return EditSlice{as_slice(leaf_page.items_begin() + begin_i,  //
                            leaf_page.items_begin() + end_i)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline void SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::advance_segment()
{
  ++this->segment_i_;
  this->needs_load_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline void SegmentedLevelScanner<NodeT, LevelT, PageLoaderT>::advance_to_pivot(
    usize target_pivot_i,
    const Segment& segment,
    const PackedLeafPage& leaf_page)
{
  BATT_CHECK_LT(target_pivot_i, this->node_->pivot_count());

  const auto old_pivot_i = this->pivot_i_;

  this->pivot_i_ = target_pivot_i;

  const KeyView pivot_lower_bound_key = this->node_->get_pivot_key(this->pivot_i_);

  const usize pivot_lower_bound = std::distance(leaf_page.items_begin(),  //
                                                leaf_page.lower_bound(pivot_lower_bound_key));

  const usize flushed_upper_bound =
      segment.get_flushed_item_upper_bound(*this->level_, this->pivot_i_);

  if (flushed_upper_bound != 0) {
    BATT_CHECK_GT(flushed_upper_bound, pivot_lower_bound)
        << BATT_INSPECT((void*)&segment) << BATT_INSPECT(this->pivot_i_)
        << BATT_INSPECT(old_pivot_i) << BATT_INSPECT_STR(pivot_lower_bound_key);

    this->item_i_ = flushed_upper_bound;
  } else {
    this->item_i_ = pivot_lower_bound;
  }
}

}  // namespace turtle_kv
