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
 *  segment->live_lower_bound(Level& level, u32 item_i) -> u32
 *    - Returns the position of the next live (non flushed item) in the segment at or after item_i
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
                                 llfs::PageCacheOvercommit& overcommit,
                                 Status& status,
                                 i32 min_pivot_i = 0,
                                 Optional<KeyView> min_key = None) noexcept;

  explicit SegmentedLevelScanner(Node& node,
                                 Level& level,
                                 PageLoader& loader,
                                 llfs::PinPageToJob pin_pages_to_job,
                                 llfs::PageCacheOvercommit& overcommit,
                                 i32 min_pivot_i = 0,
                                 Optional<KeyView> min_key = None) noexcept
      : SegmentedLevelScanner{node,
                              level,
                              loader,
                              pin_pages_to_job,
                              overcommit,
                              this->Super::self_contained_status_,
                              min_pivot_i,
                              min_key}
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
  llfs::PageCacheOvercommit& overcommit_;
  Status& status_;
  PinnedPageT pinned_leaf_;
  Optional<KeyView> min_key_;
  usize segment_i_;
  i32 min_pivot_i_;
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
    llfs::PageCacheOvercommit& overcommit,
    Status& status,
    i32 min_pivot_i,
    Optional<KeyView> min_key) noexcept
    : node_{std::addressof(node)}
    , level_{std::addressof(level)}
    , loader_{std::addressof(loader)}
    , pin_pages_to_job_{pin_pages_to_job}
    , overcommit_{overcommit}
    , status_{status}
    , pinned_leaf_{}
    , min_key_{min_key}
    , segment_i_{0}
    , min_pivot_i_{min_pivot_i}
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
        segment->load_leaf_page(*this->loader_, this->pin_pages_to_job_, this->overcommit_);

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

  const usize begin_i = this->item_i_;
  u32 end_i = begin_i;

  if (begin_i >= leaf_page.key_count) {
    this->advance_segment();
    return EditSlice{as_slice(leaf_page.items_begin() + begin_i,  //
                              leaf_page.items_begin() + end_i)};
  }

  Interval<u32> live_range = segment->get_live_item_range(
      *this->level_,
      Interval<u32>{BATT_CHECKED_CAST(u32, begin_i), BATT_CHECKED_CAST(u32, leaf_page.key_count)});

  BATT_CHECK(!live_range.empty());
  end_i = live_range.upper_bound;

  if (advance) {
    if (end_i >= leaf_page.key_count) {
      this->advance_segment();
    } else {
      u32 next_live_item = segment->live_lower_bound(*this->level_, end_i);
      if (next_live_item >= leaf_page.key_count) {
        this->advance_segment();
      } else {
        this->item_i_ = next_live_item;
      }
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

  const KeyView pivot_lower_bound_key = this->node_->get_pivot_key(target_pivot_i);

  const KeyView lower_bound_key = this->min_key_
                                      ? std::max(*this->min_key_, pivot_lower_bound_key, KeyOrder{})
                                      : pivot_lower_bound_key;

  const usize lower_bound_i = std::distance(leaf_page.items_begin(),  //
                                            leaf_page.lower_bound(lower_bound_key));

  if (lower_bound_i >= leaf_page.key_count) {
    this->item_i_ = leaf_page.key_count;
    return;
  }

  this->item_i_ = segment.live_lower_bound(*this->level_, BATT_CHECKED_CAST(u32, lower_bound_i));
}

}  // namespace turtle_kv
