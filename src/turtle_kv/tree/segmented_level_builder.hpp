#pragma once

#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_buffer.hpp>
#include <llfs/page_id.hpp>

#include <batteries/assert.hpp>

#include <iterator>
#include <memory>

namespace turtle_kv {

template <typename NodeT,
          typename LevelT,
          typename SegmentT,
          typename PageLoaderT,
          typename PinnedPageT>
class SegmentedLevelBuilder
{
 public:
  using Self = SegmentedLevelBuilder;

  explicit SegmentedLevelBuilder(const NodeT& node, LevelT& level) noexcept
      : node_{node}
      , level_{level}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename PinnedPageRangeT>
  Self& add_segments_for_leaf_pages(const PinnedPageRangeT& pinned_pages)
  {
    for (const PinnedPageT& pinned_page : pinned_pages) {
      this->add_segment(pinned_page);
    }
    return *this;
  }

  Self& add_segment(const PinnedPageT& pinned_page)
  {
    llfs::PageId page_id = pinned_page.page_id();
    std::shared_ptr<const llfs::PageBuffer> page_buffer = pinned_page.get_page_buffer();
    SegmentT& segment = this->level_.append_segment();

    // Reference the serialized leaf corresponding to this segment.
    //
    auto& leaf_view = PackedLeafPage::view_of(page_buffer);

    // Initialize the segment structure.
    //
    segment.set_page_id(page_id);
    segment.clear_active_pivots();
    segment.clear_flushed_pivots();

    // The goal of this inner loop is to determine the set of pivots for which each segment is
    // active (i.e., it contains keys in the range of that pivot).
    //
    for (;;) {
      // We do this by searching the leaf to find the lower-bound (inclusive) position within the
      // leaf of each pivot key...
      //
      auto pivot_range_begin = leaf_view.lower_bound(this->node_.get_pivot_key(this->pivot_i_));

      // ...and also the next pivot key.  If the two positions are equal, that means there can be
      // *no* keys in that pivot's range within the leaf.
      //
      auto pivot_range_end = [&] {
        // Check for whether this is the last pivot; in this case, we can't use the "next" pivot
        // key since there is no "next."
        //
        if (this->pivot_i_ + 1 == this->node_.pivot_count()) {
          return leaf_view.items_end();
        }
        return leaf_view.lower_bound(this->node_.get_pivot_key(this->pivot_i_ + 1));
      }();

      // Do a few quick sanity checks on pivot_range_begin, pivot_range_end.
      //
      {
        if (pivot_range_begin != leaf_view.items_begin()) {
          KeyView begin_prev_key = get_key(*std::prev(pivot_range_begin));
          BATT_CHECK_LT(begin_prev_key, this->node_.get_pivot_key(this->pivot_i_));
        }
        if (pivot_range_begin != leaf_view.items_end()) {
          KeyView begin_key = get_key(*pivot_range_begin);
          BATT_CHECK_GE(begin_key, this->node_.get_pivot_key(this->pivot_i_));
        }
        if (this->pivot_i_ + 1 != this->node_.pivot_count()) {
          if (pivot_range_end != leaf_view.items_begin()) {
            KeyView end_prev_key = get_key(*std::prev(pivot_range_end));
            BATT_CHECK_LT(end_prev_key, this->node_.get_pivot_key(this->pivot_i_ + 1));
          }
          if (pivot_range_end != leaf_view.items_end()) {
            KeyView end_key = get_key(*pivot_range_end);
            BATT_CHECK_GE(end_key, this->node_.get_pivot_key(this->pivot_i_ + 1));
          }
        }
      }

      // Now [pivot_range_begin, pivot_range_end) is the interval within the leaf of keys in the
      // range of pivot_i.
      //
      const usize live_items_count = std::distance(pivot_range_begin, pivot_range_end);
      segment.set_pivot_items_count(this->pivot_i_, live_items_count);

      // If the matched range reaches the end of the leaf, then there can be no more pivots active
      // for this segment.
      //
      if (pivot_range_end == leaf_view.items_end()) {
        break;
      }

      // If we are at the end of the pivots, we are done.
      //
      if (this->pivot_i_ + 1 == this->node_.pivot_count()) {
        break;
      }

      // Move to the next pivot.
      //
      ++this->pivot_i_;
    }

    return *this;
  }

 private:
  const NodeT& node_;
  LevelT& level_;
  usize pivot_i_ = 0;
};

}  // namespace turtle_kv
