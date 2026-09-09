#include <turtle_kv/tree/in_memory_node_segmented_level.hpp>
//

#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/algo/segments.hpp>
#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/in_memory_node_hybrid_level.hpp>
#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/util/piecewise_filter.ipp>

#include <batteries/case_of.hpp>

#include <type_traits>

namespace turtle_kv {

using Level = InMemoryNodeLevel;
using EmptyLevel = InMemoryNodeEmptyLevel;
using MergedLevel = InMemoryNodeMergedLevel;
using SegmentedLevel = InMemoryNodeSegmentedLevel;
using HybridLevel = InMemoryNodeHybridLevel;
using Segment = InMemoryNodeSegment;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// InMemoryNodeSegment
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> InMemoryNodeSegment::load_leaf_page(
    llfs::PageLoader& page_loader,
    llfs::PinPageToJob pin_page_to_job,
    llfs::PageCacheOvercommit& overcommit) const
{
  return this->page_id_slot.load_through(page_loader,
                                         llfs::PageLoadOptions{
                                             LeafPageView::page_layout_id(),
                                             pin_page_to_job,
                                             llfs::OkIfNotFound{false},
                                             llfs::LruPriority{kLeafLruPriority},
                                             overcommit,
                                         });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegment::check_invariants(const char* file, int line) const
{
  BATT_CHECK(this->filter.check_invariants()) << BATT_INSPECT(file) << BATT_INSPECT(line);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegment::insert_pivot(i32 pivot_i, bool is_active)
{
  this->check_invariants(__FILE__, __LINE__);
  auto on_scope_exit = batt::finally([&] {
    this->check_invariants(__FILE__, __LINE__);
  });

  this->active_pivots.insert(pivot_i, is_active);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegment::remove_pivot(i32 pivot_i)
{
  this->check_invariants(__FILE__, __LINE__);
  auto on_scope_exit = batt::finally([&] {
    this->check_invariants(__FILE__, __LINE__);
  });

  this->active_pivots.remove(pivot_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegment::pop_front_pivots(i32 count)
{
  BATT_CHECK_LT(count, 64);

  this->active_pivots.pop_front_pivots(count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeSegment::is_inactive() const
{
  const bool inactive = this->active_pivots.is_empty();
  if (inactive) {
    Slice<const Interval<u32>> live_ranges = this->filter.live();
    BATT_CHECK_EQ(live_ranges.size(), 1) << BATT_INSPECT(live_ranges);
    BATT_CHECK_EQ(live_ranges[0].upper_bound, PiecewiseFilter<u32>::kMaxUpperBound)
        << BATT_INSPECT(live_ranges);
  }
  return inactive;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNodeSegment::num_cut_points() const
{
  Slice<const Interval<u32>> live_ranges = this->filter.live();

  // If the first element is live, don't include 0 as a cut point, only include the
  // upper bound.
  //
  bool first_element_live = !live_ranges.empty() &&
                            live_ranges.front().lower_bound == PiecewiseFilter<u32>::kMinLowerBound;
  bool ends_at_max = !live_ranges.empty() &&
                     live_ranges.back().upper_bound == PiecewiseFilter<u32>::kMaxUpperBound;

  return live_ranges.size() * 2 - (first_element_live ? 1 : 0) - (ends_at_max ? 1 : 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeSegment::deduplicate(const InMemoryNodeSegment& other)
{
  if (this->page_id_slot.page_id != other.page_id_slot.page_id) {
    return false;
  }
  this->active_pivots |= other.active_pivots;
  this->filter.merge(other.filter);
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNodeSegment::dump(bool multi_line) const
{
  return [this, multi_line](std::ostream& out) {
    if (multi_line) {
      out << "Segment:" << std::endl
          << "   active=" << this->active_pivots.printable() << std::endl
          << "   filter=" << this->filter.dump() << std::endl
          << std::endl;
    } else {
      out << "Segment{.active=" << this->active_pivots.printable()
          << ", .filter=" << this->filter.dump() << ",}";
    }
  };
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// InMemoryNodeSegmentedLevel
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::drop_segment(usize i)
{
  this->segments.erase(this->segments.begin() + i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::drop_pivot_range(const Interval<i32>& pivot_i_range,
                                                  const Interval<KeyView>& pivot_key_range,
                                                  llfs::PageLoader& page_loader,
                                                  const TreeOptions& tree_options)
{
  for (Segment& segment : this->segments) {
    BATT_CHECK_OK(in_segment(segment).drop_pivot_range(pivot_i_range,
                                                       pivot_key_range,
                                                       page_loader,
                                                       tree_options));

    if (pivot_i_range.lower_bound == 0) {
      segment.pop_front_pivots(pivot_i_range.upper_bound);
    }
  }

  this->segments.erase(std::remove_if(this->segments.begin(),
                                      this->segments.end(),
                                      [](const Segment& segment) {
                                        return segment.is_inactive();
                                      }),
                       this->segments.end());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::drop_before_pivot(i32 pivot_i,
                                                   const KeyView& pivot_key,
                                                   llfs::PageLoader& page_loader,
                                                   const TreeOptions& tree_options)
{
  this->drop_pivot_range((Interval<i32>{0, pivot_i}),
                         (Interval<KeyView>{global_min_key(), pivot_key}),
                         page_loader,
                         tree_options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::drop_after_pivot(i32 pivot_i,
                                                  const KeyView& pivot_key,
                                                  llfs::PageLoader& page_loader,
                                                  const TreeOptions& tree_options)
{
  this->drop_pivot_range((Interval<i32>{pivot_i, InMemoryNode::kMaxTempPivots}),
                         (Interval<KeyView>{pivot_key, global_max_key()}),
                         page_loader,
                         tree_options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeSegmentedLevel::is_pivot_active(i32 pivot_i) const
{
  for (const Segment& segment : this->segments) {
    if (segment.is_pivot_active(pivot_i)) {
      return true;
    }
  }
  return false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::check_items_sorted(const InMemoryNode& node,
                                                    llfs::PageLoader& page_loader) const
{
  SegmentedLevelScanner<const InMemoryNode, const SegmentedLevel, llfs::PageLoader> scanner{
      node,
      *this,
      page_loader,
      llfs::PinPageToJob::kDefault,
      llfs::PageCacheOvercommit::not_allowed(),
  };

  Optional<std::string> prev_slice_max_key;
  usize item_i = 0;

  for (;;) {
    Optional<EditSlice> edit_slice = scanner.next();

    if (!edit_slice) {
      break;
    }

    batt::case_of(*edit_slice, [&](const auto& slice_impl) {
      if (slice_impl.empty()) {
        return;
      }
      if (prev_slice_max_key) {
        BATT_CHECK_LE(*prev_slice_max_key, get_key(slice_impl.front()));
      }

      prev_slice_max_key = std::string{get_key(slice_impl.back())};
      item_i += slice_impl.size();
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<EditSlice> InMemoryNodeSegmentedLevel::to_boxed_seq(const InMemoryNode& node,
                                                             BatchUpdateContext& update_context,
                                                             Status& segment_load_status,
                                                             i32 min_pivot_i,
                                                             bool only_pivot,
                                                             Optional<KeyView> min_key) const
{
  if (only_pivot && !this->is_pivot_active(min_pivot_i)) {
    return seq::Empty<EditSlice>{}  //
           | seq::boxed();
  }
  return SegmentedLevelScanner<const InMemoryNode, const SegmentedLevel, llfs::PageLoader>{
             node,
             *this,
             update_context.page_loader,
             llfs::PinPageToJob::kDefault,
             update_context.overcommit,
             segment_load_status,
             min_pivot_i,
             min_key}  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeSegmentedLevel::set_pivot_items_flushed(const InMemoryNode& node,
                                                         BatchUpdateContext& update_context,
                                                         usize pivot_i,
                                                         const CInterval<KeyView>& flush_key_crange,
                                                         Status& segment_load_status)
{
  segment_load_status.Update(
      in_segmented_level(node, *this, update_context.page_loader, update_context.overcommit)
          .drop_key_range(pivot_i, flush_key_crange.upper_bound));

  return this->empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeSegmentedLevel::set_pivot_completely_flushed(usize pivot_i)
{
  for (usize segment_i = 0; segment_i < this->segment_count();) {
    Segment& segment = this->get_segment(segment_i);

    segment.set_pivot_active(pivot_i, false);

    if (segment.is_inactive()) {
      this->drop_segment(segment_i);
    } else {
      ++segment_i;
    }
  }

  return this->empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNodeSegmentedLevel::segment_filter_cut_points() const
{
  usize n = 0;
  for (const Segment& segment : this->segments) {
    n += segment.num_cut_points();
  }
  return n;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeSegmentedLevel::push_front_pivots(usize node_pivot_count)
{
  for (usize segment_i = 0; segment_i < this->segment_count(); ++segment_i) {
    Segment& segment = this->segments[segment_i];
    segment.active_pivots.push_front_pivots(node_pivot_count);
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> InMemoryNodeSegmentedLevel::find_key(const InMemoryNode& node,
                                                         KeyQuery& query,
                                                         i32 key_pivot_i) const
{
  return in_segmented_level(node, *this, *query.page_loader, query.overcommit())
      .find_key(key_pivot_i, query);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Level InMemoryNodeSegmentedLevel::merge(Level&& sibling_level, usize node_pivot_count) &&
{
  return batt::case_of(
      sibling_level,
      [&](EmptyLevel&) -> Level {
        return *this;
      },
      [&](MergedLevel& right_merged_level) -> Level {
        HybridLevel new_hybrid_level;
        new_hybrid_level.add_new_sub_level(std::move(*this));
        new_hybrid_level.add_new_sub_level(std::move(right_merged_level));

        return new_hybrid_level;
      },
      [&](SegmentedLevel& right_segmented_level) -> Level {
        // We need to find potential duplicate segments that have arisen from a previous
        // split. For these duplicates, we need to merge their metadata.
        //
        this->deduplicate(right_segmented_level, node_pivot_count);

        // Concatenate the two segment vectors.
        //
        this->segments.insert(this->segments.end(),
                              std::make_move_iterator(right_segmented_level.segments.begin()),
                              std::make_move_iterator(right_segmented_level.segments.end()));

        return *this;
      },
      [&](HybridLevel& right_hybrid_level) -> Level {
        this->deduplicate(right_hybrid_level, node_pivot_count);

        HybridLevel new_hybrid_level;
        new_hybrid_level.add_new_sub_level(std::move(*this));
        new_hybrid_level.add_new_sub_level(std::move(right_hybrid_level));

        return new_hybrid_level;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
void InMemoryNodeSegmentedLevel::deduplicate(T& right_level, usize push_pivot_count)
{
  right_level.push_front_pivots(push_pivot_count);

  BATT_CHECK_NOT_NULLPTR(this->back());
  BATT_CHECK_NOT_NULLPTR(right_level.front());

  if constexpr (std::is_same_v<T, SegmentedLevel>) {
    // Base case: two segmented levels. If the two segements at the seam are the same, discard one.
    //
    if (right_level.front()->deduplicate(*this->back())) {
      this->segments.pop_back();
    }
  } else {
    // If we have a HybridLevel, only try to deduplicate if the first sub level is
    // a SegmentedLevel.
    //
    if (batt::is_case<SegmentedLevel>(*right_level.front())) {
      auto& right_sub_level = std::get<SegmentedLevel>(*right_level.front());
      this->deduplicate(right_sub_level);
    }
  }
}

template void InMemoryNodeSegmentedLevel::deduplicate(SegmentedLevel&, usize);
template void InMemoryNodeSegmentedLevel::deduplicate(HybridLevel&, usize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNodeSegmentedLevel::dump() const
{
  return [this](std::ostream& out) {
    out << "SegmentedLevel{\n";
    for (const Segment& segment : this->segments) {
      out << "    " << segment.dump(/*multi_line=*/false) << ",\n";
    }
    out << "  }";
  };
}

}  // namespace turtle_kv
