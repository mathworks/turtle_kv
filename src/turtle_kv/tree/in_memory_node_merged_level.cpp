#include <turtle_kv/tree/in_memory_node_merged_level.hpp>
//

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>

#include <turtle_kv/core/algo/split_parts.hpp>

#include <batteries/case_of.hpp>

namespace turtle_kv {

using Level = InMemoryNodeLevel;
using EmptyLevel = InMemoryNodeEmptyLevel;
using MergedLevel = InMemoryNodeMergedLevel;
using SegmentedLevel = InMemoryNodeSegmentedLevel;
using HybridLevel = InMemoryNodeHybridLevel;
using Segment = InMemoryNodeSegment;


//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<EditSlice> InMemoryNodeMergedLevel::to_boxed_seq(const InMemoryNode& node,
                                                          i32 min_pivot_i) const
{
  return this->result_set.live_edit_slices(node.get_pivot_key(min_pivot_i));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Level InMemoryNodeMergedLevel::merge(Level&& sibling_level, usize node_pivot_count) &&
{
  return batt::case_of(
      sibling_level,
      [&](EmptyLevel&) -> Level {
        return *this;
      },
      [&](MergedLevel& right_merged_level) -> Level {
        return this->concat(std::move(right_merged_level));
      },
      [&](auto& right_segmented_or_hybrid_level) -> Level {
        HybridLevel new_hybrid_level;
        new_hybrid_level.add_new_sub_level(std::move(*this));

        new_hybrid_level.add_new_sub_level(std::move(right_segmented_or_hybrid_level),
                                           node_pivot_count);

        return new_hybrid_level;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> InMemoryNodeMergedLevel::start_serialize(const InMemoryNode& node,
                                                         TreeSerializeContext& context)
{
  batt::RunningTotal running_total =
      compute_running_total(context.worker_pool(), this->result_set, DecayToItem<false>{});

  SplitParts page_parts = split_parts(running_total,
                                      MinPartSize{context.tree_options().flush_size() / 4},
                                      MaxPartSize{context.tree_options().flush_size()},
                                      MaxItemSize{context.tree_options().max_item_size()});

  BATT_CHECK_EQ(running_total.back() - running_total.front(), this->result_set.get_packed_size());

  auto filter_bits_per_key = context.tree_options().filter_bits_per_key();
  const bool overcommit_triggered = context.overcommit().is_triggered();
  llfs::PageSize filter_page_size = context.tree_options().filter_page_size();

  for (const Interval<usize>& part_extents : page_parts) {
    BATT_ASSIGN_OK_RESULT(
        TreeSerializeContext::BuildPageJobId id,
        context.async_build_page(
            context.tree_options().leaf_size(),
            packed_leaf_page_layout_id(),
            llfs::LruPriority{kNewLeafLruPriority},
            /*task_count=*/2,
            [this,
             &node,
             part_extents,
             filter_bits_per_key,
             overcommit_triggered,
             filter_page_size](
                TreeSerializeContext::BuildPageArgs args) -> TreeSerializeContext::PinPageToJobFn {
              //----- --- -- -  -  -   -
              const auto all_items_in_level = this->result_set.get();
              const auto items_in_this_page = batt::slice_range(all_items_in_level, part_extents);

              if (args.task_i == 0) {
                return build_leaf_page_in_job(node.tree_options.trie_index_reserve_size(),
                                              args.page_buffer,
                                              items_in_this_page);
              }
              BATT_CHECK_EQ(args.task_i, 1);

              return build_filter_for_leaf_in_job(batt::make_copy(args.filter_page_write_state),
                                                  args.page_cache,
                                                  overcommit_triggered,
                                                  filter_bits_per_key,
                                                  filter_page_size,
                                                  args.page_buffer.page_id(),
                                                  items_in_this_page);
            }));

    this->segment_future_ids_.emplace_back(id);
  }

  return page_parts.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<InMemoryNodeSegmentedLevel> InMemoryNodeMergedLevel::finish_serialize(
    const InMemoryNode& node,
    TreeSerializeContext& context)
{
  BATT_CHECK_EQ(node.tree_options.filter_bits_per_key(),
                context.tree_options().filter_bits_per_key());
  BATT_CHECK_EQ(node.tree_options.expected_items_per_leaf(),
                context.tree_options().expected_items_per_leaf());

  SegmentedLevel segmented_level;

  const usize pivot_count = node.pivot_count();
  const usize segment_count = this->segment_future_ids_.size();
  segmented_level.segments.resize(segment_count);

  for (usize segment_i = 0; segment_i < segment_count; ++segment_i) {
    Segment& segment = segmented_level.segments[segment_i];

    BATT_ASSIGN_OK_RESULT(llfs::PinnedPage pinned_leaf_page,
                          context.get_build_page_result(this->segment_future_ids_[segment_i]));

    segment.page_id_slot.page_id = pinned_leaf_page.page_id();
    segment.active_pivots.clear();

    const PackedLeafPage& leaf_page = PackedLeafPage::view_of(pinned_leaf_page);

    for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
      const Interval<KeyView> pivot_key_range = in_node(node).get_pivot_key_range(pivot_i);

      const Interval<const PackedKeyValue*> pivot_range_in_leaf{
          .lower_bound = leaf_page.lower_bound(pivot_key_range.lower_bound),
          .upper_bound = leaf_page.lower_bound(pivot_key_range.upper_bound),
      };

      segment.set_pivot_active(pivot_i, !pivot_range_in_leaf.empty());
    }

    segment.check_invariants(__FILE__, __LINE__);
  }

  return {std::move(segmented_level)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNodeMergedLevel::dump() const
{
  return [this](std::ostream& out) {
    out << "MergedLevel{" << this->result_set.debug_dump("    ") << "\n}";
  };
}

}  // namespace turtle_kv
