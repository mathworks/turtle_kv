#include <turtle_kv/tree/in_memory_node_hybrid_level.hpp>
//

#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/key_query.hpp>

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
void InMemoryNodeHybridLevel::push_front_pivots(usize node_pivot_count)
{
  for (auto& sub_level : this->sub_levels) {
    if (batt::is_case<SegmentedLevel>(sub_level)) {
      SegmentedLevel& segmented_sub_level = std::get<SegmentedLevel>(sub_level);
      segmented_sub_level.push_front_pivots(node_pivot_count);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeHybridLevel::set_pivot_items_flushed(const InMemoryNode& node,
                                                      BatchUpdateContext& update_context,
                                                      usize pivot_i,
                                                      const CInterval<KeyView>& flush_key_crange,
                                                      Status segment_load_status)
{
  for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end();) {
    bool sub_level_is_now_empty = false;

    batt::case_of(
        *iter,
        [&](MergedLevel& merged_sub_level) {
          sub_level_is_now_empty = merged_sub_level.set_items_flushed(flush_key_crange);
        },
        [&](SegmentedLevel& segmented_sub_level) {
          sub_level_is_now_empty = segmented_sub_level.set_pivot_items_flushed(node,
                                                                               update_context,
                                                                               pivot_i,
                                                                               flush_key_crange,
                                                                               segment_load_status);
        });

    if (sub_level_is_now_empty) {
      iter = this->sub_levels.erase(iter);
    } else {
      ++iter;
    }
  }

  return this->empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNodeHybridLevel::set_pivot_completely_flushed(usize pivot_i,
                                                           const Interval<KeyView>& pivot_key_range)
{
  for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end();) {
    bool sub_level_is_now_empty = false;

    batt::case_of(
        *iter,
        [&](MergedLevel& merged_sub_level) {
          sub_level_is_now_empty = merged_sub_level.set_items_flushed(pivot_key_range);
        },
        [&](SegmentedLevel& segmented_sub_level) {
          sub_level_is_now_empty = segmented_sub_level.set_pivot_completely_flushed(pivot_i);
        });

    if (sub_level_is_now_empty) {
      iter = this->sub_levels.erase(iter);
    } else {
      ++iter;
    }
  }

  return this->empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<EditSlice> InMemoryNodeHybridLevel::to_boxed_seq(const InMemoryNode& node,
                                                          BatchUpdateContext& update_context,
                                                          Status& segment_load_status,
                                                          i32 min_pivot_i,
                                                          bool only_pivot,
                                                          Optional<KeyView> min_key) const
{
  return as_seq(this->sub_levels) |
         seq::map([&](const std::variant<MergedLevel, SegmentedLevel>& v) -> BoxedSeq<EditSlice> {
           return batt::case_of(
               v,
               [&](const MergedLevel& merged_level) -> BoxedSeq<EditSlice> {
                 return merged_level.to_boxed_seq(node, min_pivot_i);
               },
               [&](const SegmentedLevel& segmented_level) -> BoxedSeq<EditSlice> {
                 return segmented_level.to_boxed_seq(node,
                                                     update_context,
                                                     segment_load_status,
                                                     min_pivot_i,
                                                     only_pivot,
                                                     min_key);
               });
         }) |
         seq::flatten() | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNodeHybridLevel::segment_count(const TreeOptions& tree_options) const
{
  usize total = 0;

  for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end(); ++iter) {
    total += batt::case_of(
        *iter,
        [&](const MergedLevel& merged_sub_level) -> usize {
          return merged_sub_level.estimate_segment_count(tree_options);
        },
        [&](const SegmentedLevel& segmented_sub_level) -> usize {
          return segmented_sub_level.segment_count();
        });
  }

  return total;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeHybridLevel::drop_before_pivot(i32 pivot_i,
                                                const KeyView& pivot_key,
                                                llfs::PageLoader& page_loader,
                                                const TreeOptions& tree_options)
{
  for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end(); ++iter) {
    batt::case_of(*iter, [&](auto& sub_level) {
      sub_level.drop_before_pivot(pivot_i, pivot_key, page_loader, tree_options);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeHybridLevel::drop_after_pivot(i32 pivot_i,
                                               const KeyView& pivot_key,
                                               llfs::PageLoader& page_loader,
                                               const TreeOptions& tree_options)
{
  for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end(); ++iter) {
    batt::case_of(*iter, [&](auto& sub_level) {
      sub_level.drop_after_pivot(pivot_i, pivot_key, page_loader, tree_options);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNodeHybridLevel::split_pivot(InMemoryNode& node,
                                            BatchUpdateContext& update_context,
                                            i32 pivot_i,
                                            const Interval<KeyView>& pivot_key_range,
                                            const KeyView& sibling_min_key)
{
  for (auto& sub_level : this->sub_levels) {
    if (batt::is_case<SegmentedLevel>(sub_level)) {
      SegmentedLevel& segmented_sub_level = std::get<SegmentedLevel>(sub_level);
      BATT_REQUIRE_OK(in_segmented_level(node,
                                         segmented_sub_level,
                                         update_context.page_loader,
                                         update_context.overcommit)  //
                          .split_pivot(pivot_i, pivot_key_range, sibling_min_key));
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNodeHybridLevel::merge_pivots(InMemoryNode& node,
                                           BatchUpdateContext& update_context,
                                           i32 left_pivot_i,
                                           i32 right_pivot_i)
{
  for (auto& sub_level : this->sub_levels) {
    if (batt::is_case<SegmentedLevel>(sub_level)) {
      SegmentedLevel& segmented_sub_level = std::get<SegmentedLevel>(sub_level);
      in_segmented_level(*this,
                         segmented_sub_level,
                         update_context.page_loader,
                         update_context.overcommit)
          .merge_pivots(left_pivot_i, right_pivot_i);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> InMemoryNodeHybridLevel::find_key(const InMemoryNode& node,
                                                      KeyQuery& query,
                                                      i32 key_pivot_i) const
{
  StatusOr<ValueView> result{Status{batt::StatusCode::kNotFound}};

  // TODO [vsilai 2026-04-28]: Implement a way to avoid iterating through all sub-levels.
  //
  for (const auto& sub_level : this->sub_levels) {
    result = batt::case_of(
        sub_level,
        [&](const MergedLevel& merged_sub_level) -> StatusOr<ValueView> {
          return merged_sub_level.find_key(query.key());
        },
        [&](const SegmentedLevel& segmented_sub_level) -> StatusOr<ValueView> {
          return segmented_sub_level.find_key(node, query, key_pivot_i);
        });

    if (result.ok() || result.status() != batt::StatusCode::kNotFound) {
      return result;
    }
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Level InMemoryNodeHybridLevel::merge(Level&& sibling_level, usize node_pivot_count) &&
{
  batt::case_of(
      sibling_level,
      [&](EmptyLevel&) {
      },
      [&](MergedLevel& right_merged_level) {
        this->add_new_sub_level(std::move(right_merged_level));
      },
      [&](SegmentedLevel& right_segmented_level) {
        this->deduplicate_and_add_sub_level(std::move(right_segmented_level), node_pivot_count);
      },
      [&](HybridLevel& right_hybrid_level) {
        this->deduplicate_and_add_sub_level(std::move(right_hybrid_level), node_pivot_count);
      });

  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> InMemoryNodeHybridLevel::start_serialize(const InMemoryNode& node,
                                                         TreeSerializeContext& context)
{
  usize total_segment_count = 0;

  for (auto& sub_level : this->sub_levels) {
    StatusOr<usize> sub_level_segment_count = batt::case_of(
        sub_level,
        [&](MergedLevel& merged_sub_level) -> StatusOr<usize> {
          BATT_ASSIGN_OK_RESULT(usize sub_level_total,
                                merged_sub_level.start_serialize(node, context));
          return sub_level_total;
        },
        [&](SegmentedLevel& segmented_sub_level) -> StatusOr<usize> {
          return segmented_sub_level.segment_count();
        });

    BATT_REQUIRE_OK(sub_level_segment_count);

    total_segment_count += *sub_level_segment_count;
  }

  return total_segment_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<InMemoryNodeSegmentedLevel> InMemoryNodeHybridLevel::finish_serialize(
    const InMemoryNode& node,
    TreeSerializeContext& context)
{
  static InMemoryNode::Metrics& r_metrics = InMemoryNode::metrics();

  SegmentedLevel final_segmented_level;

  for (auto& sub_level : this->sub_levels) {
    r_metrics.serialized_nonempty_level_count.add(1);

    StatusOr<SegmentedLevel> new_segmented_sub_level = batt::case_of(
        sub_level,
        [&](MergedLevel& merged_sub_level) -> StatusOr<SegmentedLevel> {
          return merged_sub_level.finish_serialize(node, context);
        },
        [&](SegmentedLevel& segmented_sub_level) -> StatusOr<SegmentedLevel> {
          r_metrics.serialized_buffer_segment_count.add(segmented_sub_level.segment_count());

          return {std::move(segmented_sub_level)};
        });

    BATT_REQUIRE_OK(new_segmented_sub_level);

    final_segmented_level.segments.insert(
        final_segmented_level.segments.end(),
        std::make_move_iterator(new_segmented_sub_level->segments.begin()),
        std::make_move_iterator(new_segmented_sub_level->segments.end()));
  }

  return {std::move(final_segmented_level)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNodeHybridLevel::dump() const
{
  return [this](std::ostream& out) {
    out << "HybridLevel{\n";
    for (auto iter = this->sub_levels.begin(); iter != this->sub_levels.end(); ++iter) {
      batt::case_of(*iter, [&out](const auto& sub_level) {
        out << "  " << sub_level.dump() << ",\n";
      });
    }
    out << "  }";
  };
}

}  // namespace turtle_kv
