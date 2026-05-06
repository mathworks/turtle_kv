#pragma once

#include <turtle_kv/tree/in_memory_node_merged_level.hpp>
#include <turtle_kv/tree/in_memory_node_segmented_level.hpp>
#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>

#include <vector>

namespace turtle_kv {

struct InMemoryNode;
struct BatchUpdateContext;
class KeyQuery;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct InMemoryNodeHybridLevel {
  using SubLevel = std::variant<InMemoryNodeMergedLevel, InMemoryNodeSegmentedLevel>;

  SmallVec<SubLevel, 2> sub_levels;

  bool empty() const
  {
    return this->sub_levels.empty();
  }

  Slice<const SubLevel> get_levels() const
  {
    return as_const_slice(this->sub_levels);
  }

  SubLevel* front()
  {
    if (this->empty()) {
      return nullptr;
    }

    return &this->sub_levels.front();
  }

  SubLevel* back()
  {
    if (this->empty()) {
      return nullptr;
    }

    return &this->sub_levels.back();
  }

  void add_new_sub_level(SubLevel&& level, usize push_pivot_count = 0)
  {
    if (push_pivot_count) {
      BATT_CHECK(batt::is_case<InMemoryNodeSegmentedLevel>(level));
      InMemoryNodeSegmentedLevel& segmented_sub_level = std::get<InMemoryNodeSegmentedLevel>(level);
      segmented_sub_level.push_front_pivots(push_pivot_count);
    }

    this->sub_levels.emplace_back(std::move(level));
  }

  void add_new_sub_level(InMemoryNodeHybridLevel&& other, usize push_pivot_count = 0)
  {
    if (push_pivot_count) {
      other.push_front_pivots(push_pivot_count);
    }

    this->sub_levels.insert(this->sub_levels.end(),
                            std::make_move_iterator(other.sub_levels.begin()),
                            std::make_move_iterator(other.sub_levels.end()));
  }

  void deduplicate_and_add_sub_level(InMemoryNodeSegmentedLevel&& right_level,
                                     usize push_pivot_count = 0)
  {
    right_level.push_front_pivots(push_pivot_count);

    BATT_CHECK_NOT_NULLPTR(this->back());
    BATT_CHECK_NOT_NULLPTR(right_level.front());

    if (batt::is_case<InMemoryNodeSegmentedLevel>(*this->back())) {
      InMemoryNodeSegmentedLevel& left_sub_level =
          std::get<InMemoryNodeSegmentedLevel>(*this->back());
      BATT_CHECK_NOT_NULLPTR(left_sub_level.back());

      left_sub_level.deduplicate(right_level);
    }

    this->add_new_sub_level(std::move(right_level));
  }

  void deduplicate_and_add_sub_level(InMemoryNodeHybridLevel&& right_level,
                                     usize push_pivot_count = 0)
  {
    right_level.push_front_pivots(push_pivot_count);

    BATT_CHECK_NOT_NULLPTR(this->back());
    BATT_CHECK_NOT_NULLPTR(right_level.front());

    if (batt::is_case<InMemoryNodeSegmentedLevel>(*this->back()) &&
        batt::is_case<InMemoryNodeSegmentedLevel>(*right_level.front())) {
      InMemoryNodeSegmentedLevel& left_sub_level =
          std::get<InMemoryNodeSegmentedLevel>(*this->back());
      BATT_CHECK_NOT_NULLPTR(left_sub_level.back());

      left_sub_level.deduplicate(right_level);
    }

    this->add_new_sub_level(std::move(right_level));
  }

  void push_front_pivots(usize node_pivot_count);

  bool set_pivot_items_flushed(const InMemoryNode& node,
                               BatchUpdateContext& update_context,
                               usize pivot_i,
                               const CInterval<KeyView>& flush_key_crange,
                               Status segment_load_status);

  bool set_pivot_completely_flushed(usize pivot_i, const Interval<KeyView>& pivot_key_range);

  BoxedSeq<EditSlice> to_boxed_seq(const InMemoryNode& node,
                                   BatchUpdateContext& update_context,
                                   Status& segment_load_status,
                                   i32 min_pivot_i,
                                   bool only_pivot,
                                   Optional<KeyView> min_key) const;

  usize segment_count(const TreeOptions& tree_options) const;

  void drop_before_pivot(i32 pivot_i,
                         const KeyView& pivot_key,
                         llfs::PageLoader& page_loader,
                         const TreeOptions& tree_options);

  void drop_after_pivot(i32 pivot_i,
                        const KeyView& pivot_key,
                        llfs::PageLoader& page_loader,
                        const TreeOptions& tree_options);

  Status split_pivot(InMemoryNode& node,
                     BatchUpdateContext& update_context,
                     i32 pivot_i,
                     const Interval<KeyView>& pivot_key_range,
                     const KeyView& sibling_min_key);

  void merge_pivots(InMemoryNode& node,
                    BatchUpdateContext& update_context,
                    i32 left_pivot_i,
                    i32 right_pivot_i);

  StatusOr<ValueView> find_key(const InMemoryNode& node, KeyQuery& query, i32 key_pivot_i) const;

  InMemoryNodeLevel merge(InMemoryNodeLevel&& sibling_level, usize node_pivot_count) &&;

  StatusOr<usize> start_serialize(const InMemoryNode& node, TreeSerializeContext& context);

  StatusOr<InMemoryNodeSegmentedLevel> finish_serialize(const InMemoryNode& node,
                                                        TreeSerializeContext& context);

  SmallFn<void(std::ostream&)> dump() const;
};

}  // namespace turtle_kv
