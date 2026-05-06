#pragma once

#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>
#include <turtle_kv/tree/update_buffer_levels.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/interval.hpp>
#include<turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>


#include <vector>

namespace turtle_kv {

struct InMemoryNode;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct InMemoryNodeMergedLevel {
  using Self = InMemoryNodeMergedLevel;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  std::vector<TreeSerializeContext::BuildPageJobId> segment_future_ids_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void drop_key_range(const Interval<KeyView>& key_drop_range)
  {
    this->result_set.drop_key_range_half_open(key_drop_range);
  }

  void drop_after_pivot(i32 pivot_i [[maybe_unused]],
                        const KeyView& pivot_key,
                        llfs::PageLoader& page_loader [[maybe_unused]],
                        const TreeOptions& tree_options [[maybe_unused]])
  {
    this->drop_key_range(Interval<KeyView>{
        .lower_bound = pivot_key,
        .upper_bound = global_max_key(),
    });
  }

  void drop_before_pivot(i32 pivot_i [[maybe_unused]],
                         const KeyView& pivot_key,
                         llfs::PageLoader& page_loader [[maybe_unused]],
                         const TreeOptions& tree_options [[maybe_unused]])
  {
    this->drop_key_range(Interval<KeyView>{
        .lower_bound = global_min_key(),
        .upper_bound = pivot_key,
    });
  }

  usize estimate_segment_count(const TreeOptions& tree_options) const
  {
    const usize packed_size = this->result_set.get_packed_size();
    if (packed_size == 0) {
      return 0;
    }

    const usize capacity_per_segment = tree_options.flush_size() - tree_options.max_item_size();
    const usize estimated = (packed_size + capacity_per_segment - 1) / capacity_per_segment;

    BATT_CHECK_GE(estimated * capacity_per_segment, packed_size);
    BATT_CHECK_LT((estimated - 1) * capacity_per_segment, packed_size)
        << BATT_INSPECT(estimated) << BATT_INSPECT(capacity_per_segment);

    return estimated;
  }

  InMemoryNodeMergedLevel concat(InMemoryNodeMergedLevel&& that)
  {
    return InMemoryNodeMergedLevel{
        .result_set = MergeCompactor::ResultSet<false>::concat(std::move(this->result_set),
                                                               std::move(that.result_set)),
        .segment_future_ids_ = {}};
  }

  BoxedSeq<EditSlice> to_boxed_seq(const InMemoryNode& node, i32 min_pivot_i) const;

  bool set_items_flushed(const CInterval<KeyView>& flush_key_crange)
  {
    this->result_set.drop_key_range(flush_key_crange);
    return this->result_set.empty();
  }

  bool set_items_flushed(const Interval<KeyView>& flush_key_range)
  {
    this->result_set.drop_key_range_half_open(flush_key_range);
    return this->result_set.empty();
  }

  StatusOr<ValueView> find_key(const KeyView& key) const
  {
    return this->result_set.find_key(key);
  }

  MergeCompactor::ResultSet<false>* front()
  {
    if (this->result_set.empty()) {
      return nullptr;
    }

    return &this->result_set;
  }

  MergeCompactor::ResultSet<false>* back()
  {
    return this->front();
  }

  InMemoryNodeLevel merge(InMemoryNodeLevel&& sibling_level, usize node_pivot_count) &&;

  /** \brief Returns the number of segment leaf page build jobs added to the context.
   */
  StatusOr<usize> start_serialize(const InMemoryNode& node, TreeSerializeContext& context);

  StatusOr<InMemoryNodeSegmentedLevel> finish_serialize(const InMemoryNode& node,
                                                        TreeSerializeContext& context);

  SmallFn<void(std::ostream&)> dump() const;
};

}  // namespace turtle_kv
