#pragma once

#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>
#include <turtle_kv/tree/update_buffer_levels.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
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

  StatusOr<ValueView> find_key(const KeyView& key) const
  {
    return this->result_set.find_key(key);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize estimate_segment_count(const TreeOptions& tree_options) const;

  InMemoryNodeMergedLevel concat(InMemoryNodeMergedLevel&& that);

  BoxedSeq<EditSlice> to_boxed_seq(const InMemoryNode& node, i32 min_pivot_i) const;

  bool set_items_flushed(const CInterval<KeyView>& flush_key_crange);

  bool set_items_flushed(const Interval<KeyView>& flush_key_range);

  MergeCompactor::ResultSet<false>* front();

  MergeCompactor::ResultSet<false>* back();

  InMemoryNodeLevel merge(InMemoryNodeLevel&& sibling_level, usize node_pivot_count) &&;

  /** \brief Returns the number of segment leaf page build jobs added to the context.
   */
  StatusOr<usize> start_serialize(const InMemoryNode& node, TreeSerializeContext& context);

  StatusOr<InMemoryNodeSegmentedLevel> finish_serialize(const InMemoryNode& node,
                                                        TreeSerializeContext& context);

  SmallFn<void(std::ostream&)> dump() const;
};

}  // namespace turtle_kv
