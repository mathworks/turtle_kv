#pragma once

#include <turtle_kv/tree/in_memory_node_empty_level.hpp>
#include <turtle_kv/tree/in_memory_node_hybrid_level.hpp>
#include <turtle_kv/tree/in_memory_node_merged_level.hpp>
#include <turtle_kv/tree/in_memory_node_segmented_level.hpp>

#include <turtle_kv/tree/active_pivots_set.hpp>
#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/in_memory_leaf.hpp>
#include <turtle_kv/tree/max_pending_bytes.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/subtree.hpp>
#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>

#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>

#include <llfs/page_cache_job.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/assert.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct InMemoryNode {
  using Self = InMemoryNode;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct Metrics {
    /** \brief The number of nodes which have been serialized.
     */
    CountMetric<u64> serialized_node_count;

    /** \brief The sum of the pivot counts of all nodes which have been serialized.
     */
    CountMetric<u64> serialized_pivot_count;

    /** \brief The sum of the segment counts of all nodes which have been serialized.
     */
    CountMetric<u64> serialized_buffer_segment_count;

    /** \brief The sum total count of all non-empty buffer levels in all serialized nodes.
     */
    CountMetric<u64> serialized_nonempty_level_count;

    /** \brief Captures statistics about the number of levels per node.
     */
    StatsMetric<u16> level_depth_stats;

    /** \brief The total time spent on merging two Subtrees and updating parent metadata.
     */
    LatencyMetric merge_latency;

    /** \brief The number of times a merge operation was followed by a split operation.
     */
    CountMetric<u64> merge_then_split_count;
  };

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr usize kMaxTempPivots = 128;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief Mutable, in-memory representation of a node update buffer.
   */
  struct UpdateBuffer {
    using Self = UpdateBuffer;

    using Segment = InMemoryNodeSegment;
    using EmptyLevel = InMemoryNodeEmptyLevel;
    using MergedLevel = InMemoryNodeMergedLevel;
    using SegmentedLevel = InMemoryNodeSegmentedLevel;
    using HybridLevel = InMemoryNodeHybridLevel;
    using Level = InMemoryNodeLevel;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallVec<Level, 6> levels;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallFn<void(std::ostream&)> dump() const;

    usize count_non_empty_levels() const
    {
      usize count = 0;
      for (const Level& level : this->levels) {
        if (!batt::is_case<EmptyLevel>(level)) {
          ++count;
        }
      }
      return count;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PinnedPage pinned_node_page_;
  TreeOptions tree_options;
  const IsSizeTiered size_tiered_;
  i32 height = 0;
  SmallVec<Subtree, 64> children;
  SmallVec<llfs::PinnedPage, 64> child_pages;
  SmallVec<usize, 64> pending_bytes;
  ActivePivotsSet128 pending_bytes_is_exact = {};
  Optional<i32> latest_flush_pivot_i_;
  SmallVec<KeyView, 65> pivot_keys_;
  KeyView max_key_;
  KeyView common_key_prefix;
  UpdateBuffer update_buffer;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<std::unique_ptr<InMemoryNode>> unpack(llfs::PinnedPage&& pinned_node_page,
                                                        const TreeOptions& tree_options,
                                                        const PackedNodePage& packed_node);

  static StatusOr<std::unique_ptr<InMemoryNode>> from_subtrees(BatchUpdateContext& update_context,
                                                               const TreeOptions& tree_options,
                                                               Subtree&& first_subtree,
                                                               Subtree&& second_subtree,
                                                               const KeyView& key_upper_bound,
                                                               IsRoot is_root);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit InMemoryNode(llfs::PinnedPage&& pinned_node_page,
                        const TreeOptions& tree_options_arg,
                        IsSizeTiered size_tiered) noexcept
      : pinned_node_page_{std::move(pinned_node_page)}
      , tree_options{tree_options_arg}
      , size_tiered_{size_tiered}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IsSizeTiered is_size_tiered() const
  {
    return this->size_tiered_;
  }

  usize max_pivot_count() const
  {
    return kMaxPivots;
  }

  usize max_segment_count() const
  {
    return kMaxPivots - 1;
  }

  Slice<const KeyView> get_pivot_keys() const
  {
    return as_slice(this->pivot_keys_);
  }

  KeyView& min_key()
  {
    return this->pivot_keys_.front();
  }

  const KeyView& get_min_key() const
  {
    return this->pivot_keys_.front();
  }

  KeyView& max_key()
  {
    return this->max_key_;
  }

  const KeyView& get_max_key() const
  {
    return this->max_key_;
  }

  KeyView& key_upper_bound()
  {
    return this->pivot_keys_.back();
  }

  const KeyView& get_key_upper_bound() const
  {
    return this->pivot_keys_.back();
  }

  usize get_level_count() const
  {
    return this->update_buffer.levels.size();
  }

  const Subtree& get_child(i32 pivot_i) const
  {
    return this->children[pivot_i];
  }

  const KeyView& get_pivot_key(usize i) const
  {
    return this->pivot_keys_[i];
  }

  usize pivot_count() const
  {
    return this->children.size();
  }

  void add_pending_bytes(usize pivot_i, usize byte_count)
  {
    this->pending_bytes_is_exact.set(pivot_i, false);
    BATT_CHECK_EQ(this->pending_bytes_is_exact.get(pivot_i), false);

    this->pending_bytes[pivot_i] += byte_count;
  }

  //----- --- -- -  -  -   -

  StatusOr<ValueView> find_key(KeyQuery& query) const;

  StatusOr<ValueView> find_key_in_level(usize level_i, KeyQuery& query, i32 key_pivot_i) const;

  Status apply_batch_update(BatchUpdate& update, const KeyView& key_upper_bound, IsRoot is_root);

  Status update_buffer_insert(BatchUpdate& update);

  Status flush_if_necessary(BatchUpdateContext& context, bool force_flush = false);

  bool has_too_many_tiers() const;

  Status flush_to_pivot(BatchUpdateContext& context, i32 pivot_i);

  Status make_child_viable(BatchUpdateContext& context, i32 pivot_i);

  MaxPendingBytes find_max_pending() const;

  void push_levels_to_merge(MergeCompactor& compactor,
                            BatchUpdateContext& update_context,
                            Status& segment_load_status,
                            HasPageRefs& has_page_refs,
                            const Slice<UpdateBuffer::Level>& levels_to_merge,
                            i32 min_pivot_i,
                            bool only_pivot,
                            Optional<KeyView> min_key = None);

  Status set_pivot_items_flushed(BatchUpdateContext& update_context,
                                 usize pivot_i,
                                 const CInterval<KeyView>& flush_key_crange);

  Status set_pivot_completely_flushed(usize pivot_i, const Interval<KeyView>& pivot_key_range);

  void squash_empty_levels();

  usize key_data_byte_size() const;

  usize segment_filters_byte_size() const;

  usize total_segment_filter_cut_points() const;

  usize segment_count() const;

  SubtreeViability get_viability() const;

  bool is_viable(IsRoot is_root) const;

  /** \brief Split the node and return its new upper half (sibling).
   */
  StatusOr<std::unique_ptr<InMemoryNode>> try_split(BatchUpdateContext& context);

  /** \brief (internal use only) Try splitting the node directly (don't apply any
   * compaction/flushing remedies).
   */
  StatusOr<std::unique_ptr<InMemoryNode>> try_split_direct(BatchUpdateContext& context);

  /** \brief Attempt to make the node viable by flushing a batch.
   */
  Status try_flush(BatchUpdateContext& context);

  /** \brief Attempt to collapse one level of the tree. Returns the node's single pivot.
   * TODO [tastolfi 2026-05-04] describe the conditions under which this fn will panic
   */
  Subtree shrink_or_panic();

  /** \brief Merge the node in place with its right sibling.
   *
   * Returns nullptr if `sibling` is completely consumed; otherwise, returns the modified sibling
   * since a borrow occurred.
   */
  Status try_merge(BatchUpdateContext& context, std::unique_ptr<InMemoryNode> sibling) noexcept;

  /** \brief Splits the specified child, inserting a new pivot immediately after `pivot_i`.
   */
  Status split_child(BatchUpdateContext& update_context, i32 pivot_i);

  /** \brief Merges the specified child with a sibling.
   */
  Status merge_child(BatchUpdateContext& update_context, i32 pivot_i) noexcept;

  /** \brief Returns true iff there are no MergedLevels or unserialized Subtree children in this
   * node.
   */
  bool is_packable() const;

  Status start_serialize(TreeSerializeContext& context);

  StatusOr<llfs::PageId> finish_serialize(TreeSerializeContext& context);

  StatusOr<BatchUpdate> collect_pivot_batch(BatchUpdateContext& update_context,
                                            i32 pivot_i,
                                            const Interval<KeyView>& pivot_key_range);

  /** \brief Merges and compacts all live edits in all levels/segments, producing a single level
   * (if not size-tiered), or a series of non-key-overlapping levels with a single segment in each
   * (if size-tiered).
   *
   * This can be done if node splitting fails, to reduce the serialized space required by getting
   * rid of all the non-zero flushed key upper bounds.  This should NOT be done under normal
   * circumstances (while applying batch updates), since it will reduce the write-optimization
   * significantly.
   */
  Status compact_update_buffer_levels(BatchUpdateContext& context);
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace turtle_kv
