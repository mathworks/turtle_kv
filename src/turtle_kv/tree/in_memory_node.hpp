#pragma once

#include <turtle_kv/tree/algo/segments.hpp>
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
    StatsMetric<u16> level_depth_stats;
  };

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief Mutable, in-memory representation of a node update buffer.
   */
  struct UpdateBuffer {
    using Self = UpdateBuffer;

    struct SegmentedLevel;

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    //
    /** \brief Mutable, in-memory representation of a serialized buffer segment (one leaf page).
     */
    struct Segment {
      using Self = Segment;

      /** \brief The id of the leaf page for this segment.
       */
      llfs::PageIdSlot page_id_slot;

      /** \brief A bit set of pivots in whose key range this segment contains items.
       */
      u64 active_pivots = 0;

      /** \brief A bit set indicating the non-zero elements of `flushed_item_count`.
       */
      u64 flushed_pivots = 0;

      /** \brief For each pivot, the number of items that have been flushed to that pivot from this
       * segment.
       */
      SmallVec<u32, 64> flushed_item_upper_bound_;

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      /** \brief Returns a reference to the PageId of this segment's leaf page, plus weak reference
       * to its cache slot (if known).
       */
      const llfs::PageIdSlot& get_leaf_page_id() const
      {
        return this->page_id_slot;
      }

      /** \brief Returns the active pivots bit set.
       */
      u64 get_active_pivots() const
      {
        return this->active_pivots;
      }

      /** \brief Returns the bit set of pivots with a non-zero flushed item upper bound in this
       * segment.
       */
      u64 get_flushed_pivots() const
      {
        return this->flushed_pivots;
      }

      /** \brief Marks this segment as containing (or not) active keys addressed to `pivot_i`.
       */
      void set_pivot_active(i32 pivot_i, bool active)
      {
        this->active_pivots = set_bit(this->active_pivots, pivot_i, active);
      }

      /** \brief Returns true iff this segment has active keys addressed to `pivot_i`.
       */
      bool is_pivot_active(i32 pivot_i) const
      {
        return get_bit(this->active_pivots, pivot_i);
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      /** \brief Panic if the following invariants are not satisfied:
       *
       * 1. The size of this->flushed_item_upper_bound_ is equal to the number of 1 bits in
       *    this->flushed_pivots
       * 2. There are no pivots marked as flushed and inactive.
       */
      void check_invariants(const char* file, int line) const;

      /** \brief Returns the item index within the segment leaf page of the first unflushed key
       * addressed to `pivot_i`, or 0 if there are no flushed keys for the given pivot.
       */
      u32 get_flushed_item_upper_bound(const SegmentedLevel&, i32 pivot_i) const;

      /** \brief Marks all keys whose index within the segment leaf page is less than `upper_bound`
       * as flushed for `pivot_i`.
       *
       * Should only be called by SegmentAlgorithms or SegmentedLevelAlgorithms.
       */
      void set_flushed_item_upper_bound(i32 pivot_i, u32 upper_bound);

      /** \brief Inserts a new pivot bit in this->active_pivots and this->flushed_pivots at position
       * `pivot_i`.  This is called when a child subtree needs to be split.
       */
      void insert_pivot(i32 pivot_i, bool is_active);

      /** \brief Removes the specified number (`count`) pivots from the front of this segment.  This
       * is used while splitting a node's update buffer.
       */
      void pop_front_pivots(i32 count);

      /** \brief Returns true iff this segment is not active for any pivots.
       */
      bool is_inactive() const;

      /** \brief Loads the leaf page for this Segment, returning the resulting llfs::PinnedPage.
       */
      StatusOr<llfs::PinnedPage> load_leaf_page(llfs::PageLoader& page_loader,
                                                llfs::PinPageToJob pin_page_to_job) const;

      /** \brief Prints a human-readable representation of this Segment.
       */
      SmallFn<void(std::ostream&)> dump(bool multi_line = true) const;
    };

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    //
    /** \brief An empty update buffer level.
     */
    struct EmptyLevel {
      using Self = EmptyLevel;

      void drop_after_pivot(i32 split_pivot_i [[maybe_unused]],
                            const KeyView& split_pivot_key [[maybe_unused]])
      {
        // Nothing to do!
      }

      void drop_before_pivot(i32 split_pivot_i [[maybe_unused]],
                             const KeyView& split_pivot_key [[maybe_unused]])
      {
        // Nothing to do!
      }

      SmallFn<void(std::ostream&)> dump() const;
    };

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    //
    /** \brief Mutable, in-memory representation of a non-empty serialized update buffer level.
     */
    struct SegmentedLevel {
      using Self = SegmentedLevel;
      using Segment = InMemoryNode::UpdateBuffer::Segment;

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      SmallVec<Segment, 32> segments;

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      bool empty() const
      {
        return this->segments.empty();
      }

      usize segment_count() const
      {
        return this->segments.size();
      }

      Segment& get_segment(usize i)
      {
        return this->segments[i];
      }

      const Segment& get_segment(usize i) const
      {
        return this->segments[i];
      }

      Slice<const Segment> get_segments_slice() const
      {
        return as_const_slice(this->segments);
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      /** \brief Removes the specified segment from this level.
       *
       * Should only be called by SegmentedLevelAlgorithms::flush_pivot_up_to_key.
       */
      void drop_segment(usize i);

      /** \brief Removes the specified set of pivots from this level.
       *
       * Used to implement node splits.
       */
      void drop_pivot_range(const Interval<i32>& pivot_range);

      /** \brief Drops all pivots before (but not including) the specified pivot.
       *
       * The `pivot_key` parameter is ignored.
       */
      void drop_before_pivot(i32 pivot_i, const KeyView& pivot_key [[maybe_unused]]);

      /** \brief Drops all pivots after (and including) the specified pivot.
       *
       * The `pivot_key` parameter is ignored.
       */
      void drop_after_pivot(i32 pivot_i, const KeyView& pivot_key [[maybe_unused]]);

      /** \brief Returns true iff the specified pivot is active for any of the Segments in this
       * level.
       */
      bool is_pivot_active(i32 pivot_i) const;

      /** \brief Verifies that all items appear in this level in key-sorted order; panic if this is
       * not the case.
       *
       * Warning: This is a very expensive operation!  Do not call it on a performance-critical code
       * path.
       */
      void check_items_sorted(const InMemoryNode& node, llfs::PageLoader& page_loader) const;

      /** \brief Prints a human-readable representation of the level.
       */
      SmallFn<void(std::ostream&)> dump() const;
    };

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    //
    struct MergedLevel {
      using Self = MergedLevel;

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
      std::vector<TreeSerializeContext::BuildPageJobId> segment_future_ids_;

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      void drop_key_range(const Interval<KeyView>& key_drop_range)
      {
        this->result_set.drop_key_range_half_open(key_drop_range);
      }

      void drop_after_pivot(i32 pivot_i [[maybe_unused]], const KeyView& pivot_key)
      {
        this->drop_key_range(Interval<KeyView>{
            .lower_bound = pivot_key,
            .upper_bound = global_max_key(),
        });
      }

      void drop_before_pivot(i32 pivot_i [[maybe_unused]], const KeyView& pivot_key)
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

      /** \brief Returns the number of segment leaf page build jobs added to the context.
       */
      StatusOr<usize> start_serialize(const InMemoryNode& node, TreeSerializeContext& context);

      StatusOr<SegmentedLevel> finish_serialize(const InMemoryNode& node,
                                                TreeSerializeContext& context);

      SmallFn<void(std::ostream&)> dump() const;
    };

    using Level = std::variant<EmptyLevel, MergedLevel, SegmentedLevel>;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallVec<Level, 6> levels;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallFn<void(std::ostream&)> dump() const;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PinnedPage pinned_node_page_;
  TreeOptions tree_options;
  const IsSizeTiered size_tiered_;
  i32 height = 0;
  SmallVec<Subtree, 64> children;
  SmallVec<llfs::PinnedPage, 64> child_pages;
  SmallVec<usize, 64> pending_bytes;
  u64 pending_bytes_is_exact = 0;
  SmallVec<KeyView, 65> pivot_keys_;
  KeyView max_key_;
  KeyView common_key_prefix;
  UpdateBuffer update_buffer;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<std::unique_ptr<InMemoryNode>> unpack(llfs::PinnedPage&& pinned_node_page,
                                                        const TreeOptions& tree_options,
                                                        const PackedNodePage& packed_node);

  static StatusOr<std::unique_ptr<InMemoryNode>> from_subtrees(llfs::PageLoader& page_loader,  //
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
    return this->is_size_tiered() ? (64 - 1) : (64 - 1);
  }

  usize max_segment_count() const
  {
    return this->is_size_tiered() ? (64 - 2) : (64 - 2);
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
    this->pending_bytes_is_exact = set_bit(this->pending_bytes_is_exact, pivot_i, false);
    BATT_CHECK_EQ(get_bit(this->pending_bytes_is_exact, pivot_i), false);

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

  void push_levels_to_merge(MergeFrame& frame,
                            llfs::PageLoader& page_loader,
                            Status& segment_load_status,
                            HasPageRefs& has_page_refs,
                            const Slice<UpdateBuffer::Level>& levels_to_merge,
                            i32 min_pivot_i,
                            bool only_pivot);

  Status set_pivot_items_flushed(llfs::PageLoader& page_loader,
                                 usize pivot_i,
                                 const CInterval<KeyView>& flush_key_crange);

  Status set_pivot_completely_flushed(usize pivot_i, const Interval<KeyView>& pivot_key_range);

  void squash_empty_levels();

  usize key_data_byte_size() const;

  usize flushed_item_counts_byte_size() const;

  usize segment_count() const;

  SubtreeViability get_viability() const;

  bool is_viable(IsRoot is_root) const;

  /** \brief Split the node and return its new upper half (sibling).
   */
  StatusOr<std::unique_ptr<InMemoryNode>> try_split(BatchUpdateContext& context);

  /** \brief Attempt to make the node viable by flushing a batch.
   */
  Status try_flush(BatchUpdateContext& context);

  /** \brief Splits the specified child, inserting a new pivot immediately after `pivot_i`.
   */
  Status split_child(BatchUpdateContext& update_context, i32 pivot_i);

  /** \brief Returns true iff there are no MergedLevels or unserialized Subtree children in this
   * node.
   */
  bool is_packable() const;

  Status start_serialize(TreeSerializeContext& context);

  StatusOr<llfs::PageId> finish_serialize(TreeSerializeContext& context);

  StatusOr<BatchUpdate> collect_pivot_batch(BatchUpdateContext& update_context,
                                            i32 pivot_i,
                                            const Interval<KeyView>& pivot_key_range);

  /** \brief Merges and compacts all live edits in all levels/segments, producing a single level (if
   * not size-tiered), or a series of non-key-overlapping levels with a single segment in each (if
   * size-tiered).
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
