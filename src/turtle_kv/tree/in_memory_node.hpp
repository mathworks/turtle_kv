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

  struct Metrics {
    StatsMetric<u16> level_depth_stats;
  };

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  struct UpdateBuffer {
    using Self = UpdateBuffer;

    struct SegmentedLevel;

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

      void check_invariants(const char* file, int line) const;

      auto dump(bool multi_line = true) const
      {
        return [this, multi_line](std::ostream& out) {
          auto active = std::bitset<64>{this->active_pivots};
          auto flushed = std::bitset<64>{this->flushed_pivots};
          auto flushed_bounds = batt::dump_range(this->flushed_item_upper_bound_);
          if (multi_line) {
            out << "Segment:" << std::endl
                << "   active=" << active << std::endl
                << "  flushed=" << flushed << std::endl
                << "  flushed_upper_bounds=" << flushed_bounds;
          } else {
            out << "Segment{.active=" << active << ", .flushed=" << flushed
                << ", .flushed_upper_bounds=" << flushed_bounds << ",}";
          }
        };
      }

      const llfs::PageIdSlot& get_leaf_page_id() const
      {
        return this->page_id_slot;
      }

      u32 get_flushed_item_upper_bound(const SegmentedLevel&, i32 pivot_i) const;

      void set_flushed_item_upper_bound(i32 pivot_i, u32 upper_bound);

      u64 get_active_pivots() const
      {
        return this->active_pivots;
      }

      u64 get_flushed_pivots() const
      {
        return this->flushed_pivots;
      }

      void set_pivot_active(i32 pivot_i, bool active)
      {
        this->active_pivots = set_bit(this->active_pivots, pivot_i, active);
      }

      bool is_pivot_active(i32 pivot_i) const
      {
        return get_bit(this->active_pivots, pivot_i);
      }

      void insert_pivot(i32 pivot_i, bool is_active)
      {
        this->check_invariants(__FILE__, __LINE__);
        auto on_scope_exit = batt::finally([&] {
          this->check_invariants(__FILE__, __LINE__);
        });

        this->active_pivots = insert_bit(this->active_pivots, pivot_i, is_active);
        this->flushed_pivots = insert_bit(this->flushed_pivots, pivot_i, false);
      }

      void pop_front_pivots(i32 count)
      {
        if (count < 1) {
          return;
        }

        // Before we modify the bit sets, make sure we aren't losing any active/flushed pivots.
        //
        const u64 mask = (u64{1} << count) - 1;

        BATT_CHECK_EQ(bit_count(mask), count);
        BATT_CHECK_EQ((this->active_pivots & mask), u64{0});
        BATT_CHECK_EQ((this->flushed_pivots & mask), u64{0});

        // Shift both active and flushed pivot sets down by count.  We don't need to touch
        // flushed_item_upper_bound_ since getting rid of low-order zero bits doesn't change any
        // bit_rank calculations for flushed pivots.
        //
        this->active_pivots = (this->active_pivots >> count);
        this->flushed_pivots = (this->flushed_pivots >> count);
      }

      bool is_inactive() const
      {
        const bool inactive = (this->active_pivots == 0);
        if (inactive) {
          BATT_CHECK_EQ(this->flushed_pivots, 0);
          BATT_CHECK(this->flushed_item_upper_bound_.empty());
        }
        return inactive;
      }

      StatusOr<llfs::PinnedPage> load_leaf_page(llfs::PageLoader& page_loader,
                                                llfs::PinPageToJob pin_page_to_job) const;
    };

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

      auto dump() const
      {
        return [](std::ostream& out) {
          out << "EmptyLevel{}";
        };
      }
    };

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

      void drop_segment(usize i)
      {
        this->segments.erase(this->segments.begin() + i);
      }

      void drop_pivot_range(const Interval<i32>& pivot_range)
      {
        for (Segment& segment : this->segments) {
          in_segment(segment).drop_pivot_range(pivot_range);
          if (pivot_range.lower_bound == 0) {
            segment.pop_front_pivots(pivot_range.upper_bound);
          }
        }

        this->segments.erase(std::remove_if(this->segments.begin(),
                                            this->segments.end(),
                                            [](const Segment& segment) {
                                              return segment.is_inactive();
                                            }),
                             this->segments.end());
      }

      void drop_before_pivot(i32 pivot_i, const KeyView& pivot_key [[maybe_unused]])
      {
        this->drop_pivot_range(Interval<i32>{0, pivot_i});
      }

      void drop_after_pivot(i32 pivot_i, const KeyView& pivot_key [[maybe_unused]])
      {
        this->drop_pivot_range(Interval<i32>{pivot_i, 64});
      }

      bool is_pivot_active(i32 pivot_i) const
      {
        for (const Segment& segment : this->segments) {
          if (segment.is_pivot_active(pivot_i)) {
            return true;
          }
        }
        return false;
      }

      void check_items_sorted(const InMemoryNode& node, llfs::PageLoader& page_loader) const;

      SmallFn<void(std::ostream&)> dump() const
      {
        return [this](std::ostream& out) {
          out << "SegmentedLevel{\n";
          for (const Segment& segment : this->segments) {
            out << "    " << segment.dump(/*multi_line=*/false) << ",\n";
          }
          out << "  }";
        };
      }
    };

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

      auto dump() const
      {
        return [this](std::ostream& out) {
          out << "MergedLevel{" << this->result_set.debug_dump("    ") << "\n}";
        };
      }
    };

    using Level = std::variant<EmptyLevel, MergedLevel, SegmentedLevel>;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallVec<Level, 6> levels;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallFn<void(std::ostream&)> dump() const
    {
      return [this](std::ostream& out) {
        out << "UpdateBuffer{.levels={\n";
        for (const Level& level : levels) {
          batt::case_of(level, [&out](const auto& level_case) {
            out << "  " << level_case.dump() << ",\n";
          });
        }
        out << "},}";
      };
    }
  };

  struct PivotPendingBytes {
    using Self = PivotPendingBytes;

    usize pivot_index;
    usize pending_bytes;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void InMemoryNode::UpdateBuffer::Segment::check_invariants(const char* file, int line) const
{
  // Make sure the flushed pivots bit set and flushed_item_upper_bound (non-zero values) are in
  // sync.
  //
  BATT_CHECK_EQ(this->flushed_item_upper_bound_.size(), bit_count(this->flushed_pivots))
      << BATT_INSPECT(file) << BATT_INSPECT(line);

  // There should be no inactive pivots with a flushed upper bound.
  //
  BATT_CHECK_EQ(((~this->active_pivots) & this->flushed_pivots), u64{0})
      << BATT_INSPECT(file) << BATT_INSPECT(line);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline u32 InMemoryNode::UpdateBuffer::Segment::get_flushed_item_upper_bound(const SegmentedLevel&,
                                                                             i32 pivot_i) const
{
  if (!get_bit(this->flushed_pivots, pivot_i)) {
    return 0;
  }

  const i32 index = bit_rank(this->flushed_pivots, pivot_i);
  //----- --- -- -  -  -   -
  // TODO [tastolfi 2025-03-23] Remove these checks once we are convinced this is correct.
  //
  BATT_CHECK_GE(index, 0);
  BATT_CHECK_LT(index, this->flushed_item_upper_bound_.size());
  //----- --- -- -  -  -   -

  return this->flushed_item_upper_bound_[index];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void InMemoryNode::UpdateBuffer::Segment::set_flushed_item_upper_bound(i32 pivot_i,
                                                                              u32 upper_bound)
{
  this->check_invariants(__FILE__, __LINE__);
  auto on_scope_exit = batt::finally([&] {
    this->check_invariants(__FILE__, __LINE__);
  });

  if (!get_bit(this->flushed_pivots, pivot_i)) {
    if (upper_bound == 0) {
      return;
    }
    this->flushed_pivots = set_bit(this->flushed_pivots, pivot_i, true);

    const i32 index = bit_rank(this->flushed_pivots, pivot_i);
    //----- --- -- -  -  -   -
    // TODO [tastolfi 2025-03-23] Remove these checks once we are convinced this is correct.
    //
    BATT_CHECK_GE(index, 0);
    //----- --- -- -  -  -   -

    this->flushed_item_upper_bound_.insert(this->flushed_item_upper_bound_.begin() + index,
                                           upper_bound);

    //----- --- -- -  -  -   -
    // TODO [tastolfi 2025-03-23] Remove these checks once we are convinced this is correct.
    //
    BATT_CHECK_LT(index, this->flushed_item_upper_bound_.size());
    //----- --- -- -  -  -   -

  } else {
    const i32 index = bit_rank(this->flushed_pivots, pivot_i);
    //----- --- -- -  -  -   -
    // TODO [tastolfi 2025-03-23] Remove these checks once we are convinced this is correct.
    //
    BATT_CHECK_GE(index, 0);
    BATT_CHECK_LT(index, this->flushed_item_upper_bound_.size());
    //----- --- -- -  -  -   -

    if (upper_bound != 0) {
      this->flushed_item_upper_bound_[index] = upper_bound;
    } else {
      this->flushed_item_upper_bound_.erase(this->flushed_item_upper_bound_.begin() + index);
      this->flushed_pivots = set_bit(this->flushed_pivots, pivot_i, false);
    }
  }
}

}  // namespace turtle_kv
