#include <turtle_kv/tree/in_memory_node.hpp>
//

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/algo/segments.hpp>
#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>
#include <turtle_kv/tree/subtree_viability.hpp>

#include <turtle_kv/core/algo/split_parts.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/key_view_range.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <batteries/case_of.hpp>
#include <batteries/require.hpp>

namespace turtle_kv {

namespace {

using UpdateBuffer = InMemoryNode::UpdateBuffer;
using Level = UpdateBuffer::Level;
using EmptyLevel = UpdateBuffer::EmptyLevel;
using MergedLevel = UpdateBuffer::MergedLevel;
using SegmentedLevel = UpdateBuffer::SegmentedLevel;
using Segment = UpdateBuffer::Segment;

using PackedUpdateBuffer = PackedNodePage::UpdateBuffer;
using PackedLevel = PackedUpdateBuffer::SegmentedLevel;
using PackedSegment = PackedUpdateBuffer::Segment;

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::unpack(
    llfs::PinnedPage&& pinned_node_page,
    const TreeOptions& tree_options,
    const PackedNodePage& packed_node)
{
  auto node = std::make_unique<InMemoryNode>(std::move(pinned_node_page),
                                             tree_options,
                                             packed_node.is_size_tiered());

  const usize pivot_count = packed_node.pivot_count();

  BATT_REQUIRE_LE(pivot_count, kMaxPivots);

  node->tree_options = tree_options;
  node->height = packed_node.height;
  node->children.resize(pivot_count);
  node->child_pages.resize(pivot_count);
  node->pending_bytes.resize(pivot_count);
  node->pending_bytes_is_exact = 0;
  node->pivot_keys_.resize(pivot_count + 1);
  node->max_key_ = packed_node.max_key();
  node->common_key_prefix = packed_node.common_key_prefix();

  for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
    node->children[pivot_i] = Subtree::from_packed_page_id(packed_node.children[pivot_i]);
    node->pending_bytes[pivot_i] = packed_node.pending_bytes[pivot_i];
    node->pivot_keys_[pivot_i] = packed_node.get_pivot_key(pivot_i);
  }
  node->pivot_keys_[pivot_count] = packed_node.get_pivot_key(pivot_count);

  // Unpack the update buffer.
  //
  if (packed_node.is_size_tiered()) {
    node->update_buffer.levels.resize(packed_node.update_buffer.segment_count());
  } else {
    node->update_buffer.levels.resize(tree_options.max_buffer_levels());
  }
  const usize in_memory_level_count = node->update_buffer.levels.size();

  const usize packed_level_count = packed_node.is_size_tiered()
                                       ? packed_node.update_buffer.segment_count()
                                       : PackedNodePage::kMaxLevels;

  for (usize level_i = 0; level_i < packed_level_count; ++level_i) {
    const PackedLevel level = packed_node.is_size_tiered() ? packed_node.get_tier(level_i)
                                                           : packed_node.get_level(level_i);

    const Slice<const PackedSegment> level_segments = level.segments_slice;

    if (level_segments.empty()) {
      // Base case: empty level.
      //
      if (level_i < in_memory_level_count) {
        node->update_buffer.levels[level_i] = EmptyLevel{};
      }

    } else {
      BATT_CHECK_LT(level_i, in_memory_level_count);

      // General case: non-empty level.
      //
      SegmentedLevel& segmented_level =
          node->update_buffer.levels[level_i].emplace<SegmentedLevel>();

      const usize segment_count = level_segments.size();
      segmented_level.segments.resize(segment_count);

      if (packed_node.is_size_tiered()) {
        BATT_CHECK_LE(segment_count, 1);
      }

      for (usize segment_i = 0; segment_i < segment_count; ++segment_i) {
        const PackedNodePage::UpdateBuffer::Segment& packed_segment = level_segments[segment_i];
        Segment& segment = segmented_level.segments[segment_i];

        segment.page_id_slot = llfs::PageIdSlot::from_page_id(packed_segment.leaf_page_id.unpack());
        segment.active_pivots = packed_segment.active_pivots.unpack();

        BATT_ASSIGN_OK_RESULT(segment.filter,
                              packed_node.create_piecewise_filter(level_i, segment_i));

        segment.check_invariants(__FILE__, __LINE__);
      }
    }
  }

  return {std::move(node)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::from_subtrees(
    BatchUpdateContext& update_context,
    const TreeOptions& tree_options,
    Subtree&& first_subtree,
    Subtree&& second_subtree,
    const KeyView& key_upper_bound,
    IsRoot is_root)
{
  llfs::PageLoader& page_loader = update_context.page_loader;
  llfs::PageCacheOvercommit& overcommit = update_context.overcommit;

  auto new_node = std::make_unique<InMemoryNode>(llfs::PinnedPage{},
                                                 tree_options,
                                                 tree_options.is_size_tiered());

  BATT_ASSIGN_OK_RESULT(const i32 first_height,  //
                        first_subtree.get_height(page_loader, overcommit));

  BATT_ASSIGN_OK_RESULT(const i32 second_height,  //
                        second_subtree.get_height(page_loader, overcommit));

  BATT_CHECK_EQ(first_height, second_height);

  new_node->height = first_height + 1;
  new_node->children.resize(2);
  new_node->children[0] = std::move(first_subtree);
  new_node->children[1] = std::move(second_subtree);

  new_node->pending_bytes.resize(2, 0);
  new_node->child_pages.resize(2);

  new_node->pivot_keys_.resize(3);
  if (is_root) {
    new_node->pivot_keys_[0] = global_min_key();
  } else {
    BATT_ASSIGN_OK_RESULT(
        new_node->pivot_keys_[0],
        new_node->children[0].get_min_key(page_loader, overcommit, new_node->child_pages[0]));
  }

  BATT_ASSIGN_OK_RESULT(
      const KeyView first_child_max_key,
      new_node->children[0].get_max_key(page_loader, overcommit, new_node->child_pages[0]));

  BATT_ASSIGN_OK_RESULT(
      const KeyView second_child_min_key,
      new_node->children[1].get_min_key(page_loader, overcommit, new_node->child_pages[1]));

  const KeyView prefix = llfs::find_common_prefix(0, first_child_max_key, second_child_min_key);

  new_node->pivot_keys_[1] = second_child_min_key.substr(0, prefix.size() + 1);
  new_node->pivot_keys_[2] = key_upper_bound;

  BATT_ASSIGN_OK_RESULT(
      new_node->max_key_,
      new_node->children[1].get_max_key(page_loader, overcommit, new_node->child_pages[1]));

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::apply_batch_update(BatchUpdate& update,
                                        const KeyView& key_upper_bound,
                                        IsRoot is_root)
{
  BATT_DEBUG_INFO("InMemoryNode::apply_batch_update");

  BATT_CHECK_EQ(key_upper_bound, this->pivot_keys_.back());

  if (update.result_set.empty()) {
    return OkStatus();
  }

  BATT_CHECK(this->is_viable(is_root));

  // Update key bounds.
  //
  BATT_CHECK(!this->pivot_keys_.empty());
  BATT_CHECK_LT(update.result_set.get_max_key(), this->key_upper_bound());

  this->min_key() = std::min(this->min_key(), update.result_set.get_min_key());
  this->max_key() = std::max(this->max_key(), update.result_set.get_max_key());
  this->key_upper_bound() = std::max(this->key_upper_bound(), key_upper_bound);

  // Update per-pivot pending bytes.
  //
  in_node(*this).update_pending_bytes(update);

  // Merge the update batch into the buffer.
  //
  BATT_REQUIRE_OK(this->update_buffer_insert(update));

  // Check for flush.
  //
  BATT_REQUIRE_OK(this->flush_if_necessary(update.context));

  // We don't need to check whether _this_ node needs to be split; the caller will take care of
  // that!
  //
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::update_buffer_insert(BatchUpdate& update)
{
  auto on_scope_exit = batt::finally([&] {
    Self::metrics().level_depth_stats.update(this->update_buffer.levels.size());
  });

  if (this->is_size_tiered()) {
    this->update_buffer.levels.insert(this->update_buffer.levels.begin(),
                                      MergedLevel{
                                          .result_set = update.result_set,
                                          .segment_future_ids_ = {},
                                      });
    return OkStatus();
  }

  // Base case 0: UpdateBuffer completely empty.
  //
  if (this->update_buffer.levels.empty()) {
    this->update_buffer.levels.emplace_back(MergedLevel{
        .result_set = update.result_set,
        .segment_future_ids_ = {},
    });
    return OkStatus();
  }

  // Base case 1: UpdateBuffer's first level is empty.
  //
  if (batt::is_case<EmptyLevel>(this->update_buffer.levels.front())) {
    this->update_buffer.levels.front() = MergedLevel{
        .result_set = update.result_set,
        .segment_future_ids_ = {},
    };
    return OkStatus();
  }

  // General case: Collect the levels to merge by repeatedly adding the next, stopping when we
  // see an EmptyLevel.
  //
  Slice<Level> levels_to_merge = as_slice(this->update_buffer.levels.data(), 1);
  while (levels_to_merge.size() < this->update_buffer.levels.size()) {
    levels_to_merge.advance_end(1);
    if (batt::is_case<EmptyLevel>(levels_to_merge.back())) {
      levels_to_merge.drop_back();
      break;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Merge the levels.
  //
  MergedLevel new_merged_level;

  // Set the generator function for the MergeCompactor; it will push a single MergeFrame with a
  // MergeLine for the incoming batch plus each level in `levels_to_merge`.
  //
  HasPageRefs has_page_refs{false};
  Status segment_load_status;

  BATT_ASSIGN_OK_RESULT(  //
      new_merged_level.result_set,
      update.context.merge_compact_edits</*decay_to_items=*/false>(  //
          global_max_key(),
          [&](MergeCompactor& compactor) -> Status {
            compactor.push_level(update.result_set.live_edit_slices());
            this->push_levels_to_merge(compactor,
                                       update.context,
                                       segment_load_status,
                                       has_page_refs,
                                       levels_to_merge,
                                       /*min_pivot_i=*/0,
                                       /*only_pivot=*/false);
            return OkStatus();
          }));

  // Make sure there were no segment leaf page load failures that may have prematurely
  // terminated a merge line.
  //
  BATT_REQUIRE_OK(segment_load_status);

  // ----- Cleanup Stage : remove old levels and adjust the stack -----

  // Purge the old (merged) levels.
  //
  for (Level& level : levels_to_merge) {
    level = EmptyLevel{};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Insert the new level into the levels stack.

  // If there is already an empty level slot in the vector, just put the new MergedLevel there.
  //
  if (levels_to_merge.size() < this->update_buffer.levels.size()) {
    BATT_CHECK(batt::is_case<EmptyLevel>(this->update_buffer.levels[levels_to_merge.size()]));
    this->update_buffer.levels[levels_to_merge.size()] = std::move(new_merged_level);

  } else {
    // Sanity check: levels_to_merge can't be larger than the vector from which it was taken!
    //
    BATT_CHECK_EQ(levels_to_merge.size(), this->update_buffer.levels.size());

    // Grow the levels vector if under the max depth.
    //
    BATT_CHECK(!this->is_size_tiered());
    if (this->update_buffer.levels.size() < this->tree_options.max_buffer_levels()) {
      this->update_buffer.levels.emplace_back();
    }

    // The new level goes at the bottom.
    //
    this->update_buffer.levels.back() = std::move(new_merged_level);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::flush_if_necessary(BatchUpdateContext& context, bool force_flush)
{
  // If we have enough buffered edit bytes on some pivot to flush, then do it.
  //
  const MaxPendingBytes max_pending = this->find_max_pending();

  const bool flush_needed = force_flush ||                                                      //
                            (max_pending.byte_count >= this->tree_options.min_flush_size()) ||  //
                            this->has_too_many_tiers();

  if (!flush_needed) {
    return OkStatus();
  }

  BATT_REQUIRE_OK(this->flush_to_pivot(context, max_pending.pivot_index));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNode::has_too_many_tiers() const
{
  constexpr usize kMaxBytesPending = usize{1} << 30;

  if (!this->is_size_tiered()) {
    return false;
  }

  if (this->get_level_count() > this->max_segment_count()) {
    return true;
  }

  if (this->get_level_count() < this->pivot_count()) {
    return false;
  }

  for (usize n_bytes : this->pending_bytes) {
    if (n_bytes > kMaxBytesPending) {
      return true;
    }
  }

  return false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::compact_update_buffer_levels(BatchUpdateContext& update_context)
{
  MergedLevel new_merged_level;

  HasPageRefs has_page_refs{false};
  Status segment_load_status;

  BATT_ASSIGN_OK_RESULT(new_merged_level.result_set,
                        update_context.merge_compact_edits</*decay_to_items=*/false>(
                            global_max_key(),
                            [&](MergeCompactor& compactor) -> Status {
                              this->push_levels_to_merge(compactor,
                                                         update_context,
                                                         segment_load_status,
                                                         has_page_refs,
                                                         as_slice(this->update_buffer.levels),
                                                         /*min_pivot_i=*/0,
                                                         /*only_pivot=*/false);
                              return OkStatus();
                            }));

  BATT_REQUIRE_OK(segment_load_status);

  this->update_buffer.levels.clear();
  this->update_buffer.levels.emplace_back(std::move(new_merged_level));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::try_flush(BatchUpdateContext& context)
{
  return this->flush_if_necessary(context, /*force_flush=*/true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<BatchUpdate> InMemoryNode::collect_pivot_batch(BatchUpdateContext& update_context,
                                                        i32 pivot_i,
                                                        const Interval<KeyView>& pivot_key_range)
{
  BatchUpdate pivot_batch{
      .context = update_context,
      .result_set = {},
      .edit_size_totals = None,
  };

  Status segment_load_status;
  HasPageRefs has_page_refs{false};

  // Merge/compact all pending edits for the specified pivot.
  //
  BATT_ASSIGN_OK_RESULT(                                             //
      pivot_batch.result_set,                                        //
      update_context.merge_compact_edits</*decay_to_items=*/false>(  //
          /*max_key=*/pivot_key_range.upper_bound,                   //
          [&](MergeCompactor& compactor) -> Status {
            this->push_levels_to_merge(compactor,
                                       update_context,
                                       segment_load_status,
                                       has_page_refs,
                                       as_slice(this->update_buffer.levels),
                                       /*min_pivot_i=*/pivot_i,
                                       /*only_pivot=*/true);
            return OkStatus();
          }));

  BATT_REQUIRE_OK(segment_load_status);

  // Make sure the result set doesn't contain the first key from the next pivot.
  //
  pivot_batch.result_set.drop_key_range_half_open(Interval<KeyView>{
      pivot_key_range.upper_bound,
      this->key_upper_bound(),
  });

  // Reset edit size totals.
  //
  pivot_batch.edit_size_totals = None;

  return pivot_batch;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::flush_to_pivot(BatchUpdateContext& update_context, i32 pivot_i)
{
#if TURTLE_KV_PROFILE_UPDATES
  update_context.metrics.flush_count.add(1);
#endif  // TURTLE_KV_PROFILE_UPDATES

  Interval<KeyView> pivot_key_range = in_node(*this).get_pivot_key_range(pivot_i);

  BATT_ASSIGN_OK_RESULT(BatchUpdate child_update,
                        this->collect_pivot_batch(update_context, pivot_i, pivot_key_range));

  this->latest_flush_pivot_i_ = pivot_i;

  const usize orig_child_update_byte_size = child_update.get_byte_size();

  // Take the largest prefix of the merged edits as possible, without making the flushed batch too
  // large.  However, stick to the single-leaf flush size when flushing directly to the bottom layer
  // *or* if using lazy (size-tiered) compaction.
  //
  const usize max_flush_size = (this->height == 2 || this->is_size_tiered())
                                   ? this->tree_options.flush_size()
                                   : this->tree_options.max_flush_size();

  const usize byte_size_limit = max_flush_size - (this->tree_options.max_item_size() - 1);

  BatchUpdate::TrimResult trim_result = child_update.trim_back_down_to_size(byte_size_limit);

  // If the child update batch is empty, then update our metadata and we are done.
  //
  if (child_update.result_set.empty()) {
    BATT_REQUIRE_OK(this->set_pivot_completely_flushed(pivot_i, pivot_key_range));
    BATT_CHECK_EQ(trim_result.n_bytes_trimmed, 0);
    this->pending_bytes[pivot_i] = 0;
    this->pending_bytes_is_exact = set_bit(this->pending_bytes_is_exact, pivot_i, true);
    BATT_CHECK_EQ(get_bit(this->pending_bytes_is_exact, pivot_i), true);
    return OkStatus();
  }

  // Calculate the flushed key range.
  //
  CInterval<KeyView> flush_key_crange = child_update.get_key_crange();
  flush_key_crange.lower_bound = pivot_key_range.lower_bound;

  // Mark all keys in the child update as flushed.
  //
  BATT_REQUIRE_OK(this->set_pivot_items_flushed(update_context, pivot_i, flush_key_crange));

  // Update pending bytes for the flushed pivot; this is equal to the number of bytes we had to trim
  // from the end of the batch to make it fit under the limit.
  //
  BATT_CHECK_EQ(trim_result.n_bytes_trimmed + child_update.get_byte_size(),
                orig_child_update_byte_size)
      << BATT_INSPECT(trim_result.n_bytes_trimmed) << BATT_INSPECT(child_update.get_byte_size());

  this->pending_bytes[pivot_i] = trim_result.n_bytes_trimmed;
  this->pending_bytes_is_exact = set_bit(this->pending_bytes_is_exact, pivot_i, true);
  BATT_CHECK_EQ(get_bit(this->pending_bytes_is_exact, pivot_i), true);

  // Recursively apply batch update.
  //
  BATT_REQUIRE_OK(this->children[pivot_i].apply_batch_update(
      this->tree_options,
      ParentNodeHeight{this->height},
      child_update,
      /*key_upper_bound=*/this->get_pivot_key(pivot_i + 1),
      IsRoot{false}));

  return this->make_child_viable(update_context, pivot_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::make_child_viable(BatchUpdateContext& update_context, i32 pivot_i)
{
  bool tried_flush = false;
  bool tried_split = false;

  Status status = batt::case_of(                //
      this->children[pivot_i].get_viability(),  //

      //----- --- -- -  -  -   -
      [](const Viable&) -> Status {
        return OkStatus();
      },

      //----- --- -- -  -  -   -
      [&](const NeedsSplit& needs_split) -> Status {
        // If the only thing that's stopping the child from being viable is that it has too many
        // buffer segments, then attempt to fix the problem by flushing.
        //
        if (needs_split.too_many_segments &&  //
            !needs_split.too_many_pivots &&   //
            !needs_split.keys_too_large)      //
        {
          tried_flush = true;

          Subtree& child = this->children[pivot_i];

          Status child_flush_status = child.try_flush(update_context);
          if (child_flush_status.ok() && batt::is_case<Viable>(child.get_viability())) {
            return OkStatus();
          }
          //
          // else - fall through and try a regular split.
        }
        tried_split = true;

        return this->split_child(update_context, pivot_i);
      },

      //----- --- -- -  -  -   -
      [&](const NeedsMerge&) -> Status {
        BATT_PANIC() << "TODO [tastolfi 2025-03-16] implement me!";
        return batt::StatusCode::kUnimplemented;
      });

  return status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::split_child(BatchUpdateContext& update_context, i32 pivot_i)
{
#if TURTLE_KV_PROFILE_UPDATES
  update_context.metrics.split_count.add(1);
#endif

  // Make sure we don't exceed the temporary pivot count limit.
  //
  BATT_CHECK_LT(this->pivot_count(), (i32)InMemoryNode::kMaxTempPivots);
  auto on_scope_exit = batt::finally([&] {
    BATT_CHECK_LE(this->pivot_count(), (i32)InMemoryNode::kMaxTempPivots);
  });

  Subtree& child = this->children[pivot_i];

  StatusOr<Optional<Subtree>> status_or_sibling = child.try_split(update_context);
  if (!status_or_sibling.ok()) {
    LOG(ERROR) << BATT_INSPECT(child.get_viability());
  }
  BATT_REQUIRE_OK(status_or_sibling);

  if (!*status_or_sibling) {
    BATT_CHECK(batt::is_case<Viable>(child.get_viability()));
    return OkStatus();
  }
  Subtree& sibling = **status_or_sibling;

  const i32 sibling_i = pivot_i + 1;

  this->child_pages.insert(this->child_pages.begin() + sibling_i, llfs::PinnedPage{});

  llfs::PinnedPage& child_page = this->child_pages[pivot_i];
  llfs::PinnedPage& sibling_page = this->child_pages[sibling_i];

  BATT_ASSIGN_OK_RESULT(const KeyView child_max_key,
                        child.get_max_key(update_context.page_loader,  //
                                          update_context.overcommit,
                                          child_page));

  BATT_ASSIGN_OK_RESULT(const KeyView sibling_min_key,
                        sibling.get_min_key(update_context.page_loader,  //
                                            update_context.overcommit,
                                            sibling_page));

  //----- --- -- -  -  -   -
  // Update update_buffer levels.  This comes first because we use u64-based bit set for
  // active_pivots, and we assert that when we insert a new pivot (via split), it does not
  // overflow the bit set.
  //
  Interval<KeyView> pivot_key_range = in_node(*this).get_pivot_key_range(pivot_i);

  for (Level& level : this->update_buffer.levels) {
    BATT_REQUIRE_OK(batt::case_of(
        level,
        [](EmptyLevel&) -> Status {
          return OkStatus();
        },
        [](MergedLevel&) -> Status {
          return OkStatus();
        },
        [&](SegmentedLevel& segmented_level) -> Status {
          return in_segmented_level(*this,
                                    segmented_level,
                                    update_context.page_loader,
                                    update_context.overcommit)  //
              .split_pivot(pivot_i, pivot_key_range, sibling_min_key);
        }));
  }

  //----- --- -- -  -  -   -
  // Update children.
  //
  // This will cause pivot_count() to go up, which is why we must do it *after* inserting the
  // new pivot into the update buffer levels.
  //
  this->children.insert(this->children.begin() + sibling_i, std::move(sibling));

  //----- --- -- -  -  -   -
  // Update pending_bytes.
  //
  // We approximate how much of the pending count should belong to each side of the split by
  // just dividing it in half.  The count for each pivot will be fixed the next time we flush
  // to that pivot.
  //
  const usize both_pending_bytes = this->pending_bytes[pivot_i];
  const usize sibling_pending_bytes = both_pending_bytes / 2;
  this->pending_bytes[pivot_i] -= sibling_pending_bytes;

  BATT_CHECK_EQ(sibling_pending_bytes + this->pending_bytes[pivot_i], both_pending_bytes);

  // The pending bytes counts for this pivot and its new sibling are not exact.
  //
  this->pending_bytes_is_exact = set_bit(this->pending_bytes_is_exact, pivot_i, false);
  this->pending_bytes_is_exact = insert_bit(this->pending_bytes_is_exact, sibling_i, false);

  this->pending_bytes.insert(this->pending_bytes.begin() + sibling_i, sibling_pending_bytes);

  //----- --- -- -  -  -   -
  // Finally, insert a new pivot key.  Truncate as large a suffix as we can without losing the
  // ability to partition the keys on either side of the subtree split.
  //
  {
    const KeyView prefix = llfs::find_common_prefix(0, child_max_key, sibling_min_key);
    const KeyView new_sibling_pivot_key = sibling_min_key.substr(0, prefix.size() + 1);

    this->pivot_keys_.insert(this->pivot_keys_.begin() + sibling_i, new_sibling_pivot_key);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::set_pivot_items_flushed(BatchUpdateContext& update_context,
                                             usize pivot_i,
                                             const CInterval<KeyView>& flush_key_crange)
{
  Interval<KeyView> pivot_key_range = in_node(*this).get_pivot_key_range(pivot_i);

  BATT_CHECK_LE(pivot_key_range.lower_bound, flush_key_crange.lower_bound);
  BATT_CHECK_LT(flush_key_crange.upper_bound, pivot_key_range.upper_bound);

  Status segment_load_status;

  for (Level& level : this->update_buffer.levels) {
    bool is_now_empty = false;

    batt::case_of(  //
        level,      //
        [](EmptyLevel&) {
          // nothing to do
        },
        [&](MergedLevel& merged_level) {
          merged_level.result_set.drop_key_range(flush_key_crange);

          is_now_empty = merged_level.result_set.empty();
        },
        [&](SegmentedLevel& segmented_level) {
          segment_load_status.Update(in_segmented_level(*this,
                                                        segmented_level,
                                                        update_context.page_loader,
                                                        update_context.overcommit)
                                         .drop_key_range(pivot_i, flush_key_crange.upper_bound));

          is_now_empty = segmented_level.empty();
        });

    if (is_now_empty) {
      level = EmptyLevel{};
    }
  }

  if (this->is_size_tiered()) {
    this->squash_empty_levels();
  }

  return segment_load_status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::set_pivot_completely_flushed(usize pivot_i,
                                                  const Interval<KeyView>& pivot_key_range)
{
  for (Level& level : this->update_buffer.levels) {
    bool is_now_empty = false;

    batt::case_of(  //
        level,      //
        [](EmptyLevel&) {
          // nothing to do
        },
        [&](MergedLevel& merged_level) {
          merged_level.result_set.drop_key_range_half_open(pivot_key_range);

          is_now_empty = merged_level.result_set.empty();
        },
        [&](SegmentedLevel& segmented_level) {
          for (usize segment_i = 0; segment_i < segmented_level.segment_count();) {
            Segment& segment = segmented_level.get_segment(segment_i);

            segment.set_pivot_active(pivot_i, false);

            if (segment.is_inactive()) {
              segmented_level.drop_segment(segment_i);
            } else {
              ++segment_i;
            }
          }
          is_now_empty = segmented_level.empty();
        });

    if (is_now_empty) {
      level = EmptyLevel{};
    }
  }

  if (this->is_size_tiered()) {
    this->squash_empty_levels();
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::squash_empty_levels()
{
  BATT_CHECK(this->is_size_tiered());

  const usize level_count = this->update_buffer.levels.size();
  usize dst_i = 0;
  for (usize src_i = 0; src_i < level_count; ++src_i) {
    if (!batt::is_case<UpdateBuffer::EmptyLevel>(this->update_buffer.levels[src_i])) {
      if (src_i > dst_i) {
        this->update_buffer.levels[dst_i] = std::move(this->update_buffer.levels[src_i]);
      }
      ++dst_i;
    }
  }
  this->update_buffer.levels.resize(dst_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MaxPendingBytes InMemoryNode::find_max_pending() const
{
  const auto first_pending = this->pending_bytes.begin();
  const auto last_pending = this->pending_bytes.end();
  const auto max_pending = std::max_element(first_pending, last_pending);

  if (max_pending == last_pending) {
    return MaxPendingBytes{
        .pivot_index = 0,
        .byte_count = 0,
    };
  }
  return MaxPendingBytes{
      .pivot_index = static_cast<usize>(std::distance(first_pending, max_pending)),
      .byte_count = *max_pending,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::push_levels_to_merge(MergeCompactor& compactor,
                                        BatchUpdateContext& update_context,
                                        Status& segment_load_status,
                                        HasPageRefs& has_page_refs,
                                        const Slice<Level>& levels_to_merge,
                                        i32 min_pivot_i,
                                        bool only_pivot,
                                        Optional<KeyView> min_key)
{
  for (Level& level : levels_to_merge) {
    compactor.push_level(batt::case_of(  //
        level,                           //
        [](const EmptyLevel&) -> BoxedSeq<EditSlice> {
          return seq::Empty<EditSlice>{}  //
                 | seq::boxed();
        },
        [&](const MergedLevel& merged_level) -> BoxedSeq<EditSlice> {
          has_page_refs = HasPageRefs{has_page_refs || merged_level.result_set.has_page_refs()};
          return merged_level.result_set.live_edit_slices(this->get_pivot_key(min_pivot_i));
        },
        [&](const SegmentedLevel& segmented_level) -> BoxedSeq<EditSlice> {
          //----- --- -- -  -  -   -
          // TODO [tastolfi 2025-03-14] update has_page_refs here!
          //----- --- -- -  -  -   -
          if (only_pivot && !segmented_level.is_pivot_active(min_pivot_i)) {
            return seq::Empty<EditSlice>{}  //
                   | seq::boxed();
          }
          return SegmentedLevelScanner<const InMemoryNode, const SegmentedLevel, llfs::PageLoader>{
                     *this,
                     segmented_level,
                     update_context.page_loader,
                     llfs::PinPageToJob::kDefault,
                     update_context.overcommit,
                     segment_load_status,
                     min_pivot_i,
                     min_key}  //
                 | seq::boxed();
        }));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNode::segment_count() const
{
  usize total = 0;
  for (const Level& level : this->update_buffer.levels) {
    total += batt::case_of(
        level,
        [](const EmptyLevel&) -> usize {
          return 0;
        },
        [this](const MergedLevel& merged_level) -> usize {
          return merged_level.estimate_segment_count(this->tree_options);
        },
        [](const SegmentedLevel& segmented_level) -> usize {
          return segmented_level.segment_count();
        });
  }
  return total;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNode::key_data_byte_size() const
{
  usize total = packed_key_data_size(this->max_key_) +  //
                packed_key_data_size(this->common_key_prefix);

  for (const KeyView& key : this->pivot_keys_) {
    total += packed_key_data_size(key);
  }

  return total;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNode::segment_filters_byte_size() const
{
  usize count = this->total_segment_filter_cut_points();

  return llfs::packed_array_size<little_u32>(count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize InMemoryNode::total_segment_filter_cut_points() const
{
  usize count = 0;

  for (const Level& level : this->update_buffer.levels) {
    count += batt::case_of(  //
        level,               //
        [](const EmptyLevel&) -> usize {
          return 0;
        },
        [](const MergedLevel&) -> usize {
          return 0;
        },
        [](const SegmentedLevel& segmented_level) -> usize {
          usize n = 0;
          for (const Segment& segment : segmented_level.segments) {
            n += segment.num_cut_points();
          }
          return n;
        });
  }

  return count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SubtreeViability InMemoryNode::get_viability() const
{
  NeedsSplit needs_split;

  const usize variable_space = PackedNodePage::variable_data_space();
  const usize keys_size = this->key_data_byte_size();
  const usize segment_filters_size = this->segment_filters_byte_size();
  const bool variables_too_large = (keys_size + segment_filters_size) > variable_space;

  needs_split.pivot_count = BATT_CHECKED_CAST(u16, this->pivot_count());
  needs_split.segment_count = BATT_CHECKED_CAST(u16, this->segment_count());
  needs_split.height = BATT_CHECKED_CAST(i16, this->height);
  needs_split.too_many_pivots = (needs_split.pivot_count > this->max_pivot_count());
  needs_split.too_many_segments = (needs_split.segment_count > this->max_segment_count());
  needs_split.keys_too_large = (keys_size > variable_space);
  needs_split.segment_filters_too_large = (segment_filters_size != 0 && variables_too_large);

  if (needs_split) {
    return needs_split;
  }

  NeedsMerge needs_merge;

  needs_merge.single_pivot = (this->pivot_count() == 1);
  needs_merge.too_few_pivots = (this->pivot_count() < kMinPivots);

  if (needs_merge) {
    return needs_merge;
  }

  return Viable{};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNode::is_viable(IsRoot is_root) const
{
  return batt::case_of(
      this->get_viability(),
      [](const Viable&) {
        return /*is_viable()=*/true;
      },
      [](const NeedsSplit&) {
        return /*is_viable()=*/false;
      },
      [is_root](const NeedsMerge& needs_merge) {
        return is_root && !needs_merge.single_pivot;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::try_split(BatchUpdateContext& context)
{
  bool tried_compacting_levels = false;
  usize normal_flush_count = 0;

  for (;;) {
    StatusOr<std::unique_ptr<InMemoryNode>> upper_half = this->try_split_direct(context);
    if (upper_half.ok() || upper_half.status() != batt::StatusCode::kInternal) {
      return upper_half;
    }

    // Check to see whether various fixes might avoid the failure to split...
    //
    SubtreeViability viability = this->get_viability();

    // If the only thing preventing us from splitting the node is related to there being a lot
    // of segments/levels, then we can try fixing this by re-compacting the entire update
    // buffer.
    //
    if (compacting_levels_might_fix(viability) && !tried_compacting_levels &&
        this->update_buffer.count_non_empty_levels() > 1) {
      BATT_REQUIRE_OK(this->compact_update_buffer_levels(context));
      tried_compacting_levels = true;
      continue;
    }

    // If we need to reduce the number of levels, and there is enough pending update data to
    // reflush to the most recently flushed pivot, then try doing that.
    //
    {
      // The reason we are only considering the latest pivot to be flushed is that we are
      // probably inside try_split because of another flush, and we don't want to deal with any
      // issues arising from two splits (i.e., different pivots) at once.
      //
      const i32 reflush_pivot_i = this->latest_flush_pivot_i_.value_or((i32)this->pivot_count());

      const bool recently_flushed = reflush_pivot_i < (i32)this->pivot_count();

      const bool can_reflush = recently_flushed && (this->pending_bytes[reflush_pivot_i] >=
                                                    this->tree_options.min_flush_size());
      if (can_reflush) {
        BATT_REQUIRE_OK(this->flush_to_pivot(context, reflush_pivot_i));
        continue;
      }
    }

    // If max flush factor is >1 and we are at the bottom of the tree, then we can attempt a normal
    // flush, so long as the number of pivots is under the split limit.
    //
    if (this->tree_options.max_flush_factor() > 1 && normal_flush_count < this->pivot_count() &&
        normal_flush_might_fix(viability)) {
      Optional<i32> saved_pivot_i = this->latest_flush_pivot_i_;
      this->latest_flush_pivot_i_ = None;
      Status flush_status = this->flush_if_necessary(context);
      if (flush_status.ok() && this->latest_flush_pivot_i_) {
        ++normal_flush_count;
        continue;
      }
      this->latest_flush_pivot_i_ = saved_pivot_i;
    }

    LOG(ERROR) << "Failed to split node;" << BATT_INSPECT(this->tree_options.flush_size())
               << BATT_INSPECT(this->tree_options.min_flush_size())
               << BATT_INSPECT(this->tree_options.max_flush_size())
               << BATT_INSPECT(this->update_buffer.levels.size()) << BATT_INSPECT(viability) << "\n"
               << BATT_INSPECT_RANGE(this->pending_bytes) << "\n"
               << BATT_INSPECT(compacting_levels_might_fix(viability)) << "\n"
               << BATT_INSPECT(normal_flush_might_fix(viability)) << "\n"
               << boost::stacktrace::stacktrace{};

    return upper_half;
  }
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::try_split_direct(BatchUpdateContext& context)
{
  const SubtreeViability orig_viability = this->get_viability();

  if (!batt::is_case<NeedsSplit>(orig_viability)) {
    return {nullptr};
  }

  const usize orig_pivot_count = this->pivot_count();
  SmallVec<KeyView, 65> orig_pivot_keys = std::move(this->pivot_keys_);
  SmallVec<Level, 6> orig_levels = std::move(this->update_buffer.levels);
  SmallVec<Subtree, 64> orig_children = std::move(this->children);
  SmallVec<llfs::PinnedPage, 64> orig_child_pages = std::move(this->child_pages);
  SmallVec<usize, 64> orig_pending_bytes = std::move(this->pending_bytes);
  KeyView orig_max_key = this->max_key_;
  const u64 orig_pending_bytes_is_exact = this->pending_bytes_is_exact;

  auto reset_this_on_failure = batt::finally([&] {
    this->pivot_keys_ = std::move(orig_pivot_keys);
    this->update_buffer.levels = std::move(orig_levels);
    this->children = std::move(orig_children);
    this->child_pages = std::move(orig_child_pages);
    this->pending_bytes = std::move(orig_pending_bytes);
    this->max_key_ = orig_max_key;
    this->pending_bytes_is_exact = orig_pending_bytes_is_exact;
  });

  this->children.clear();
  this->child_pages.clear();
  this->pending_bytes.clear();
  this->pending_bytes_is_exact = u64{0};

  BATT_CHECK_EQ(orig_pivot_count + 1, orig_pivot_keys.size());

  ActivePivotsSet128 tried_already;
  usize split_pivot_i = (orig_pivot_count + 1) / 2;

  auto* node_lower_half = this;
  auto node_upper_half = std::make_unique<InMemoryNode>(batt::make_copy(this->pinned_node_page_),
                                                        this->tree_options,
                                                        this->is_size_tiered());
  node_upper_half->height = this->height;

  SubtreeViability lower_viability;
  SubtreeViability upper_viability;

  for (;;) {
    // If we ever try the same split point a second time, fail.
    //
    if (tried_already.get(split_pivot_i)) {
      return {batt::StatusCode::kInternal};
    }
    tried_already.set(split_pivot_i, true);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // Reset pivot keys and buffer levels for both halves.
    //
    node_lower_half->pivot_keys_.clear();
    node_upper_half->pivot_keys_.clear();

    node_lower_half->update_buffer.levels.clear();
    node_upper_half->update_buffer.levels.clear();

    node_lower_half->update_buffer.levels.resize(orig_levels.size());
    node_upper_half->update_buffer.levels.resize(orig_levels.size());

    // Populate the lower and upper pivot keys.
    //
    node_lower_half->pivot_keys_.assign(orig_pivot_keys.begin(),
                                        orig_pivot_keys.begin() + split_pivot_i + 1);

    node_upper_half->pivot_keys_.assign(orig_pivot_keys.begin() + split_pivot_i,
                                        orig_pivot_keys.end());

    BATT_CHECK_EQ(node_lower_half->pivot_keys_.back(), node_upper_half->pivot_keys_.front());
    BATT_CHECK_EQ(node_lower_half->pivot_keys_.size() + node_upper_half->pivot_keys_.size(),
                  orig_pivot_keys.size() + 1);

    // Initialize children of node_lower_half.
    //
    node_lower_half->children.resize(node_lower_half->pivot_keys_.size() - 1);

    const usize lower_half_pivot_count = node_lower_half->children.size();

    BATT_CHECK_EQ(split_pivot_i, lower_half_pivot_count);

    node_lower_half->pending_bytes.resize(lower_half_pivot_count);
    node_lower_half->pending_bytes_is_exact = 0;
    node_lower_half->child_pages.resize(lower_half_pivot_count);

    for (usize i = 0; i < lower_half_pivot_count; ++i) {
      node_lower_half->children[i] = std::move(orig_children[i]);
      node_lower_half->child_pages[i] = std::move(orig_child_pages[i]);
      node_lower_half->pending_bytes[i] = orig_pending_bytes[i];
    }

    BATT_ASSIGN_OK_RESULT(
        node_lower_half->max_key_,
        node_lower_half->children.back().get_max_key(context.page_loader,
                                                     context.overcommit,
                                                     node_lower_half->child_pages.back()));

    node_lower_half->common_key_prefix = "";  // TODO [tastolfi 2025-03-17]

    // Initialize children of node_upper_half.
    //
    node_upper_half->children.resize(node_upper_half->pivot_keys_.size() - 1);

    const usize upper_half_pivot_count = node_upper_half->children.size();

    BATT_CHECK_EQ(split_pivot_i + upper_half_pivot_count, orig_children.size());

    node_upper_half->pending_bytes.resize(upper_half_pivot_count);
    node_upper_half->pending_bytes_is_exact = 0;
    node_upper_half->child_pages.resize(upper_half_pivot_count);

    for (usize i = 0; i < upper_half_pivot_count; ++i) {
      const usize orig_i = i + split_pivot_i;
      node_upper_half->children[i] = std::move(orig_children[orig_i]);
      node_upper_half->child_pages[i] = std::move(orig_child_pages[orig_i]);
      node_upper_half->pending_bytes[i] = orig_pending_bytes[orig_i];
    }

    node_upper_half->max_key_ = orig_max_key;
    node_upper_half->common_key_prefix = "";  // TODO [tastolfi 2025-03-17]

    // Split the original update buffer according to the split point.
    //
    for (usize level_i = 0; level_i < orig_levels.size(); ++level_i) {
      Level& lower_half_level = node_lower_half->update_buffer.levels[level_i];
      Level& upper_half_level = node_upper_half->update_buffer.levels[level_i];

      batt::case_of(orig_levels[level_i], [&](const auto& level_case) {
        in_node(*this).split_level(level_case,
                                   split_pivot_i,
                                   lower_half_level,
                                   upper_half_level,
                                   context.page_loader,
                                   this->tree_options);
      });
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // Now evaluate whether the split works.
    //
    lower_viability = node_lower_half->get_viability();
    upper_viability = node_upper_half->get_viability();

    // If both halves are ok, then break out of the loop and finish building each half-node.
    //
    if (batt::is_case<Viable>(lower_viability) && batt::is_case<Viable>(upper_viability)) {
      reset_this_on_failure.cancel();
      break;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    // The split is no good.  We must restore orig_children and orig_child_pages to their initial
    // state and see if we have other options for where to split.
    //
    for (usize i = 0; i < lower_half_pivot_count; ++i) {
      orig_children[i] = std::move(node_lower_half->children[i]);
      orig_child_pages[i] = std::move(node_lower_half->child_pages[i]);
    }
    for (usize i = 0; i < upper_half_pivot_count; ++i) {
      const usize orig_i = i + split_pivot_i;
      orig_children[orig_i] = std::move(node_upper_half->children[i]);
      orig_child_pages[orig_i] = std::move(node_upper_half->child_pages[i]);
    }

    // If the lower half is too large, then move the split point down and retry if possible.
    //
    if (split_pivot_i > kMinPivots && batt::is_case<NeedsSplit>(lower_viability) &&
        !batt::is_case<NeedsSplit>(upper_viability)) {
      --split_pivot_i;
      continue;
    }

    // If the upper half is too large, then move the split point up and retry if possible.
    //
    if (split_pivot_i + kMinPivots < orig_pivot_count &&
        batt::is_case<NeedsSplit>(upper_viability) && !batt::is_case<NeedsSplit>(lower_viability)) {
      ++split_pivot_i;
      continue;
    }
  }

  // Final sanity checks.
  //
  BATT_CHECK_EQ(node_lower_half->pivot_count() + node_upper_half->pivot_count(), orig_pivot_count);

  BATT_CHECK_EQ(node_lower_half->is_size_tiered(), node_upper_half->is_size_tiered());

  BATT_CHECK(!batt::is_case<NeedsSplit>(node_lower_half->get_viability()))
      << BATT_INSPECT(node_lower_half->get_viability())
      << BATT_INSPECT(node_lower_half->update_buffer.dump());

  BATT_CHECK(!batt::is_case<NeedsSplit>(node_upper_half->get_viability()))
      << BATT_INSPECT(node_upper_half->get_viability())
      << BATT_INSPECT(node_upper_half->update_buffer.dump());

  return {std::move(node_upper_half)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> InMemoryNode::find_key(KeyQuery& query) const
{
  return in_node(*this).find_key(query);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> InMemoryNode::find_key_in_level(usize level_i,
                                                    KeyQuery& query,
                                                    i32 key_pivot_i) const
{
  const Level& level = this->update_buffer.levels[level_i];

  return batt::case_of(
      level,
      [&](const EmptyLevel&) -> StatusOr<ValueView> {
        return {batt::StatusCode::kNotFound};
      },
      [&](const MergedLevel& merged_level) -> StatusOr<ValueView> {
        return merged_level.result_set.find_key(query.key());
      },
      [&](const SegmentedLevel& segmented_level) -> StatusOr<ValueView> {
        return in_segmented_level(*this, segmented_level, *query.page_loader, query.overcommit())
            .find_key(key_pivot_i, query);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNode::is_packable() const
{
  for (const Level& level : this->update_buffer.levels) {
    if (batt::is_case<MergedLevel>(level)) {
      return false;
    }
  }

  for (const Subtree& child : this->children) {
    if (!child.is_serialized()) {
      return false;
    }
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::start_serialize(TreeSerializeContext& context)
{
  BATT_CHECK(!batt::is_case<NeedsSplit>(this->get_viability()))
      << BATT_INSPECT(this->get_viability()) << BATT_INSPECT(this->pivot_count());

  usize total_segments = 0;

  for (Level& level : this->update_buffer.levels) {
    BATT_REQUIRE_OK(    //
        batt::case_of(  //
            level,      //
            [](const EmptyLevel&) -> Status {
              return OkStatus();
            },
            [this, &context, &total_segments](MergedLevel& merged_level) -> Status {
              BATT_ASSIGN_OK_RESULT(usize segment_count,
                                    merged_level.start_serialize(*this, context));
              total_segments += segment_count;
              return OkStatus();
            },
            [&total_segments](const SegmentedLevel& segmented_level) -> Status {
              total_segments += segmented_level.segment_count();
              return OkStatus();
            }));
  }

  BATT_CHECK_LE(total_segments, this->max_segment_count());

  for (Subtree& child : this->children) {
    BATT_REQUIRE_OK(child.start_serialize(context));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PageId> InMemoryNode::finish_serialize(TreeSerializeContext& context)
{
  static Metrics& r_metrics = Self::metrics();
  //----- --- -- -  -  -   -
  r_metrics.serialized_node_count.add(1);
  r_metrics.serialized_pivot_count.add(this->pivot_count());
  r_metrics.level_depth_stats.update(this->update_buffer.levels.size());

  for (Level& level : this->update_buffer.levels) {
    Optional<SegmentedLevel> new_segmented_level;

    BATT_REQUIRE_OK(    //
        batt::case_of(  //
            level,      //
            [](const EmptyLevel&) -> Status {
              return OkStatus();
            },
            [this, &context, &new_segmented_level](MergedLevel& merged_level) -> Status {
              r_metrics.serialized_nonempty_level_count.add(1);
              StatusOr<SegmentedLevel> result = merged_level.finish_serialize(*this, context);
              if (result.ok()) {
                new_segmented_level.emplace(std::move(*result));
                r_metrics.serialized_buffer_segment_count.add(result->segment_count());
              }
              return result.status();
            },
            [](const SegmentedLevel& segmented_level) -> Status {
              r_metrics.serialized_nonempty_level_count.add(1);
              r_metrics.serialized_buffer_segment_count.add(segmented_level.segment_count());
              return OkStatus();
            }));

    if (new_segmented_level) {
      level = std::move(*new_segmented_level);
    }
  }

  for (Subtree& child : this->children) {
    BATT_REQUIRE_OK(child.finish_serialize(context));
    BATT_CHECK(child.is_serialized());
  }

  BATT_CHECK(this->is_packable());

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BATT_CHECK_OK(NodePageView::register_layout(context.page_job().cache()));

  StatusOr<std::shared_ptr<llfs::PageBuffer>> node_page_buffer =
      context.page_job().new_page(this->tree_options.node_size(),
                                  batt::WaitForResource::kTrue,
                                  context.overcommit(),
                                  NodePageView::page_layout_id(),
                                  llfs::LruPriority{kNewNodeLruPriority},
                                  /*callers=*/0,
                                  context.cancel_token());

  if (!node_page_buffer.ok()) {
    LOG(ERROR) << BATT_INSPECT(this->tree_options.node_size());
  }

  BATT_REQUIRE_OK(node_page_buffer);

  const llfs::PageId new_page_id = (*node_page_buffer)->page_id();

  const PackedNodePage* packed_node = build_node_page((*node_page_buffer)->mutable_buffer(), *this);
  BATT_CHECK_NOT_NULLPTR(packed_node);

  BATT_REQUIRE_OK(
      context.page_job().pin_new(std::make_shared<NodePageView>(std::move(*node_page_buffer)),
                                 llfs::LruPriority{kNewNodeLruPriority},
                                 /*callers=*/0));

  return new_page_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> Segment::load_leaf_page(llfs::PageLoader& page_loader,
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
StatusOr<usize> MergedLevel::start_serialize(const InMemoryNode& node,
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
                usize task_i,
                llfs::PageCache& page_cache,
                llfs::PageBuffer& page_buffer) -> TreeSerializeContext::PinPageToJobFn {
              //----- --- -- -  -  -   -
              const auto all_items_in_level = this->result_set.get();
              const auto items_in_this_page = batt::slice_range(all_items_in_level, part_extents);

              if (task_i == 0) {
                return build_leaf_page_in_job(node.tree_options.trie_index_reserve_size(),
                                              page_buffer,
                                              items_in_this_page);
              }
              BATT_CHECK_EQ(task_i, 1);

              return build_filter_for_leaf_in_job(page_cache,
                                                  overcommit_triggered,
                                                  filter_bits_per_key,
                                                  filter_page_size,
                                                  page_buffer.page_id(),
                                                  items_in_this_page);
            }));

    this->segment_future_ids_.emplace_back(id);
  }

  return page_parts.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SegmentedLevel> MergedLevel::finish_serialize(const InMemoryNode& node,
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
void InMemoryNode::UpdateBuffer::SegmentedLevel::drop_segment(usize i)
{
  this->segments.erase(this->segments.begin() + i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::UpdateBuffer::SegmentedLevel::drop_pivot_range(
    const Interval<i32>& pivot_i_range,
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
void InMemoryNode::UpdateBuffer::SegmentedLevel::drop_before_pivot(i32 pivot_i,
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
void InMemoryNode::UpdateBuffer::SegmentedLevel::drop_after_pivot(i32 pivot_i,
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
bool InMemoryNode::UpdateBuffer::SegmentedLevel::is_pivot_active(i32 pivot_i) const
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
void InMemoryNode::UpdateBuffer::SegmentedLevel::check_items_sorted(
    const InMemoryNode& node,
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::UpdateBuffer::Segment::check_invariants(const char* file, int line) const
{
  BATT_CHECK(this->filter.check_invariants()) << BATT_INSPECT(file) << BATT_INSPECT(line);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::UpdateBuffer::Segment::insert_pivot(i32 pivot_i, bool is_active)
{
  this->check_invariants(__FILE__, __LINE__);
  auto on_scope_exit = batt::finally([&] {
    this->check_invariants(__FILE__, __LINE__);
  });

  this->active_pivots.insert(pivot_i, is_active);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void InMemoryNode::UpdateBuffer::Segment::pop_front_pivots(i32 count)
{
  BATT_CHECK_LT(count, 64);

  // Shift the active pivot sets down by count.
  //
  this->active_pivots.pop_front_pivots(count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool InMemoryNode::UpdateBuffer::Segment::is_inactive() const
{
  const bool inactive = this->active_pivots.is_empty();
  if (inactive) {
    Slice<const Interval<u32>> filter_dropped_ranges = this->filter.dropped();
    BATT_CHECK_EQ(filter_dropped_ranges.size(), 1);
    BATT_CHECK_EQ(filter_dropped_ranges[0].lower_bound, 0);
  }
  return inactive;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNode::UpdateBuffer::dump() const
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNode::UpdateBuffer::EmptyLevel::dump() const
{
  return [](std::ostream& out) {
    out << "EmptyLevel{}";
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNode::UpdateBuffer::MergedLevel::dump() const
{
  return [this](std::ostream& out) {
    out << "MergedLevel{" << this->result_set.debug_dump("    ") << "\n}";
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNode::UpdateBuffer::SegmentedLevel::dump() const
{
  return [this](std::ostream& out) {
    out << "SegmentedLevel{\n";
    for (const Segment& segment : this->segments) {
      out << "    " << segment.dump(/*multi_line=*/false) << ",\n";
    }
    out << "  }";
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNode::UpdateBuffer::Segment::dump(bool multi_line) const
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

}  // namespace turtle_kv
