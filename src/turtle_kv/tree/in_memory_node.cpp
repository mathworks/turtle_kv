#include <turtle_kv/tree/in_memory_node.hpp>
//

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/core/algo/split_parts.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <batteries/case_of.hpp>

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
  auto node = std::make_unique<InMemoryNode>(std::move(pinned_node_page), tree_options);

  const usize pivot_count = packed_node.pivot_count;

  node->tree_options = tree_options;
  node->height = packed_node.height;
  node->children.resize(pivot_count);
  node->child_pages.resize(pivot_count);
  node->pending_bytes.resize(packed_node.pivot_count);
  node->pivot_keys_.resize(packed_node.pivot_count + 1);
  node->max_key_ = packed_node.max_key();
  node->common_key_prefix = packed_node.common_key_prefix();

  for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
    node->children[pivot_i] = Subtree{
        .impl = llfs::PageIdSlot::from_page_id(packed_node.children[pivot_i].unpack()),
    };
    node->pending_bytes[pivot_i] = packed_node.pending_bytes[pivot_i];
    node->pivot_keys_[pivot_i] = packed_node.get_pivot_key(pivot_i);
  }
  node->pivot_keys_[pivot_count] = packed_node.get_pivot_key(pivot_count);

  // Unpack the update buffer.
  //
  node->update_buffer.levels.resize(tree_options.max_buffer_levels());
  const usize in_memory_level_count = node->update_buffer.levels.size();

  for (usize level_i = 0; level_i < PackedNodePage::kMaxLevels; ++level_i) {
    const PackedLevel level = packed_node.update_buffer.get_level(level_i);
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

      for (usize segment_i = 0; segment_i < segment_count; ++segment_i) {
        const PackedNodePage::UpdateBuffer::Segment& packed_segment = level_segments[segment_i];
        Segment& segment = segmented_level.segments[segment_i];

        segment.page_id_slot = llfs::PageIdSlot::from_page_id(packed_segment.leaf_page_id.unpack());
        segment.active_pivots = packed_segment.active_pivots;
        segment.flushed_pivots = packed_segment.flushed_pivots;

        Slice<const little_u32> packed_flushed_item_upper_bounds =
            packed_node.update_buffer.get_flushed_item_upper_bounds(level_i, segment_i);

        BATT_CHECK_EQ(packed_flushed_item_upper_bounds.size(),
                      bit_count(segment.get_flushed_pivots()));

        for (const little_u32& upper_bound : packed_flushed_item_upper_bounds) {
          segment.flushed_item_upper_bound_.emplace_back(upper_bound);
        }

        segment.check_invariants(__FILE__, __LINE__);
      }
    }
  }

  return {std::move(node)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::from_subtrees(
    llfs::PageLoader& page_loader,
    const TreeOptions& tree_options,
    Subtree&& first_subtree,
    Subtree&& second_subtree,
    const KeyView& key_upper_bound,
    IsRoot is_root)
{
  auto new_node = std::make_unique<InMemoryNode>(llfs::PinnedPage{}, tree_options);

  BATT_ASSIGN_OK_RESULT(const i32 first_height, first_subtree.get_height(page_loader));
  BATT_ASSIGN_OK_RESULT(const i32 second_height, second_subtree.get_height(page_loader));

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
    BATT_ASSIGN_OK_RESULT(new_node->pivot_keys_[0],
                          new_node->children[0].get_min_key(page_loader, new_node->child_pages[0]));
  }

  BATT_ASSIGN_OK_RESULT(const KeyView first_child_max_key,
                        new_node->children[0].get_max_key(page_loader, new_node->child_pages[0]));

  BATT_ASSIGN_OK_RESULT(const KeyView second_child_min_key,
                        new_node->children[1].get_min_key(page_loader, new_node->child_pages[1]));

  const KeyView prefix = llfs::find_common_prefix(0, first_child_max_key, second_child_min_key);

  new_node->pivot_keys_[1] = second_child_min_key.substr(0, prefix.size() + 1);
  new_node->pivot_keys_[2] = key_upper_bound;

  BATT_ASSIGN_OK_RESULT(new_node->max_key_,
                        new_node->children[1].get_max_key(page_loader, new_node->child_pages[1]));

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
  in_node(*this).update_pending_bytes(update.worker_pool,
                                      update.result_set.get(),
                                      PackedSizeOfEdit{});

  // Merge the update batch into the buffer.
  //
  BATT_REQUIRE_OK(this->update_buffer_insert(update));

  // Check for flush.
  //
  BATT_REQUIRE_OK(this->flush_if_necessary(update));

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

  BATT_ASSIGN_OK_RESULT(
      new_merged_level.result_set,
      update.merge_compact_edits(global_max_key(),
                                 [&](MergeCompactor::GeneratorContext& context) -> Status {
                                   MergeFrame frame;
                                   frame.push_line(update.result_set.live_edit_slices());
                                   this->push_levels_to_merge(frame,
                                                              update.page_loader,
                                                              segment_load_status,
                                                              has_page_refs,
                                                              levels_to_merge);
                                   context.push_frame(&frame);
                                   return context.await_frame_consumed(&frame);
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
  // Insert the new level into the log.

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
Status InMemoryNode::flush_if_necessary(BatchUpdate& update, bool force_flush)
{
  // If we have enough buffered edit bytes on some pivot to flush, then do it.
  //
  const MaxPendingBytes max_pending = this->find_max_pending();

  if (!force_flush && max_pending.byte_count < this->tree_options.min_flush_size()) {
    return OkStatus();
  }

  return this->flush_to_pivot(update, max_pending.pivot_index);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::try_flush(batt::WorkerPool& worker_pool,
                               llfs::PageLoader& page_loader,
                               const batt::CancelToken& cancel_token)
{
  BatchUpdate fake_update{
      .worker_pool = worker_pool,
      .page_loader = page_loader,
      .cancel_token = cancel_token,
      .result_set = {},
      .edit_size_totals = None,
  };

  return this->flush_if_necessary(fake_update, /*force_flush=*/true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::flush_to_pivot(BatchUpdate& update, i32 pivot_i)
{
  Interval<KeyView> pivot_key_range = in_node(*this).get_pivot_key_range(pivot_i);
  BatchUpdate child_update = update.make_child_update();

  // Merge/compact all edits in the pivot's key range.
  //
  {
    Status segment_load_status;
    HasPageRefs has_page_refs{false};

    BATT_ASSIGN_OK_RESULT(                            //
        child_update.result_set,                      //
        update.merge_compact_edits(                   //
            /*max_key=*/pivot_key_range.upper_bound,  //
            [&](MergeCompactor::GeneratorContext& context) -> Status {
              MergeFrame frame;
              this->push_levels_to_merge(frame,
                                         update.page_loader,
                                         segment_load_status,
                                         has_page_refs,
                                         as_slice(this->update_buffer.levels),
                                         /*min_pivot=*/pivot_i);
              context.push_frame(&frame);
              return context.await_frame_consumed(&frame);
            }));

    BATT_REQUIRE_OK(segment_load_status);
  }

  // Make sure the result set doesn't contain the first key from the next pivot.
  //
  child_update.result_set.drop_key_range_half_open(Interval<KeyView>{
      pivot_key_range.upper_bound,
      this->key_upper_bound(),
  });

  // Reset edit size totals.
  //
  child_update.edit_size_totals = None;

  // Take the largest prefix of the merged edits as possible, without making the flushed batch too
  // large.
  //
  const usize max_flush_size =
      (this->height == 2) ? this->tree_options.flush_size() : this->tree_options.max_flush_size();

  const usize byte_size_limit = max_flush_size - (this->tree_options.max_item_size() - 1);

  BatchUpdate::TrimResult trim_result = child_update.trim_back_down_to_size(byte_size_limit);

  // Calculate the flushed key range.
  //
  CInterval<KeyView> flush_key_crange = child_update.get_key_crange();

  BATT_CHECK_LE(pivot_key_range.lower_bound, flush_key_crange.lower_bound);
  BATT_CHECK_LT(flush_key_crange.upper_bound, pivot_key_range.upper_bound);

  flush_key_crange.lower_bound = pivot_key_range.lower_bound;

  // Mark all keys in the child update as flushed.
  //
  BATT_REQUIRE_OK(this->set_pivot_items_flushed(update.page_loader, pivot_i, flush_key_crange));

  // Update pending bytes for the flushed pivot; this is equal to the number of bytes we had to trim
  // from the end of the batch to make it fit under the limit.
  //
  this->pending_bytes[pivot_i] = trim_result.n_bytes_trimmed;

  // Recursively apply batch update.
  //
  Subtree& child = this->children[pivot_i];

  BATT_REQUIRE_OK(child.apply_batch_update(this->tree_options,              //
                                           ParentNodeHeight{this->height},  //
                                           child_update,                    //
                                           /*key_upper_bound=*/this->get_pivot_key(pivot_i + 1),
                                           IsRoot{false}));

  // TODO [tastolfi 2025-03-16] put this case_of in a helper function.
  //
  return batt::case_of(       //
      child.get_viability(),  //

      //----- --- -- -  -  -   -
      [](const Viable&) -> Status {
        return OkStatus();
      },

      //----- --- -- -  -  -   -
      [&](const NeedsSplit& needs_split) -> Status {
        if (needs_split.too_many_segments && !needs_split.too_many_pivots &&
            !needs_split.keys_too_large) {
          // If the only thing that's stopping the child from being viable is that it has too many
          // buffer segments, then attempt to fix the problem by flushing.
          //
          Status child_flush_status =
              child.try_flush(update.worker_pool, update.page_loader, update.cancel_token);

          if (child_flush_status.ok() && batt::is_case<Viable>(child.get_viability())) {
            return OkStatus();
          }
          //
          // else - fall through and try a regular split.
        }

        StatusOr<Subtree> sibling = child.try_split(update.page_loader);
        BATT_REQUIRE_OK(sibling);

        const i32 sibling_pivot_i = pivot_i + 1;

        this->child_pages.insert(this->child_pages.begin() + sibling_pivot_i, llfs::PinnedPage{});

        BATT_ASSIGN_OK_RESULT(            //
            const KeyView child_max_key,  //
            child.get_max_key(update.page_loader, this->child_pages[pivot_i]));

        BATT_ASSIGN_OK_RESULT(              //
            const KeyView sibling_min_key,  //
            sibling->get_min_key(update.page_loader, this->child_pages[sibling_pivot_i]));

        //----- --- -- -  -  -   -
        // Update update_buffer levels.  This comes first because we use u64-based bit sets for
        // active_pivots and flushed_pivots, and we assert that when we insert a new pivot (via
        // split), it does not overflow the bit set.
        //
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
                return in_segmented_level(*this, segmented_level, update.page_loader)  //
                    .split_pivot(pivot_i, pivot_key_range, sibling_min_key);
              }));
        }

        //----- --- -- -  -  -   -
        // Update children.
        //
        // This will cause pivot_count() to go up, which is why we must do it *after* inserting the
        // new pivot into the update buffer levels.
        //
        this->children.insert(this->children.begin() + sibling_pivot_i, std::move(*sibling));

        //----- --- -- -  -  -   -
        // Update pending_bytes.
        //
        // We approximate how much of the pending count should belong to each side of the split by
        // just dividing it in half.  The count for each pivot will be fixed the next time we flush
        // to that pivot.
        //
        this->pending_bytes.insert(this->pending_bytes.begin() + sibling_pivot_i, 0);
        this->pending_bytes[sibling_pivot_i] = this->pending_bytes[pivot_i] / 2;
        this->pending_bytes[pivot_i] -= this->pending_bytes[sibling_pivot_i];

        //----- --- -- -  -  -   -
        // Finally, insert a new pivot key.  Truncate as large a suffix as we can without losing the
        // ability to partition the keys on either side of the subtree split.
        //
        {
          const KeyView prefix = llfs::find_common_prefix(0, child_max_key, sibling_min_key);
          const KeyView new_sibling_pivot_key = sibling_min_key.substr(0, prefix.size() + 1);

          this->pivot_keys_.insert(this->pivot_keys_.begin() + sibling_pivot_i,
                                   new_sibling_pivot_key);
        }

        return OkStatus();
      },

      //----- --- -- -  -  -   -
      [&](const NeedsMerge&) -> Status {
        BATT_PANIC() << "TODO [tastolfi 2025-03-16] implement me!";
        return batt::StatusCode::kUnimplemented;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryNode::set_pivot_items_flushed(llfs::PageLoader& page_loader,
                                             usize pivot_i,
                                             const CInterval<KeyView>& flush_key_crange)
{
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
          segment_load_status.Update(
              in_segmented_level(*this, segmented_level, page_loader)
                  .flush_pivot_up_to_key(pivot_i, flush_key_crange.upper_bound));

          is_now_empty = segmented_level.empty();
        });

    if (is_now_empty) {
      level = EmptyLevel{};
    }
  }

  return segment_load_status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MaxPendingBytes InMemoryNode::find_max_pending() const
{
  // TODO [tastolfi 2021-09-01] use parallel_accumulate here?
  //
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
void InMemoryNode::push_levels_to_merge(MergeFrame& frame,
                                        llfs::PageLoader& page_loader,
                                        Status& segment_load_status,
                                        HasPageRefs& has_page_refs,
                                        const Slice<Level>& levels_to_merge,
                                        i32 min_pivot_i)
{
  for (Level& level : levels_to_merge) {
    frame.push_line(batt::case_of(  //
        level,                      //
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
          return SegmentedLevelScanner<const InMemoryNode, const SegmentedLevel, llfs::PageLoader>{
                     *this,
                     segmented_level,
                     page_loader,
                     llfs::PinPageToJob::kDefault,
                     segment_load_status,
                     min_pivot_i,
                 }  //
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
usize InMemoryNode::flushed_item_counts_byte_size() const
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
            n += bit_count(segment.get_flushed_pivots());
          }
          return n;
        });
  }

  return count * sizeof(little_u32);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SubtreeViability InMemoryNode::get_viability() const
{
  NeedsSplit needs_split;

  const usize variable_space = sizeof(PackedNodePage::key_and_flushed_item_data_);
  const usize keys_size = this->key_data_byte_size();
  const usize counts_size = this->flushed_item_counts_byte_size();
  const bool variables_too_large = (keys_size + counts_size) > variable_space;

  needs_split.too_many_pivots = (this->pivot_count() > Self::kMaxPivotCount);
  needs_split.too_many_segments = (this->segment_count() > Self::kMaxSegmentCount);
  needs_split.keys_too_large = (keys_size != 0 && variables_too_large);
  needs_split.flushed_item_counts_too_large = (counts_size != 0 && variables_too_large);

  if (needs_split) {
    return needs_split;
  }

  NeedsMerge needs_merge;

  needs_merge.single_pivot = (this->pivot_count() == 1);
  needs_merge.too_few_pivots = (this->pivot_count() < 4);

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
StatusOr<std::unique_ptr<InMemoryNode>> InMemoryNode::try_split(llfs::PageLoader& page_loader)
{
  const usize orig_pivot_count = this->pivot_count();
  SmallVec<KeyView, 65> orig_pivot_keys = std::move(this->pivot_keys_);
  SmallVec<Level, 6> orig_levels = std::move(this->update_buffer.levels);
  SmallVec<Subtree, 64> orig_children = std::move(this->children);
  SmallVec<llfs::PinnedPage, 64> orig_child_pages = std::move(this->child_pages);
  SmallVec<usize, 64> orig_pending_bytes = std::move(this->pending_bytes);
  KeyView orig_max_key = this->max_key_;

  auto reset_this_on_failure = batt::finally([&] {
    this->pivot_keys_ = std::move(orig_pivot_keys);
    this->update_buffer.levels = std::move(orig_levels);
    this->children = std::move(orig_children);
    this->child_pages = std::move(orig_child_pages);
    this->pending_bytes = std::move(orig_pending_bytes);
  });

  this->children.clear();
  this->child_pages.clear();
  this->pending_bytes.clear();

  BATT_CHECK_EQ(orig_pivot_count + 1, orig_pivot_keys.size());

  u64 tried_already = 0;
  usize split_pivot_i = (orig_pivot_count + 1) / 2;

  auto* node_lower_half = this;
  auto node_upper_half =
      std::make_unique<InMemoryNode>(batt::make_copy(this->pinned_node_page_), this->tree_options);

  for (;;) {
    // If we ever try the same split point a second time, fail.
    //
    if (get_bit(tried_already, split_pivot_i)) {
      return {batt::StatusCode::kInternal};
    }
    tried_already = set_bit(tried_already, split_pivot_i, true);

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

    node_lower_half->children.resize(node_lower_half->pivot_keys_.size() - 1);

    node_upper_half->pivot_keys_.assign(orig_pivot_keys.begin() + split_pivot_i,
                                        orig_pivot_keys.end());

    node_upper_half->children.resize(node_upper_half->pivot_keys_.size() - 1);

    BATT_CHECK_EQ(node_lower_half->pivot_keys_.back(), node_upper_half->pivot_keys_.front());
    BATT_CHECK_EQ(node_lower_half->pivot_keys_.size() + node_upper_half->pivot_keys_.size(),
                  orig_pivot_keys.size() + 1);

    // Split the original update buffer according to the split point.
    //
    for (usize level_i = 0; level_i < orig_levels.size(); ++level_i) {
      Level& lower_half_level = node_lower_half->update_buffer.levels[level_i];
      Level& upper_half_level = node_upper_half->update_buffer.levels[level_i];

      batt::case_of(orig_levels[level_i], [&](const auto& level_case) {
        in_node(*this).split_level(level_case, split_pivot_i, lower_half_level, upper_half_level);
      });
    }

    // Now evaluate whether the split works.
    //
    SubtreeViability lower_viability = node_lower_half->get_viability();
    SubtreeViability upper_viability = node_upper_half->get_viability();

    // If both halves are ok, then break out of the loop and finish building each half-node.
    //
    if (batt::is_case<Viable>(lower_viability) && batt::is_case<Viable>(upper_viability)) {
      reset_this_on_failure.cancel();
      break;
    }

    // If the lower half is too large, then move the split point down and retry if possible.
    //
    if (split_pivot_i > 4 && batt::is_case<NeedsSplit>(lower_viability) &&
        !batt::is_case<NeedsSplit>(upper_viability)) {
      --split_pivot_i;
      continue;
    }

    // If the upper half is too large, then move the split point up and retry if possible.
    //
    if (split_pivot_i + 4 < 64 && batt::is_case<NeedsSplit>(upper_viability) &&
        !batt::is_case<NeedsSplit>(lower_viability)) {
      ++split_pivot_i;
      continue;
    }
  }

  node_upper_half->height = this->height;

  //----- --- -- -  -  -   -
  // Initialize remaining fields of node_lower_half.
  //
  const usize lower_half_pivot_count = node_lower_half->children.size();

  BATT_CHECK_EQ(split_pivot_i, lower_half_pivot_count);

  node_lower_half->pending_bytes.resize(lower_half_pivot_count);
  node_lower_half->child_pages.resize(lower_half_pivot_count);

  for (usize i = 0; i < lower_half_pivot_count; ++i) {
    node_lower_half->children[i] = std::move(orig_children[i]);
    node_lower_half->child_pages[i] = std::move(orig_child_pages[i]);
    node_lower_half->pending_bytes[i] = orig_pending_bytes[i];
  }

  BATT_ASSIGN_OK_RESULT(
      node_lower_half->max_key_,
      node_lower_half->children.back().get_max_key(page_loader,
                                                   node_lower_half->child_pages.back()));

  node_lower_half->common_key_prefix = "";  // TODO [tastolfi 2025-03-17]

  //----- --- -- -  -  -   -
  // Initialize remaining fields of node_upper_half.
  //
  const usize upper_half_pivot_count = node_upper_half->children.size();

  BATT_CHECK_EQ(split_pivot_i + upper_half_pivot_count, orig_children.size());

  node_upper_half->pending_bytes.resize(upper_half_pivot_count);
  node_upper_half->child_pages.resize(upper_half_pivot_count);

  for (usize i = 0; i < upper_half_pivot_count; ++i) {
    const usize orig_i = i + split_pivot_i;

    node_upper_half->children[i] = std::move(orig_children[orig_i]);
    node_upper_half->child_pages[i] = std::move(orig_child_pages[orig_i]);
    node_upper_half->pending_bytes[i] = orig_pending_bytes[orig_i];
  }

  node_upper_half->max_key_ = orig_max_key;
  node_upper_half->common_key_prefix = "";  // TODO [tastolfi 2025-03-17]

  // Final sanity checks.
  //
  BATT_CHECK_EQ(node_lower_half->pivot_count() + node_upper_half->pivot_count(), orig_pivot_count);

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
        return in_segmented_level(*this, segmented_level, *query.page_loader)
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
  BATT_CHECK(!batt::is_case<NeedsSplit>(this->get_viability()));

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

  BATT_CHECK_LE(total_segments, Self::kMaxSegmentCount);

  for (Subtree& child : this->children) {
    BATT_REQUIRE_OK(child.start_serialize(context));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PageId> InMemoryNode::finish_serialize(TreeSerializeContext& context)
{
  Self::metrics().level_depth_stats.update(this->update_buffer.levels.size());

  for (Level& level : this->update_buffer.levels) {
    Optional<SegmentedLevel> new_segmented_level;

    BATT_REQUIRE_OK(    //
        batt::case_of(  //
            level,      //
            [](const EmptyLevel&) -> Status {
              return OkStatus();
            },
            [this, &context, &new_segmented_level](MergedLevel& merged_level) -> Status {
              StatusOr<SegmentedLevel> result = merged_level.finish_serialize(*this, context);
              if (result.ok()) {
                new_segmented_level.emplace(std::move(*result));
              }
              return result.status();
            },
            [](const SegmentedLevel& segmented_level) -> Status {
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
                                  NodePageView::page_layout_id(),
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
                                                   llfs::PinPageToJob pin_page_to_job) const
{
  return this->page_id_slot.load_through(page_loader,
                                         llfs::PageLoadOptions{
                                             LeafPageView::page_layout_id(),
                                             pin_page_to_job,
                                             llfs::OkIfNotFound{false},
                                             llfs::LruPriority{kLeafLruPriority},
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

  for (const Interval<usize>& part_extents : page_parts) {
    BATT_ASSIGN_OK_RESULT(
        TreeSerializeContext::BuildPageJobId id,
        context.async_build_page(
            context.tree_options().leaf_size(),
            packed_leaf_page_layout_id(),
            /*task_count=*/2,
            [this, &node, part_extents, filter_bits_per_key](
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
                                                  filter_bits_per_key,
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
    segment.active_pivots = 0;
    segment.flushed_pivots = 0;

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
void InMemoryNode::UpdateBuffer::SegmentedLevel::check_items_sorted(
    const InMemoryNode& node,
    llfs::PageLoader& page_loader) const
{
  SegmentedLevelScanner<const InMemoryNode, const SegmentedLevel, llfs::PageLoader> scanner{
      node,
      *this,
      page_loader,
      llfs::PinPageToJob::kDefault};

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

}  // namespace turtle_kv
