//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_reader.hpp>
//

#include <functional>
#include <unordered_map>

namespace turtle_kv {

namespace {

// Used to read slots from a block. Tracks which slot to read next with `next_slot_i`.
//
struct BlockIterator {
  boost::intrusive_ptr<ChangeLogBlock> block;
  usize next_slot_i = 0;

  // Get the EditOffset of the current slot.
  //
  EditOffset current_edit_offset() const
  {
    BATT_CHECK(this->has_more());
    return this->block->slot_edit_offset(this->next_slot_i);
  }

  // Check if there are more slots to process.
  //
  bool has_more() const
  {
    return next_slot_i < block->slot_count();
  }

  bool operator<(const BlockIterator& other) const
  {
    // If this has no more, it cannot be before anything else.
    //
    if (!this->has_more()) {
      return false;
    }

    // If the other has no more, this comes before; otherwise we know both have more so compare the
    // current edit offsets.
    //
    return !other.has_more() || this->current_edit_offset() < other.current_edit_offset();
  }
};

struct BlockIteratorCompare {
  bool operator()(BlockIterator* left, BlockIterator* right) const
  {
    return *left < *right;
  }
};

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<RecoveredChangeLogState> ChangeLogReader::visit_slots(
    const SlotVisitorFn& visitor) noexcept
{
  ChangeLogFile& change_log = this->change_log_file();

  BATT_ASSIGN_OK_RESULT(std::vector<boost::intrusive_ptr<ChangeLogBlock>> blocks_vec,
                        change_log.read_blocks_into_vector());

  VLOG(1) << BATT_INSPECT(blocks_vec.size());

  ChangeLogFile::PackedMetaBlock meta_block;
  BATT_REQUIRE_OK(change_log.read_meta_block(meta_block));

  const ChangeLogMetaState read_meta_state = meta_block.meta_state.unpack();

  VLOG(1) << BATT_INSPECT(read_meta_state);

  // An index from BlockIndex to EditOffset
  //
  std::unordered_set<BlockIndex, BlockIndex::Hash> visited_block_set;

  // Create block iterators, filtering out empty blocks.
  //
  std::vector<BlockIterator> block_iterators;

  for (auto& block : blocks_vec) {
    if (block->edit_offset_upper_bound() <= read_meta_state.trim_edit_offset) {
      VLOG(1) << "Skipping trimmed block;" << BATT_INSPECT(read_meta_state.trim_edit_offset)
              << BATT_INSPECT(block->edit_offset_upper_bound())
              << BATT_INSPECT(block->get_block_index());
      continue;
    }
    if (block->slot_count() > 0) {
      block_iterators.emplace_back(BlockIterator{
          .block = batt::make_copy(block),
          .next_slot_i = 0,
      });
    }
  }

  // If there's no slots to process, return early.
  //
  if (block_iterators.empty()) {
    RecoveredChangeLogState recovered_state;
    recovered_state.block_range = Interval<BlockIndex>{BlockIndex{0}, BlockIndex{0}};
    recovered_state.trim_edit_offset = read_meta_state.trim_edit_offset;
    recovered_state.next_edit_offset = read_meta_state.trim_edit_offset;
    recovered_state.active_blocks_upper_bounds.clear();

    VLOG(1) << "No slots to be processed in change log.";
    return {recovered_state};
  }

  StackMerger<BlockIterator, BlockIteratorCompare> heap{
      Slice<BlockIterator>{as_slice(block_iterators)}};

  // The first expected slot edit offset is the trim edit offset.
  //
  EditOffset expected_next_edit_offset = read_meta_state.trim_edit_offset;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Process slots in EditOffset order.
  //
  while (!heap.empty()) {
    BlockIterator* current = heap.first();
    ConstBuffer slot_buffer = current->block->get_slot(current->next_slot_i);
    EditOffset edit_offset = current->current_edit_offset();

    // Advance to the next slot when we finish each iteration of the loop.
    //
    auto on_loop_iter_exit = batt::finally([&] {
      ++current->next_slot_i;
      if (current->has_more()) {
        heap.update_first();
      } else {
        heap.remove_first();
      }
    });

    // By skipping all *blocks* below the trim bound (above), we should avoid the situation where we
    // need to filter out *slots* below the trim, since we should never be setting the trim such
    // that it bisects a block (i.e., every block is owned uniquely by a MemTable).
    //
    BATT_CHECK_GE(edit_offset, read_meta_state.trim_edit_offset);

    // If there's a gap in our slots, we're missing data and can't continue.
    //
    if (expected_next_edit_offset != edit_offset) {
      VLOG(1) << "Gap found;" << BATT_INSPECT(expected_next_edit_offset)
              << BATT_INSPECT(edit_offset) << BATT_INSPECT(current->block->get_block_index());
      break;
    }

    expected_next_edit_offset =
        edit_offset +
        EditOffsetDelta{static_cast<i64>(slot_buffer.size() - sizeof(PackedEditOffsetDelta))};

    auto first_visit_to_block = FirstVisitToBlock{current->next_slot_i == 0};
    ChangeLogBlock* block = current->block.get();

    // If recovering state, add each block which contains recovered slots to the
    // `block_edit_ranges` hash table.
    //
    if (first_visit_to_block) {
      visited_block_set.insert(block->get_block_index().value_or_panic());
    }

    // Move the payload past the EditOffset.
    //
    ConstBuffer payload = slot_buffer + sizeof(PackedEditOffsetDelta);

    Status visit_status = visitor(first_visit_to_block, block, edit_offset, payload);
    BATT_REQUIRE_OK(visit_status);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the `recovered_state` object.
  //
  RecoveredChangeLogState recovered_state;

  recovered_state.block_range = read_meta_state.block_range;
  recovered_state.trim_edit_offset = read_meta_state.trim_edit_offset;
  recovered_state.next_edit_offset = expected_next_edit_offset;
  recovered_state.active_blocks_upper_bounds.clear();

  const ChangeLogFile::Config& cfg = change_log.config();

  std::unordered_map<BlockIndex, EditOffset, BlockIndex::Hash> block_upper_bounds;

  // Cache the edit offset upper bounds for each block.
  //
  for (const boost::intrusive_ptr<ChangeLogBlock>& block : blocks_vec) {
    // First we truncate any part of each block that extends beyond the recoverable bound.
    //
    BATT_REQUIRE_OK(block->truncate_edit_offset_upper_bound(recovered_state.next_edit_offset,
                                                            cfg,
                                                            change_log.file()));

    BATT_CHECK_LE(block->edit_offset_upper_bound(), recovered_state.next_edit_offset);

    block_upper_bounds.emplace(/*key=*/block->get_block_index().value_or_panic(),
                               /*value=*/block->edit_offset_upper_bound());
  }

  // Initialize the recovered block range to all blocks, shifted by the recovered lower bound.
  //
  auto& active_range = recovered_state.block_range;
  active_range.upper_bound = BlockIndex{active_range.lower_bound + cfg.block_count};

  // Run sanity checks.
  //
  cfg.check_invariants(active_range);

  // First remove any unvisited blocks from the start of the active range.
  //
  while (!active_range.empty() && visited_block_set.count(active_range.lower_bound) == 0) {
    cfg.increment_lower_bound(active_range);
  }

  // Reset the active range and then set the upper to the maximum logical index of all visited
  // blocks.
  //
  active_range.upper_bound = active_range.lower_bound;
  for (BlockIndex visited_index : visited_block_set) {
    cfg.extend_block_range_to_include(active_range, visited_index);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // At this point the recovered active range is correct.  If it disagrees with the meta state we
  // read earlier, then refresh the meta-block.
  //
  const auto& recovered_meta_state = static_cast<const ChangeLogMetaState&>(recovered_state);
  if (read_meta_state != recovered_meta_state) {
    recovered_meta_state.pack_to(&meta_block.meta_state);
    BATT_REQUIRE_OK(change_log.write_meta_block(meta_block));
  }

  // Now we just need to populate recovered_state.active_blocks_upper_bounds.
  //
  Optional<EditOffset> most_recent_block_upper_bound;

  // Gather the edit offset upper bounds for all blocks in the recovered active range.
  //  Fill in any gaps with the previous block's upper bound.
  //
  for (Interval<BlockIndex> blocks = active_range; !blocks.empty();
       cfg.increment_lower_bound(blocks)) {
    const BlockIndex index = blocks.lower_bound;
    const auto iter = block_upper_bounds.find(index);
    if (iter != block_upper_bounds.end()) {
      most_recent_block_upper_bound = iter->second;
    }
    recovered_state.active_blocks_upper_bounds.push_back(
        most_recent_block_upper_bound.value_or_panic());
  }

  // Sanity check: there must be exactly one element in `active_blocks_upper_bounds` for each block
  // in the active range.
  //
  BATT_CHECK_EQ(BATT_CHECKED_CAST(usize, active_range.size()),
                recovered_state.active_blocks_upper_bounds.size());

  return {recovered_state};
}

}  // namespace turtle_kv
