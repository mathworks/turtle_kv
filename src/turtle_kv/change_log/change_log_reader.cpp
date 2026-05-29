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
#include <unordered_set>

namespace turtle_kv {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Changes the passed `recovered_state` against `read_meta_state`; if it is different,
 * updates the passed `meta_block` and writes it out to the passed `change_log` file.
 */
Status update_meta_state(ChangeLogFile* change_log,
                         ChangeLogFile::PackedMetaBlock* meta_block,
                         const ChangeLogMetaState& read_meta_state,
                         const RecoveredChangeLogState& recovered_state) noexcept
{
  const auto& recovered_meta_state = static_cast<const ChangeLogMetaState&>(recovered_state);
  if (read_meta_state != recovered_meta_state) {
    recovered_meta_state.pack_to(&meta_block->meta_state);
    BATT_REQUIRE_OK(change_log->write_meta_block(*meta_block));
  }
  return OkStatus();
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<RecoveredChangeLogState> ChangeLogReader::visit_slots(
    const SlotVisitorFn& visitor,
    Optional<EditOffset> new_trim_edit_offset) noexcept
{
  ChangeLogFile& change_log = this->change_log_file();

  BATT_ASSIGN_OK_RESULT(std::vector<boost::intrusive_ptr<ChangeLogBlock>> blocks_vec,
                        change_log.read_blocks_into_vector());

  VLOG(1) << BATT_INSPECT(blocks_vec.size());

  ChangeLogFile::PackedMetaBlock meta_block;
  BATT_REQUIRE_OK(change_log.read_meta_block(meta_block));

  const ChangeLogMetaState read_meta_state = meta_block.meta_state.unpack();

  VLOG(1) << BATT_INSPECT(read_meta_state);

  // Set the target trim offset.
  //
  const EditOffset target_trim_edit_offset = [&] {
    if (new_trim_edit_offset && *new_trim_edit_offset > read_meta_state.trim_edit_offset) {
      return *new_trim_edit_offset;
    }
    return read_meta_state.trim_edit_offset;
  }();

  // An index from BlockIndex to EditOffset
  //
  std::unordered_set<BlockIndex, BlockIndex::Hash> visited_block_set;

  // Build the pending block map, filtering out empty blocks.
  //
  BlockIteratorMap pending_blocks;

  for (auto& block : blocks_vec) {
    if (block->edit_offset_upper_bound() <= target_trim_edit_offset) {
      VLOG(1) << "Skipping trimmed block;" << BATT_INSPECT(target_trim_edit_offset)
              << BATT_INSPECT(block->edit_offset_upper_bound())
              << BATT_INSPECT(block->get_block_index());
      continue;
    }
    if (block->slot_count() > 0) {
      const i64 first_slot_offset = block->slot_edit_offset(0).value();
      pending_blocks[first_slot_offset] = BlockIterator{batt::make_copy(block), 0};
    }
  }

  // If there's no slots to process, return early.
  //
  if (pending_blocks.empty()) {
    RecoveredChangeLogState recovered_state;
    recovered_state.block_range = Interval<BlockIndex>{BlockIndex{0}, BlockIndex{0}};
    recovered_state.trim_edit_offset = target_trim_edit_offset;
    recovered_state.next_edit_offset = target_trim_edit_offset;
    recovered_state.active_blocks_upper_bounds.clear();

    BATT_REQUIRE_OK(update_meta_state(&change_log, &meta_block, read_meta_state, recovered_state));

    VLOG(1) << "No slots to be processed in change log.";
    return {recovered_state};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Process slots in EditOffset order.
  //
  Status visit_status = OkStatus();

  EditOffset expected_next_edit_offset{walk_change_log_blocks(
      target_trim_edit_offset.value(),
      pending_blocks,
      [&](ChangeLogBlock* block, usize slot_i, EditOffset edit_offset) {
        if (!visit_status.ok()) {
          return;
        }

        auto first_visit_to_block = FirstVisitToBlock{
            visited_block_set.insert(block->get_block_index().value_or_panic()).second};

        ConstBuffer slot_buffer = block->get_slot(slot_i);
        ConstBuffer payload = slot_buffer + sizeof(PackedEditOffsetDelta);

        visit_status = visitor(first_visit_to_block, block, edit_offset, payload);
      })};

  BATT_REQUIRE_OK(visit_status);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the `recovered_state` object.
  //
  RecoveredChangeLogState recovered_state;

  recovered_state.block_range = read_meta_state.block_range;
  recovered_state.trim_edit_offset = target_trim_edit_offset;
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
  BATT_REQUIRE_OK(update_meta_state(&change_log, &meta_block, read_meta_state, recovered_state));

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
