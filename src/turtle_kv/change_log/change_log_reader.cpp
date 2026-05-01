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
    // TODO: [Gabe Bornstein 3/27/26] Is it too extreme to do a BATT_CHECK here?
    //
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
Status ChangeLogReader::visit_slots(const SlotVisitorFn& visitor,
                                    RecoveredChangeLogState* recovered_state
                                    [[maybe_unused]]) noexcept
{
  batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> blocks =
      this->change_log_->read_blocks_into_vector();

  if (!blocks.ok()) {
    return blocks.status();
  }

  // An index from BlockIndex to EditOffset
  //
  std::unordered_map<BlockIndex, Optional<Interval<EditOffset>>, BlockIndex::Hash>
      block_edit_ranges;

  //----- --- -- -  -  -   -
  // Helpers
  //
  const auto get_block_upper_bound =
      [&block_edit_ranges](BlockIndex index) -> Optional<EditOffset> {
    auto iter = block_edit_ranges.find(index);
    if (iter == block_edit_ranges.end() || *iter == None) {
      return None;
    }
    return iter->upper_bound;
  };
  const auto get_block_edit_range =
      [&block_edit_ranges](BlockIndex index) -> Optional<Interval<EditOffset>> {
    auto iter = block_edit_ranges.find(index);
    if (iter == block_edit_ranges.end()) {
      return None;
    }
    return *iter;
  };
  //----- --- -- -  -  -   -

  // Create block iterators, filtering out empty blocks.
  //
  std::vector<BlockIterator> block_iterators;
  block_iterators.reserve(blocks->size());

  for (auto& block : *blocks) {
    if (recovered_state) {
      block_edit_ranges[block->get_block_index().value_or_panic()] = block->edit_offset_range();
    }
    if (block->slot_count() > 0) {
      block_iterators.emplace_back(BlockIterator{
          .block = std::move(block),
          .next_slot_i = 0,
      });
    }
  }

  // If there's no slots to process, return early.
  //
  if (block_iterators.empty()) {
    VLOG(1) << "No slots to be processed in change log.";
    return batt::OkStatus();
  }

  StackMerger<BlockIterator, BlockIteratorCompare> heap{
      Slice<BlockIterator>{as_slice(block_iterators)}};

  // Process slots in EditOffset order.
  //
  Optional<EditOffset> expected_next_edit_offset = None;
  while (!heap.empty()) {
    BlockIterator* current = heap.first();

    ConstBuffer slot_buffer = current->block->get_slot(current->next_slot_i);
    EditOffset edit_offset = current->current_edit_offset();

    // If there's a gap in our slots, we're missing data and can't continue
    //
    if (expected_next_edit_offset.value_or(edit_offset) != edit_offset) {
      return batt::OkStatus();
    }

    expected_next_edit_offset = edit_offset + EditOffsetDelta{static_cast<i64>(slot_buffer.size())};

    // Move the payload past the EditOffset.
    //
    ConstBuffer payload = slot_buffer + sizeof(PackedEditOffsetDelta);

    Status visit_status = visitor(FirstVisitToBlock{current->next_slot_i == 0},
                                  current->block.get(),
                                  edit_offset,
                                  payload);
    BATT_REQUIRE_OK(visit_status);

    current->next_slot_i++;

    if (current->has_more()) {
      heap.update_first();
    } else {
      heap.remove_first();
    }
  }

  if (recovered_state) {
    const ChangeLogFile::Config& config = this->change_log_->config();
    recovered_state->active_block_range = config.active_block_range;
    auto& active_range = recovered_state->active_block_range;

    // Run sanity checks.
    //
    config.check_invariants(active_range);

    // First remove any unrecoverable blocks from the start of the active range.
    //
    while (!active_range.empty() && get_block_upper_bound(active_range.lower_bound) == None) {
      config.increment_lower_bound(active_range);
    }

    // Extend the upper end of the active range to include any extra recovered blocks at the end.
    //
    if (!active_range.empty()) {
      Interval<EditOffset> first_block_edit_range =
          get_block_edit_range(active_range.lower_bound).value_or_panic();

      for (;;) {
        // If the last block is the same as the first, then there are no more blocks we could
        // possibly add; break.
        //
        BlockIndex last_block_physical_index = config.wrapped_upper_bound(active_range);
        if (last_block_physical_index == active_range.lower_bound) {
          break;
        }

        Optional<Interval<EditOffset>> last_block_edit_range =
            get_block_edit_range(last_block_physical_index);
      }
    }

    BlockIndex index = recovered_state->active_block_range.lower_bound;

    for (i64 block_i = recovered_state->active_block_range.lower_bound;
         block_i < recovered_state->active_block_range;
         ++block_i) {
    }
  }

  return batt::OkStatus();
}

}  // namespace turtle_kv
