//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_READER_HPP

#include <turtle_kv/change_log/api_types.hpp>
#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/change_log_file.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/util/stack_merger.hpp>

#include <functional>

namespace turtle_kv {

class ChangeLogReader
{
 public:
  // Function responsible for parsing one slot at a time.
  //
  using SlotVisitorFn = std::function<Status(FirstVisitToBlock first_visit,
                                             ChangeLogBlock* block,
                                             EditOffset edit_offset,
                                             ConstBuffer payload)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogReader(const ChangeLogReader&) = delete;
  ChangeLogReader& operator=(const ChangeLogReader&) = delete;

  virtual ~ChangeLogReader() = default;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  static StatusOr<std::unique_ptr<ChangeLogReader>> open(const std::filesystem::path& path) noexcept
  {
    BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

    return {std::make_unique<ChangeLogReader>(std::move(log_file))};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  explicit ChangeLogReader(std::unique_ptr<ChangeLogFile>&& change_log) noexcept
      : change_log_{std::move(change_log)}
  {
  }

  // +++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Status visit_slots(const SlotVisitorFn& visitor)
  {
    batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> blocks =
        this->change_log_->read_blocks_into_vector();

    if (!blocks.ok()) {
      return blocks.status();
    }

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
        // Both must have slots remaining for valid comparison.
        //
        // TODO: [Gabe Bornstein 3/27/26] Is it too extreme to do a BATT_CHECK here?
        //
        BATT_CHECK(this->has_more());
        BATT_CHECK(other.has_more());
        return this->current_edit_offset() < other.current_edit_offset();
      }
    };

    struct BlockIteratorCompare {
      bool operator()(BlockIterator* left, BlockIterator* right) const
      {
        return *left < *right;
      }
    };

    // Create block iterators, filtering out empty blocks.
    //
    std::vector<BlockIterator> block_iterators;
    block_iterators.reserve(blocks->size());

    for (auto& block : *blocks) {
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
      LOG(INFO) << "No slots to be processed in change log.";
      return batt::OkStatus();
    }

    StackMerger<BlockIterator, BlockIteratorCompare> heap{
        Slice<BlockIterator>{as_slice(block_iterators)}};

    // Process slots in EditOffset order.
    //
    while (!heap.empty()) {
      BlockIterator* current = heap.first();

      ConstBuffer slot_buffer = current->block->get_slot(current->next_slot_i);
      EditOffset edit_offset = current->current_edit_offset();

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

    return batt::OkStatus();
  }

 private:
  /** \brief The state of the log file.
   */
  std::unique_ptr<ChangeLogFile> change_log_;
};

}  // namespace turtle_kv
