//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_BLOCKS_VISITOR_HPP

#include <turtle_kv/change_log/api_types.hpp>
#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/seq/loop_control.hpp>

#include <absl/container/flat_hash_map.h>

#include <boost/intrusive_ptr.hpp>

#include <concepts>
#include <utility>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Used to read slots from a block. Tracks which slot to read next with `next_slot_i`.
 */
struct BlockIterator {
  boost::intrusive_ptr<ChangeLogBlock> block;
  usize next_slot_i = 0;
  bool visited = false;

  explicit BlockIterator(boost::intrusive_ptr<ChangeLogBlock>&& block_arg, usize slot_i) noexcept
      : block{std::move(block_arg)}
      , next_slot_i{slot_i}
      , visited{false}
  {
  }

  BlockIterator() = default;

  bool has_more() const noexcept
  {
    return this->next_slot_i < this->block->slot_count();
  }

  EditOffset current_edit_offset() const noexcept
  {
    BATT_CHECK(this->has_more());
    return this->block->slot_edit_offset(this->next_slot_i);
  }
};

using BlockIteratorMap = absl::flat_hash_map<EditOffset, BlockIterator, EditOffset::Hash>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename Fn>
concept BlockSlotVisitorFn =
    std::invocable<Fn, FirstVisitToBlock, ChangeLogBlock*, usize, EditOffset>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class ChangeLogBlocksVisitor
{
 public:
  explicit ChangeLogBlocksVisitor(EditOffset upper_bound) noexcept
      : visited_upper_bound_{upper_bound}
  {
  }

  ChangeLogBlocksVisitor() noexcept : visited_upper_bound_{0}
  {
  }

  EditOffset visited_upper_bound() const noexcept
  {
    return this->visited_upper_bound_;
  }

  void set_visited_upper_bound(EditOffset value) noexcept
  {
    this->visited_upper_bound_ = value;
  }

  const BlockIteratorMap& pending_blocks() const noexcept
  {
    return this->pending_blocks_;
  }

  void add_block(boost::intrusive_ptr<ChangeLogBlock>&& block)
  {
    // Find the edit offset lower bound in the block w.r.t. the visited upper bound, to account
    // for cases where a trim has happened mid-block.
    //
    Optional<usize> slot_i = block->lower_bound_slot(this->visited_upper_bound_);
    if (!slot_i) {
      return;
    }
    const EditOffset slot_offset = block->slot_edit_offset(*slot_i);
    this->pending_blocks_[slot_offset] = BlockIterator{std::move(block), *slot_i};
  }

  /** \brief Walks from current_offset_start_ forward through a set of pending blocks,
   * consuming contiguous slots in order. Calls `slot_fn` for each slot consumed. Stops at the first
   * gap.
   *
   * \param slot_fn Called for each consumed slot with
   * (FirstVisitToBlock, ChangeLogBlock*, slot_index, EditOffset).
   *
   * \return The new visited_upper_bound_ value after walking.
   */
  template <BlockSlotVisitorFn SlotFn>
  EditOffset visit_change_log_blocks(SlotFn&& slot_fn)
  {
    for (;;) {
      auto it = this->pending_blocks_.find(this->visited_upper_bound_);
      if (it == this->pending_blocks_.end()) {
        break;
      }

      BlockIterator entry = std::move(it->second);
      this->pending_blocks_.erase(it);

      do {
        auto first_visit = FirstVisitToBlock{!entry.visited};
        entry.visited = true;

        BATT_INVOKE_LOOP_FN((slot_fn,
                             first_visit,
                             entry.block.get(),
                             entry.next_slot_i,
                             this->visited_upper_bound_));

        this->visited_upper_bound_ += entry.block->next_edit_offset_of_slot(entry.next_slot_i);

        ++entry.next_slot_i;
      } while (entry.has_more() && entry.current_edit_offset() == this->visited_upper_bound_);

      if (entry.has_more()) {
        const EditOffset block_next_edit_offset = entry.current_edit_offset();
        this->pending_blocks_[block_next_edit_offset] = std::move(entry);
      }
    }
    return this->visited_upper_bound_;
  }

 private:
  /** \brief The upper bound of the contiguous range of slots that have been visited.
   */
  EditOffset visited_upper_bound_;

  /** \brief Map from slot EditOffset to block entry; entries are consumed as the
   *   visited_upper_bound_ advances. Blocks with remaining non-contiguous slots are re-inserted.
   */
  BlockIteratorMap pending_blocks_;
};

}  // namespace turtle_kv
