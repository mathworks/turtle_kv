//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_PACKED_BLOCK_LEAF_PAGE_ITEM_ITERATOR_HPP

#include "packed_blocked_leaf_page.hpp"

#include <boost/iterator/iterator_facade.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Iterator over the items in a blocked leaf page.
 */
class PackedBlockedLeafPage::ItemIterator
    : public boost::iterator_facade<            //
          PackedBlockedLeafPage::ItemIterator,  // <- Derived
          const PackedKeyValueSlotPtr,          // <- Value
          std::random_access_iterator_tag,      // <- CategoryOrTraversal
          const PackedKeyValueSlotPtr&,         // <- Reference
          isize                                 // <- Difference
          >
{
 public:
  using Self = ItemIterator;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = const PackedKeyValueSlotPtr;
  using reference = value_type&;

  ItemIterator() = default;

  explicit ItemIterator(BlockIterator block_iter, const PackedKeyValueSlotPtr* slot) noexcept
      : block_iter_{block_iter}
      , slot_{slot}
  {
  }

  reference dereference() const
  {
    return *this->slot_;
  }

  bool equal(const Self& other) const
  {
    return this->block_iter_ == other.block_iter_ && this->slot_ == other.slot_;
  }

  void increment()
  {
    ++this->slot_;
    if (this->slot_ == this->block_iter_->items_end()) {
      ++this->block_iter_;
      this->slot_ = this->block_iter_->items_begin();
    }
  }

  void decrement()
  {
    if (this->slot_ == this->block_iter_->items_begin()) {
      --this->block_iter_;
      this->slot_ = std::prev(this->block_iter_->items_end());
    } else {
      --this->slot_;
    }
  }

  void advance(isize delta)
  {
    if (delta == 0) {
      return;
    }

    isize pos_in_block = this->get_item_pos_in_block();

    if (delta > 0) {
      // Keep stepping through the page one block at a time until we reduce delta to zero.
      //
      while (delta != 0) {
        // Figure out where the current slot is in the current block.
        //
        const isize remaining_in_block = this->get_remaining_in_block(pos_in_block);
        BATT_CHECK_GT(remaining_in_block, 0);

        // If the remaining delta is inside the block, advance the slot pointer and we are done!
        //
        if (delta < remaining_in_block) {
          this->slot_ += delta;
          break;
        }
        // Else reduce delta by the number of slots after this one in the current block.
        //
        delta -= remaining_in_block;

        // Advance to the next block, resetting the slot pointer.
        //
        ++this->block_iter_;
        this->slot_ = this->block_iter_->items_begin();
        pos_in_block = 0;
      }

    } else {  // delta < 0

      delta = -delta;
      while (delta != 0) {
        BATT_CHECK_GE(pos_in_block, 0);

        // If the remaining delta is inside the block, update the slot pointer and we are done!
        //
        if (delta <= pos_in_block) {
          this->slot_ -= delta;
          break;
        }
        // Else reduce delta by the number of slots before this one in the current block, plus one
        // for the current slot.
        //
        delta -= (pos_in_block + 1);

        // Move to the last item of the previous block.
        //
        --this->block_iter_;
        this->slot_ = std::prev(this->block_iter_->items_end());
        pos_in_block = this->block_iter_->item_count() - 1;
      }
    }
  }

  isize distance_to(const Self& other) const
  {
    if (this->block_iter_ == other.block_iter_) {
      return std::distance(this->slot_, other.slot_);
    }

    // Step forward counting items in each block until we reach the same block.
    //
    if (this->block_iter_ < other.block_iter_) {
      isize delta = this->get_remaining_in_block();
      for (auto iter = std::next(this->block_iter_); iter != other.block_iter_; ++iter) {
        delta += iter->item_count();
      }
      delta += other.get_item_pos_in_block();
      return delta;
    }
    // Else step backward.
    //
    isize delta = this->get_item_pos_in_block();
    for (auto iter = std::prev(this->block_iter_); iter != other.block_iter_; --iter) {
      delta += iter->item_count();
    }
    delta += other.get_remaining_in_block();
    return -delta;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  isize get_item_pos_in_block() const noexcept
  {
    return std::distance(this->block_iter_->items_begin(), this->slot_);
  }

  isize get_remaining_in_block(isize pos_in_block) const noexcept
  {
    return this->block_iter_->item_count() - pos_in_block;
  }

  isize get_remaining_in_block() const noexcept
  {
    return this->get_remaining_in_block(this->get_item_pos_in_block());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  BlockIterator block_iter_;
  const PackedKeyValueSlotPtr* slot_ = nullptr;
};

}  // namespace turtle_kv
