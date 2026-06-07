//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_LEAF_BLOCK_HPP

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/item_view.hpp>
#include <turtle_kv/core/packed_key_value_slot.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_pointer.hpp>

#include <batteries/compare.hpp>
#include <batteries/operators.hpp>
#include <batteries/seq.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <ranges>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafBlock {
  static constexpr u32 kMagic = 0x7370b49full;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class Iterator;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u32 magic;                    // +4 = 4
  little_u16 shared_prefix_size;    // +2 = 6
  PackedKeyValueSlotPtr items_[1];  // +2 = 8

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename EditT>
  static usize packed_edit_size(const EditT& edit) noexcept
  {
    const usize slot_size = packed_key_value_slot_size(edit);
    const usize edit_size = slot_size + sizeof(PackedKeyValueSlotPtr);

    return edit_size;
  }

  static constexpr usize capacity(usize block_size) noexcept
  {
    return block_size - std::min(block_size, sizeof(PackedLeafBlock));
  }

  /** \brief Returns the passed buffer's memory region, validated as a PackedLeafBlock and cast to
   * `const PackedLeafBlock &`.
   */
  static const PackedLeafBlock& view_of(const ConstBuffer& buffer) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize item_count() const noexcept
  {
    return this->items_end() - this->items_begin();
  }

  KeyView key_at(usize i) const noexcept
  {
    return this->items_[i]->key_view();
  }

  ValueView value_at(usize i) const noexcept
  {
    return this->items_[i]->value_view(&this->items_[i]);
  }

  EditView edit_at(usize i) const noexcept
  {
    auto& packed = *this->items_[i];
    return EditView{packed.key_view(), packed.value_view(&this->items_[i])};
  }

  Optional<ItemView> item_at(usize i) const noexcept
  {
    return to_item_view(this->edit_at(i));
  }

  const PackedKeyValueSlotPtr& front_item() const noexcept
  {
    return this->items_[0];
  }

  const PackedKeyValueSlotPtr& back_item() const noexcept
  {
    return this->items_[this->item_count() - 1];
  }

  const PackedKeyValueSlotPtr* items_begin() const noexcept
  {
    return this->items_;
  }

  const PackedKeyValueSlotPtr* items_end() const noexcept
  {
    return ((const PackedKeyValueSlotPtr*)this->items_[0].get()) - 1;
  }

  Slice<const PackedKeyValueSlotPtr> items_slice() const noexcept
  {
    return as_slice(this->items_begin(), this->items_end());
  }

  Slice<const PackedKeyValueSlotPtr> items_slice(Optional<KeyView> key_lower_bound,
                                                 Optional<KeyView> key_upper_bound) const noexcept;

  KeyView min_key() const noexcept
  {
    return get_key(this->front_item());
  }

  KeyView max_key() const noexcept
  {
    return get_key(this->back_item());
  }

  KeyView shared_key_prefix() const noexcept
  {
    return this->min_key().substr(0, this->shared_prefix_size);
  }

  /** \brief Returns an iterator to the given key in this block if found or nullptr if not found.
   */
  const PackedKeyValueSlotPtr* find_key(const KeyView& key) const noexcept;

  /** \brief Returns an iterator to the first item in this block whose key is not less than `key`;
   * if all keys in the block are less than `key`, returns `this->items_end()`.
   */
  const PackedKeyValueSlotPtr* lower_bound(const KeyView& key) const noexcept;
};

static_assert(sizeof(PackedLeafBlock) == 8);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackedLeafBlock::Iterator
    : public boost::iterator_facade<        //
          PackedLeafBlock::Iterator,        // <- Derived
          const PackedLeafBlock,            // <- Value
          std::random_access_iterator_tag,  // <- CategoryOrTraversal
          const PackedLeafBlock&,           // <- Reference
          isize                             // <- Difference
          >
{
 public:
  using Self = Iterator;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = const PackedLeafBlock;
  using reference = const PackedLeafBlock&;

  Iterator() = default;

  explicit Iterator(const PackedLeafBlock* block, isize block_size) noexcept
      : block_{block}
      , block_size_{block_size}
  {
  }

  reference dereference() const
  {
    return *this->block_;
  }

  bool equal(const Self& other) const
  {
    return this->block_ == other.block_ && this->block_size_ == other.block_size_;
  }

  void increment()
  {
    this->advance(1);
  }

  void decrement()
  {
    this->advance(-1);
  }

  void advance(isize delta)
  {
    this->block_ = static_cast<const PackedLeafBlock*>(
        advance_pointer(this->block_, delta * this->block_size_));
  }

  isize distance_to(const Self& other) const
  {
    return (byte_distance(this->block_, other.block_)) / this->block_size_;
  }

 private:
  const PackedLeafBlock* block_ = nullptr;
  isize block_size_ = 0;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafBlockStats {
  usize block_size;
  usize item_count;
  usize item_slot_bytes;
  usize item_ptr_bytes;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <std::ranges::range RangeT>
  static PackedLeafBlockStats from(const RangeT& src, usize block_size) noexcept;
};

BATT_OBJECT_PRINT_IMPL((inline),
                       PackedLeafBlockStats,
                       (block_size, item_count, item_slot_bytes, item_ptr_bytes))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT,
          typename IterT = std::decay_t<decltype(std::begin(std::declval<const RangeT&>()))>>
StatusOr<IterT> pack_leaf_block(const RangeT& src,
                                MutableBuffer dst,
                                const Optional<PackedLeafBlockStats>& stats = None) noexcept;

}  // namespace turtle_kv

#include "packed_leaf_block.ipp"
