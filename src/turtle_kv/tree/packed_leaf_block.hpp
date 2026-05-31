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
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_pointer.hpp>

#include <batteries/compare.hpp>

#include <ranges>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline KeyView get_key(const PackedKeyValueSlotPtr& p_kv) noexcept
{
  return get_key(*p_kv);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafBlock {
  static constexpr u32 kMagic = 0x7370b49full;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u32 magic;                    // +4 = 4
  little_u16 shared_prefix_size;    // +2 = 6
  PackedKeyValueSlotPtr items_[1];  // +2 = 8

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static const PackedLeafBlock& view_of(const ConstBuffer& buffer) noexcept
  {
    BATT_CHECK_GE(buffer.size(), sizeof(PackedLeafBlock));

    const auto* block = static_cast<const PackedLeafBlock*>(buffer.data());

    BATT_CHECK_EQ(block->magic, PackedLeafBlock::kMagic);

    return *block;
  }

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

  KeyView min_key() const noexcept
  {
    return get_key(this->front_item());
  }

  KeyView max_key() const noexcept
  {
    return get_key(this->back_item());
  }

  const PackedKeyValueSlotPtr* find_key(const KeyView& key) const noexcept
  {
    auto [first, last] =
        std::equal_range(this->items_begin(),
                         this->items_end(),
                         key,
                         [](const auto& l, const auto& r) {
                           return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
                         });

    if (first == last) {
      return nullptr;
    }
    return std::addressof(*first);
  }

  const PackedKeyValueSlotPtr* lower_bound(const KeyView& key) const noexcept
  {
    return std::lower_bound(this->items_begin(),
                            this->items_end(),
                            key,
                            [](const auto& l, const auto& r) {
                              return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
                            });
  }
};

static_assert(sizeof(PackedLeafBlock) == 8);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafBlockStats {
  usize block_size;
  usize item_count;
  usize item_slot_bytes;
  usize item_ptr_bytes;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <std::ranges::range RangeT>
  static PackedLeafBlockStats from(const RangeT& src, usize dst_size) noexcept;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT,
          typename IterT = std::decay_t<decltype(std::begin(std::declval<const RangeT&>()))>>
StatusOr<IterT> pack_leaf_block(const RangeT& src, MutableBuffer dst) noexcept;

}  // namespace turtle_kv

#include "packed_leaf_block.ipp"
