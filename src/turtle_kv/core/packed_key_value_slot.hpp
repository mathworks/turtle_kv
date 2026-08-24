//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CORE_PACKED_KEY_VALUE_SLOT_HPP

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_pointer.hpp>

namespace turtle_kv {

struct PackedKeyValueSlot;

using PackedKeyValueSlotPtr = llfs::PackedPointer<PackedKeyValueSlot, little_u16>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedKeyValueSlot {
  little_u16 key_size;
  char key_data_[0];

  //----- --- -- -  -  -   -
  // u8 key_bytes[this->key_size]
  //----- --- -- -  -  -   -
  // u8 op_code
  // u8 value_bytes[this->item_size - offsetof(this->value_bytes)]
  //----- --- -- -  -  -   -

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedKeyValueSlot(const PackedKeyValueSlot&) = delete;
  PackedKeyValueSlot& operator=(const PackedKeyValueSlot&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize slot_size(const PackedKeyValueSlotPtr* p_this) const noexcept
  {
    const PackedKeyValueSlotPtr* const p_next = p_this + 1;
    return usize{p_next->offset.value()} - usize{p_this->offset.value()} +
           sizeof(PackedKeyValueSlotPtr);
  }

  const char* key_data() const noexcept
  {
    return this->key_data_;
  }

  KeyView key_view() const noexcept
  {
    return KeyView{this->key_data_, this->key_size};
  }

  const char* value_data() const noexcept
  {
    return this->key_data() + (this->key_size + 1);
  }

  const char* value_data_end(const PackedKeyValueSlotPtr* p_this) const noexcept
  {
    return this->value_data_end(/*size_of_slot=*/this->slot_size(p_this));
  }

  const char* value_data_end(usize size_of_slot) const noexcept
  {
    return reinterpret_cast<const char*>(this) + size_of_slot;
  }

  usize value_size(const PackedKeyValueSlotPtr* p_this) const noexcept
  {
    return this->value_size(/*size_of_slot=*/this->slot_size(p_this));
  }

  usize value_size(usize size_of_slot) const noexcept
  {
    return this->value_data_end(size_of_slot) - this->value_data();
  }

  ValueView::OpCode value_op_code() const noexcept
  {
    return static_cast<ValueView::OpCode>(this->key_data_[this->key_size]);
  }

  ValueView value_view(const PackedKeyValueSlotPtr* p_this) const noexcept
  {
    return this->value_view(/*size_of_slot=*/this->slot_size(p_this));
  }

  ValueView value_view(usize size_of_slot) const noexcept
  {
    return ValueView::from_packed(
        this->value_op_code(),
        std::string_view{this->value_data(), this->value_size(size_of_slot)});
  }
};

inline KeyView get_key(const PackedKeyValueSlot& packed_slot) noexcept
{
  return packed_slot.key_view();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// TODO [tastolfi 2026-05-31] use 16-bit pointer tagging to store slot_size with the pointer in one
// 64-bit word.
//
struct PackedKeyValueSlotRef {
  const PackedKeyValueSlot* p_slot;
  usize slot_size;
};

inline KeyView get_key(const PackedKeyValueSlotRef& slot_ref) noexcept
{
  return slot_ref.p_slot->key_view();
}

inline ValueView get_value(const PackedKeyValueSlotRef& slot_ref) noexcept
{
  return slot_ref.p_slot->value_view(slot_ref.slot_size);
}

inline const PackedKeyValueSlotRef& to_key_value_slot_ref(const PackedKeyValueSlotRef& ref) noexcept
{
  return ref;
}

inline PackedKeyValueSlotRef to_key_value_slot_ref(const PackedKeyValueSlotPtr* pp_slot) noexcept
{
  return PackedKeyValueSlotRef{
      .p_slot = pp_slot->get(),
      .slot_size = pp_slot->get()->slot_size(pp_slot),
  };
}

inline PackedKeyValueSlotRef to_key_value_slot_ref(const PackedKeyValueSlotPtr& p_slot_ref) noexcept
{
  return to_key_value_slot_ref(std::addressof(p_slot_ref));
}

inline PackedKeyValueSlotRef to_key_value_slot_ref(const ConstBuffer& slot_buffer) noexcept
{
  return PackedKeyValueSlotRef{
      .p_slot = static_cast<const PackedKeyValueSlot*>(slot_buffer.data()),
      .slot_size = slot_buffer.size(),
  };
}

inline KeyView get_key(const PackedKeyValueSlotPtr& p_kv) noexcept
{
  return get_key(*p_kv);
}

inline ValueView get_value(const PackedKeyValueSlotPtr& p_kv) noexcept
{
  return p_kv->value_view(std::addressof(p_kv));
}

template <typename T>
concept ConvertibleToKeyValueSlotRef = requires(const T& obj) {
  { to_key_value_slot_ref(obj) } -> std::convertible_to<const PackedKeyValueSlotRef&>;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the size required (in bytes) to pack a slot with the passed key and
 * value.
 */
inline usize packed_key_value_slot_size(const KeyView& key, const ValueView& value)
{
  return sizeof(little_u16)  // key size
         + key.size()        //
         + sizeof(u8)        // op code
         + value.size();
}

template <ConvertibleToKeyValueSlotRef T>
inline usize packed_key_value_slot_size(const T& obj) noexcept
{
  return to_key_value_slot_ref(obj).slot_size;
}

template <typename T>
  requires HasKeyView<T> && HasValueView<T>
inline usize packed_key_value_slot_size(const T& obj) noexcept
{
  return packed_key_value_slot_size(get_key(obj), get_value(obj));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Serializes the passed key and value into the destination buffer.
 */
inline std::pair<KeyView, ValueView> pack_key_value_slot(const KeyView& key,
                                                         const ValueView& value,
                                                         MutableBuffer dst)
{
  BATT_CHECK_GE(dst.size(), packed_key_value_slot_size(key, value));

  // Serialize key size.
  //
  *static_cast<little_u16*>(dst.data()) = BATT_CHECKED_CAST(u16, key.size());
  dst += sizeof(little_u16);

  // Serialize key data.
  //
  auto packed_key_data = static_cast<char*>(dst.data());
  std::memcpy(packed_key_data, key.data(), key.size());
  dst += key.size();

  // Serialize value op code.
  //
  *static_cast<u8*>(dst.data()) = static_cast<u8>(value.op());
  dst += sizeof(u8);

  // Serialize value data.
  //
  BATT_CHECK_EQ(dst.size(), value.size());
  auto packed_value_data = static_cast<char*>(dst.data());
  std::memcpy(packed_value_data, value.data(), value.size());

  return std::make_pair(
      KeyView{
          packed_key_data,
          key.size(),
      },
      ValueView::from_packed(value.op(),
                             std::string_view{
                                 packed_value_data,
                                 value.size(),
                             }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief
 */
template <typename T>
  requires HasKeyView<T> && HasValueView<T>
inline usize pack_key_value_slot(const T& src, void* dst) noexcept
{
  const KeyView& key = get_key(src);
  const ValueView& value = get_value(src);
  const usize slot_size = packed_key_value_slot_size(key, value);

  pack_key_value_slot(key, value, MutableBuffer{dst, slot_size});

  return slot_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief
 */
template <ConvertibleToKeyValueSlotRef T>
inline usize pack_key_value_slot(const T& src, void* dst) noexcept
{
  const PackedKeyValueSlotRef& slot_ref = to_key_value_slot_ref(src);
  std::memcpy(dst, slot_ref.p_slot, slot_ref.slot_size);
  return slot_ref.slot_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Unpacks a key/value pair from the passed packed slot buffer.
 */
inline StatusOr<std::pair<KeyView, ValueView>> unpack_key_value_slot(ConstBuffer payload)
{
  BATT_ASSIGN_OK_RESULT(const little_u16* p_key_len, consume_first<little_u16>(payload));
  BATT_ASSIGN_OK_RESULT(const char* key_data, consume_first<char>(payload, *p_key_len));
  BATT_ASSIGN_OK_RESULT(const u8* p_packed_op, consume_first<u8>(payload));

  const auto op = static_cast<ValueView::OpCode>(*p_packed_op);

  const char* value_data = static_cast<const char*>(payload.data());
  const usize value_len = payload.size();

  const std::string_view value_sv{value_data, value_len};
  ValueView value = ValueView::from_packed(op, value_sv);

  return std::make_pair(KeyView{key_data, *p_key_len}, value);
}

}  // namespace turtle_kv
