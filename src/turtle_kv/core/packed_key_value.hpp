#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_value_offset.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/static_assert.hpp>

#include <string_view>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedKeyValue {
  u32 key_offset;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PackedKeyValue(const PackedKeyValue&) = delete;
  PackedKeyValue& operator=(const PackedKeyValue&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedKeyValue& next() const noexcept
  {
    return *(this + 1);
  }

  PackedKeyValue& mut_next() noexcept
  {
    return *(this + 1);
  }

  const char* key_data() const noexcept
  {
    return ((const char*)this) + this->key_offset;
  }

  void set_key_data(const void* ptr) noexcept
  {
    this->key_offset = byte_distance(this, ptr);
  }

  usize key_size() const noexcept
  {
    return (this->next().key_data() - this->key_data()) - sizeof(PackedValueOffset);
  }

  KeyView key_view() const noexcept
  {
    return KeyView{this->key_data(), this->key_size()};
  }

  const PackedValueOffset& value_offset() const noexcept
  {
    return *(((const PackedValueOffset*)this->next().key_data()) - 1);
  }

  PackedValueOffset& mut_value_offset() noexcept
  {
    return *(((PackedValueOffset*)this->next().key_data()) - 1);
  }

  const char* value_data() const noexcept
  {
    return this->value_offset().get_pointer();
  }

  void set_value_data(const void* ptr) noexcept
  {
    this->mut_value_offset().set_pointer(ptr);
  }

  usize value_size() const noexcept
  {
    return this->next().value_data() - this->value_data();
  }

  ValueView value_view() const noexcept
  {
    const char* p_value_data = this->value_data();
    return ValueView::from_packed(static_cast<ValueView::OpCode>(*p_value_data),
                                  std::string_view{p_value_data + 1, this->value_size() - 1});
  }

}
//
__attribute__((aligned(4)))  //
__attribute__((packed));

BATT_STATIC_ASSERT_EQ(sizeof(PackedKeyValue), 4);
BATT_STATIC_ASSERT_EQ(alignof(PackedKeyValue), 4);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline KeyView get_key(const PackedKeyValue& kv) noexcept
{
  return kv.key_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline ValueView get_value(const PackedKeyValue& kv) noexcept
{
  return kv.value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize packed_sizeof(const PackedKeyValue& kv) noexcept
{
  return (4 + 4 + 1) + kv.key_size() + kv.value_size();
}

}  // namespace turtle_kv
