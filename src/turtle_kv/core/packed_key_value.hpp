#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_value_offset.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

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

  BATT_ALWAYS_INLINE const PackedKeyValue& next() const
  {
    return *(this + 1);
  }

  PackedKeyValue& mut_next()
  {
    return *(this + 1);
  }

  BATT_ALWAYS_INLINE const char* key_data() const
  {
    return ((const char*)this) + this->key_offset;
  }

  void set_key_data(const void* ptr)
  {
    this->key_offset = byte_distance(this, ptr);
  }

  BATT_ALWAYS_INLINE usize key_size() const
  {
    return (this->next().key_data() - this->key_data()) - sizeof(PackedValueOffset);
  }

  BATT_ALWAYS_INLINE KeyView key_view() const
  {
    return KeyView{this->key_data(), this->key_size()};
  }

  const PackedValueOffset& value_offset() const
  {
    return *(((const PackedValueOffset*)this->next().key_data()) - 1);
  }

  PackedValueOffset& mut_value_offset()
  {
    return *(((PackedValueOffset*)this->next().key_data()) - 1);
  }

  const char* value_data() const
  {
    return this->value_offset().get_pointer();
  }

  void set_value_data(const void* ptr)
  {
    this->mut_value_offset().set_pointer(ptr);
  }

  usize value_size() const
  {
    return this->next().value_data() - this->value_data();
  }

  ValueView value_view() const
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
BATT_ALWAYS_INLINE inline KeyView get_key(const PackedKeyValue& kv)
{
  return kv.key_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline ValueView get_value(const PackedKeyValue& kv)
{
  return kv.value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize packed_sizeof(const PackedKeyValue& kv)
{
  return (4 + 4 + 1) + kv.key_size() + kv.value_size();
}

}  // namespace turtle_kv
