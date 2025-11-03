#pragma once

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_value_offset.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <ostream>
#include <string_view>

namespace turtle_kv {

inline ValueView unpack_value_view(const void* data, usize size)
{
  const char* bytes = static_cast<const char*>(data);
  return ValueView::from_packed(static_cast<ValueView::OpCode>(bytes[0]),
                                std::string_view{bytes + 1, size - 1});
}

inline ValueView unpack_value_view(const ConstBuffer& buffer)
{
  return unpack_value_view(buffer.data(), buffer.size());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
// When TURTLE_KV_PACK_KEYS_TOGETHER is 1, this struct points to a data region
// with this layout:
//
// ┌────────────┬───────────────────────┬───────────────────────────────┐
// │key_size:u16│       key bytes       │          value bytes          │
// └────────────┴───────────────────────┴───────────────────────────────┘
//               ◀──────key_size───────▶
//
//
struct PackedKeyValue {
  using Self = PackedKeyValue;

#if TURTLE_KV_PACK_KEYS_TOGETHER

  using PackedKeySize = little_u16;

  static KeyView key_view_from_data_begin(const void* p_data_begin)
  {
    auto* p_key_size = static_cast<const PackedKeySize*>(p_data_begin);
    const char* key_begin = reinterpret_cast<const char*>(p_key_size + 1);

    return KeyView{key_begin, *p_key_size};
  }

  static ValueView value_view_from_data(const void* p_data_begin, usize data_size)
  {
    auto* p_key_size = static_cast<const PackedKeySize*>(p_data_begin);
    const char* key_begin = reinterpret_cast<const char*>(p_key_size + 1);
    const char* value_begin = key_begin + *p_key_size;
    const usize value_size = data_size - (sizeof(PackedKeySize) + *p_key_size);

    return unpack_value_view(value_begin, value_size);
  }

#endif  // TURTLE_KV_PACK_KEYS_TOGETHER

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  BATT_ALWAYS_INLINE const char* data_begin() const
  {
    return ((const char*)this) + this->key_offset;
  }

#if TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE void set_data_begin(const void* ptr)
  {
    this->key_offset = byte_distance(this, ptr);
  }

  BATT_ALWAYS_INLINE const char* shifted_data_begin(isize offset_delta) const
  {
    return this->data_begin() + offset_delta;
  }

  BATT_ALWAYS_INLINE usize data_size() const
  {
    return (this->next().data_begin() - this->data_begin());
  }

#else  // TURTLE_KV_PACK_KEYS_TOGETHER

  void set_key_data(const void* ptr)
  {
    this->key_offset = byte_distance(this, ptr);
  }

#endif  // TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE const char* key_data() const
  {
#if TURTLE_KV_PACK_KEYS_TOGETHER
    return this->data_begin() + sizeof(PackedKeySize);
#else
    return this->data_begin();
#endif
  }

  BATT_ALWAYS_INLINE usize key_size() const
  {
#if TURTLE_KV_PACK_KEYS_TOGETHER
    return reinterpret_cast<const PackedKeySize*>(this->data_begin())->value();
#else
    return (this->next().key_data() - this->key_data()) - sizeof(PackedValueOffset);
#endif
  }

  BATT_ALWAYS_INLINE KeyView key_view() const
  {
#if TURTLE_KV_PACK_KEYS_TOGETHER
    return Self::key_view_from_data_begin(this->data_begin());
#else
    return KeyView{this->key_data(), this->key_size()};
#endif
  }

  //----- --- -- -  -  -   -

#if TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE KeyView shifted_key_view(isize offset_delta) const
  {
    return Self::key_view_from_data_begin(this->shifted_data_begin(offset_delta));
  }

#else  // TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE const char* shifted_key_data(isize offset_delta) const
  {
    return this->key_data() + offset_delta;
  }

  BATT_ALWAYS_INLINE KeyView shifted_key_view(isize offset_delta) const
  {
    return KeyView{this->shifted_key_data(offset_delta), this->key_size()};
  }

#endif  // TURTLE_KV_PACK_KEYS_TOGETHER

  //----- --- -- -  -  -   -

#if TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE const char* value_data() const
  {
    return this->key_data() + this->key_size();
  }

  BATT_ALWAYS_INLINE usize value_size() const
  {
    return this->data_size() - (this->key_size() + sizeof(PackedKeySize));
  }

#else  // TURTLE_KV_PACK_KEYS_TOGETHER

  const PackedValueOffset& value_offset() const
  {
    return *(((const PackedValueOffset*)this->next().key_data()) - 1);
  }

  const char* value_data() const
  {
    return this->value_offset().get_pointer();
  }

  usize value_size() const
  {
    return this->next().value_data() - this->value_data();
  }

#endif  // TURTLE_KV_PACK_KEYS_TOGETHER

  //----- --- -- -  -  -   -

#if TURTLE_KV_PACK_KEYS_TOGETHER

  BATT_ALWAYS_INLINE ValueView value_view() const
  {
    return Self::value_view_from_data(this->data_begin(), this->data_size());
  }

  BATT_ALWAYS_INLINE ValueView shifted_value_view(isize offset_delta) const
  {
    return Self::value_view_from_data(this->shifted_data_begin(offset_delta), this->data_size());
  }

#else  // TURTLE_KV_PACK_KEYS_TOGETHER

  const PackedValueOffset& shifted_value_offset(isize offset_delta) const
  {
    return *(((const PackedValueOffset*)this->next().shifted_key_data(offset_delta)) - 1);
  }

  const char* shifted_value_data(isize offset_delta) const
  {
    return this->shifted_value_offset(offset_delta).get_pointer();
  }

  usize shifted_value_size(isize offset_delta) const
  {
    const char* next_ptr = this->next().shifted_value_data(offset_delta);
    const char* this_ptr = this->shifted_value_data(offset_delta);

    const u32 next_value_offset = this->next().shifted_value_offset(offset_delta).int_value;
    const u32 this_value_offset = this->shifted_value_offset(offset_delta).int_value;

    BATT_CHECK_GT((void*)next_ptr, (void*)this_ptr)
        << BATT_INSPECT(offset_delta) << BATT_INSPECT(next_value_offset)
        << BATT_INSPECT(this_value_offset);

    return next_ptr - this_ptr;
  }

  //----- --- -- -  -  -   -

  PackedValueOffset& mut_value_offset()
  {
    return *(((PackedValueOffset*)this->next().key_data()) - 1);
  }

  void set_value_data(const void* ptr)
  {
    this->mut_value_offset().set_pointer(ptr);
  }

  ValueView value_view() const
  {
    return unpack_value_view(this->value_data(), this->value_size());
  }

#endif  // TURTLE_KV_PACK_KEYS_TOGETHER

}
//
__attribute__((aligned(4)))  //
__attribute__((packed));

BATT_STATIC_ASSERT_EQ(sizeof(PackedKeyValue), 4);
BATT_STATIC_ASSERT_EQ(alignof(PackedKeyValue), 4);

inline std::ostream& operator<<(std::ostream& out, const PackedKeyValue& kv)
{
  return out << "{.key=" << batt::c_str_literal(kv.key_view()) << ", .value=" << kv.value_view()
             << ",}";
}

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

struct ShiftedPackedKeyOrder {
  isize offset_delta;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyView get_key_view(const PackedKeyValue& pkv) const
  {
    return pkv.shifted_key_view(offset_delta);
  }

  template <typename T>
  decltype(auto) get_key_view(const T& obj) const
  {
    return get_key(obj);
  }

  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const
  {
    return KeyOrder{}(this->get_key_view(l), this->get_key_view(r));
  }
};

/** \brief A comparator that computes a PackedKeyValue's absolute byte offset from the start of a
 * leaf page, and compares it to a given target absolute offset.
 */
struct PackedKeyOffsetCompare {
  /** \brief A "base" PackedKeyValue whose absolute offset is already known, which we can use to
   * compute any other PackedKeyValue's object. This comparator relies on base_pkv to be the start
   * of the range it is being used on, i.e., smaller than everything it is being compared to.
   */
  usize base_absolute_offset;
  const PackedKeyValue* base_pkv;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize get_key_data_offset(const PackedKeyValue& pkv) const
  {
    usize item_diff = &pkv - this->base_pkv;
    usize pkv_item_offset = this->base_absolute_offset + (item_diff * 4);
    return pkv_item_offset + pkv.key_offset;
  }

  bool operator()(u32 target_key_data_offset, const PackedKeyValue& pkv) const
  {
    return target_key_data_offset < this->get_key_data_offset(pkv);
  }
};

/** \brief A comparator used to compare a shifted/sharded PackedKeyValue range to a given comparison
 * key.
 */
struct ShiftedPackedKeyDataCompare {
  isize offset_delta;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyView get_key_view(const PackedKeyValue& pkv) const
  {
    return pkv.shifted_key_view(offset_delta);
  }

  bool operator()(const PackedKeyValue& pkv, const KeyView& comp_key) const
  {
    return KeyOrder{}(this->get_key_view(pkv), comp_key);
  }
};

}  // namespace turtle_kv
