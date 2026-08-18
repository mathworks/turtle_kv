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

namespace turtle_kv {

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