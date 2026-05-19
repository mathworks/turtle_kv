//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/static_assert.hpp>

#include <cstdlib>
#include <type_traits>

namespace turtle_kv {

template <typename T>
inline T* place_first(void* start)
{
  return static_cast<T*>(start);
}

template <typename Next, typename Prev>
inline Next* place_next(Prev* prev, usize prev_count) noexcept
{
  return (Next*)(prev + prev_count);
}

template <typename T>
inline StatusOr<const T*> consume_first(ConstBuffer& src, usize count = 1)
{
  const usize byte_count = sizeof(T) * count;
  if (src.size() < byte_count) {
    return {batt::StatusCode::kOutOfRange};
  }
  auto on_scope_exit = batt::finally([&] {
    src += byte_count;
  });
  return static_cast<const T*>(src.data());
}

/** \brief Statically asserts that the data member `field` inside the record (class or struct) type
 * `type` is placed at byte offsets `from` .. `to`, and that `from` has alignment `alignment`.
 */
#define TURTLE_KV_ASSERT_PLACEMENT(type, field, from, to, alignment)                               \
  BATT_STATIC_ASSERT_EQ(offsetof(type, field), (from));                                            \
  BATT_STATIC_ASSERT_EQ(sizeof(type::field), (to) - (from));                                       \
  BATT_STATIC_ASSERT_EQ((from) % (alignment), 0)

}  // namespace turtle_kv
