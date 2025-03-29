#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <batteries/static_assert.hpp>

#include <cstdlib>

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

/** \brief Statically asserts that the data member `field` inside the record (class or struct) type
 * `type` is placed at byte offsets `from` .. `to`, and that `from` has alignment `alignment`.
 */
#define TURTLE_KV_ASSERT_PLACEMENT(type, field, from, to, alignment)                               \
  BATT_STATIC_ASSERT_EQ(offsetof(type, field), (from));                                            \
  BATT_STATIC_ASSERT_EQ(sizeof(type::field), (to) - (from));                                       \
  BATT_STATIC_ASSERT_EQ((from) % (alignment), 0)

}  // namespace turtle_kv
