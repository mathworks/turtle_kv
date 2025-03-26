#pragma once

#include <batteries/static_assert.hpp>

#include <cstdlib>

/** \brief Statically asserts that the data member `field` inside the record (class or struct) type
 * `type` is placed at byte offsets `from` .. `to`, and that `from` has alignment `alignment`.
 */
#define TURTLE_KV_ASSERT_PLACEMENT(type, field, from, to, alignment)                               \
  BATT_STATIC_ASSERT_EQ(offsetof(type, field), (from));                                            \
  BATT_STATIC_ASSERT_EQ(sizeof(type::field), (to) - (from));                                       \
  BATT_STATIC_ASSERT_EQ((from) % (alignment), 0)
