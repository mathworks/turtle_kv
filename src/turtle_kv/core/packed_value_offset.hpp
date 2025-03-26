#pragma once

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/static_assert.hpp>

namespace turtle_kv {

struct PackedValueOffset {
  PackedValueOffset(const PackedValueOffset&) = delete;
  PackedValueOffset& operator=(const PackedValueOffset&) = delete;

  little_u32 int_value;

  void set_pointer(const void* ptr)
  {
    BATT_CHECK_GT(ptr, (const void*)this);
    this->int_value = byte_distance(this, ptr);
  }

  const char* get_pointer() const
  {
    return static_cast<const char*>(advance_pointer(this, int_value.value()));
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedValueOffset), 4);
BATT_STATIC_ASSERT_EQ(alignof(PackedValueOffset), 1);

}  // namespace turtle_kv
