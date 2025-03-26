#pragma once

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_pointer.hpp>

#include <batteries/static_assert.hpp>

namespace turtle_kv {

struct PackedNodePageKey {
  llfs::PackedPointer<char, little_u16> pointer;
};

// Verify the packed structure of PackedNodePageKey.
//
BATT_STATIC_ASSERT_EQ(sizeof(PackedNodePageKey), 2);

inline KeyView get_key(const PackedNodePageKey& key_ref) noexcept
{
  const PackedNodePageKey* this_key = std::addressof(key_ref);
  const PackedNodePageKey* next_key = this_key + 1;

  const usize length = next_key->pointer.get() - this_key->pointer.get();

  return KeyView{this_key->pointer.get(), length};
}

}  // namespace turtle_kv
