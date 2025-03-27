#pragma once

#include <turtle_kv/int_types.hpp>

#include <string_view>

namespace turtle_kv {

/*
 * Layout:
 *
 * revision = 0
 * ~~~~~~~~~~~~
 *
 * key_len (little_u16; 2 bytes)
 * key bytes (variable)
 * value_bytes (till end)
 *
 * revision > 0
 * ~~~~~~~~~~~~
 * [0, 0] (2 bytes)
 * revision MOD 65535 (little_u16; 2 bytes)
 * skip_pointers (little_u32 array; 4 bytes each)
 * value_bytes (till end)
 *
 * First skip pointer is the previous revision.
 * Second skip pointer is current - 8 revisions.
 * Third skip pointer is current - 64 revisions.
 * N-th skip pointer is revision current - 8^(N-1) (N starts at 1)
 *
 */

inline usize skip_pointer_count_at_revision(u32 revision) noexcept
{
  return (__builtin_ctz(revision & 0xffff) / 3 /*log_2(8)*/) + 1;
}

inline usize packed_sizeof_change_log_slot(const std::string_view& key,
                                           const std::string_view& value,
                                           u32 revision) noexcept
{
  if (revision == 0) {
    return sizeof(little_u16) + key.size() + value.size();
  }

  return sizeof(little_u16) + sizeof(little_u16) +
         skip_pointer_count_at_revision(revision) * sizeof(little_u32) + value.size();
}

inline usize pack_change_log_slot(const std::string_view& key,
                                  const std::string_view& value,
                                  u32 revision,
                                  const Slice<const u32>& skip_pointers) noexcept
{
  // TODO [tastolfi 2025-02-14]
  return 0;
}

}  // namespace turtle_kv
