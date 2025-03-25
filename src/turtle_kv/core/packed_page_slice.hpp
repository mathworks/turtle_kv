#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>

namespace turtle_kv {

struct PackedPageSlice {
  little_u32 offset;
  little_u32 size;
  llfs::PackedPageId page_id;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageSlice), 16);

inline usize packed_sizeof(const PackedPageSlice&)
{
  return sizeof(PackedPageSlice);
}

inline llfs::PageId get_page_id(const PackedPageSlice& page_slice)
{
  return get_page_id(page_slice.page_id);
}

inline batt::Status validate_packed_value(const PackedPageSlice& packed, const void* buffer_data,
                                          usize buffer_size)
{
  return llfs::validate_packed_struct(packed, buffer_data, buffer_size);
}

}  // namespace turtle_kv
