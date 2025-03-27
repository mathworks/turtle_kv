#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/core/strong_types.hpp>

#include <llfs/api_types.hpp>

#include <batteries/strong_typedef.hpp>

namespace turtle_kv {

BATT_STRONG_TYPEDEF(usize, TotalEditsSize);
BATT_STRONG_TYPEDEF(usize, TotalSlotsSize);
BATT_STRONG_TYPEDEF(bool, RemoveExisting);
BATT_STRONG_TYPEDEF(i64, BlockSize);
BATT_STRONG_TYPEDEF(i64, BlockCount);

using llfs::FileOffset;

}  // namespace turtle_kv
