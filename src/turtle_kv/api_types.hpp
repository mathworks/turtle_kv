//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_API_TYPES_HPP

#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/core/strong_types.hpp>

#include <llfs/api_types.hpp>

#include <batteries/strong_typedef.hpp>

namespace turtle_kv {

BATT_STRONG_TYPEDEF(bool, IsLastInGroup);
BATT_STRONG_TYPEDEF(bool, IsSizeTiered);
BATT_STRONG_TYPEDEF(bool, LockIsHeld);
BATT_STRONG_TYPEDEF(bool, RemoveExisting);
BATT_STRONG_TYPEDEF(i64, BlockCount);
BATT_STRONG_TYPEDEF(i64, BlockIndex);
BATT_STRONG_TYPEDEF(i64, BlockSize);
BATT_STRONG_TYPEDEF(usize, IndexInGroup);
BATT_STRONG_TYPEDEF(usize, TotalEditsSize);
BATT_STRONG_TYPEDEF(usize, TotalSlotsSize);

BATT_STRONG_TYPEDEF(i64, FileByteCount);
using llfs::FileOffset;

}  // namespace turtle_kv
