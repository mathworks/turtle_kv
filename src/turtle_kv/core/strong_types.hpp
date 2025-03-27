#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <batteries/strong_typedef.hpp>

namespace turtle_kv {

// BATT_STRONG_TYPEDEF(usize, NumConcurrentPages);
// BATT_STRONG_TYPEDEF(usize, NumThreads);
BATT_STRONG_TYPEDEF(bool, HasPageRefs);
// BATT_STRONG_TYPEDEF(bool, IsDecayedToItems);
// BATT_STRONG_TYPEDEF(usize, SplitSegments);
BATT_STRONG_TYPEDEF(bool, IsRoot);
BATT_STRONG_TYPEDEF(i32, ParentNodeHeight);

}  // namespace turtle_kv
