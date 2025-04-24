#pragma once

#include <turtle_kv/import/int_types.hpp>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Filter Type Selection
// ~~~~~~~~~~~~~~~~~~~~~
//
// Exactly one of the following must be set to 1, the rest to 0:
//  - TURTLE_KV_USE_BLOOM_FILTER
//  - TURTLE_KV_USE_QUOTIENT_FILTER
//

/** \brief Set to 1 to enable Bloom Filters.
 */
#define TURTLE_KV_USE_BLOOM_FILTER 0

/** \brief Set to 1 to enable Quotient Filters.
 */
#define TURTLE_KV_USE_QUOTIENT_FILTER 1

#if !(TURTLE_KV_USE_BLOOM_FILTER == 0 || TURTLE_KV_USE_BLOOM_FILTER == 1)
#error TURTLE_KV_USE_BLOOM_FILTER must be 0 or 1
#endif

#if !(TURTLE_KV_USE_QUOTIENT_FILTER == 0 || TURTLE_KV_USE_QUOTIENT_FILTER == 1)
#error TURTLE_KV_USE_QUOTIENT_FILTER must be 0 or 1
#endif

#if (TURTLE_KV_USE_BLOOM_FILTER + TURTLE_KV_USE_QUOTIENT_FILTER) != 1
#error You must choose one kind of filter!
#endif

/** \brief Whether filters are consulted during point queries.
 */
#define TURTLE_KV_ENABLE_LEAF_FILTERS 1

namespace turtle_kv {

constexpr i64 kNodeLruPriority = 800;
constexpr i64 kFilterLruPriority = 400;
constexpr i64 kTrieIndexLruPriority = 300;
constexpr i64 kLeafItemsShardLruPriority = 200;
constexpr i64 kLeafKeyDataShardLruPriority = 100;
constexpr i64 kLeafValueDataShardLruPriority = 20;
constexpr i64 kLeafLruPriority = 0;

constexpr i64 kNewPagePriorityBoost = 1000;

constexpr i64 kNewNodeLruPriority = kNodeLruPriority + kNewPagePriorityBoost;
constexpr i64 kNewFilterLruPriority = kFilterLruPriority + kNewPagePriorityBoost;
constexpr i64 kNewLeafLruPriority = kLeafLruPriority + kNewPagePriorityBoost;

}  // namespace turtle_kv
