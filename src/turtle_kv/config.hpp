#pragma once

#if __cplusplus < 202002L
#error "This code requires at least C++20. Please compile with -std=c++20 or higher."
#endif

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

/** \brief Whether to use hash-based indexing in MemTables for faster point lookups.
 */
#define TURTLE_KV_MEM_TABLE_HASH_INDEX 1

/** \brief Whether filters are consulted during point queries.
 */
#define TURTLE_KV_ENABLE_LEAF_FILTERS 1

/** \brief Enable/disable explicit support for gperftools/tcmalloc.
 */
#define TURTLE_KV_ENABLE_TCMALLOC 0

/** \brief Enable/disable heap profiling support for gperftools/tcmalloc.  Only has an effect if
 * TURTLE_KV_ENABLE_TCMALLOC is 1.
 */
#define TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING 0

/** \brief Enable/disable collection of stats/metrics.
 */
#define TURTLE_KV_ENABLE_METRICS 1

/** \brief Changes layout of leaf pages: if 1, then keys and values are packed together, else (if
 * 0), keys are packed with keys, values with values.
 */
#define TURTLE_KV_PACK_KEYS_TOGETHER 0

/** \brief Enable/disable collection of detailed stats to profile update code paths.
 */
#define TURTLE_KV_PROFILE_UPDATES 1

/** \brief Enable/disable collection of detailed stats to profile query code paths.
 */
#define TURTLE_KV_PROFILE_QUERIES 1

/** \brief Enable/disable creating multiple batches from a single bigger memtable.
 */
#define TURTLE_KV_BIG_MEM_TABLES 1

namespace turtle_kv {

constexpr i64 kNodeLruPriority = 4;
constexpr i64 kFilterLruPriority = 3;
constexpr i64 kTrieIndexLruPriority = 2;
constexpr i64 kLeafItemsShardLruPriority = 1;
constexpr i64 kLeafKeyDataShardLruPriority = 1;
constexpr i64 kLeafValueDataShardLruPriority = 1;
constexpr i64 kLeafLruPriority = 1;

constexpr i64 kNewPagePriorityBoost = 0;

constexpr i64 kNewNodeLruPriority = kNodeLruPriority + kNewPagePriorityBoost;
constexpr i64 kNewFilterLruPriority = kFilterLruPriority + kNewPagePriorityBoost;
constexpr i64 kNewLeafLruPriority = kLeafLruPriority + kNewPagePriorityBoost;

constexpr u32 kDefaultLeafShardedViewSize = 4096;

}  // namespace turtle_kv
