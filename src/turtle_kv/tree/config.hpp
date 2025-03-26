#pragma once

namespace turtle_kv {

// The utilization ratio between the smallest allowed (non-root) node and the largest.
//
constexpr unsigned kMaxNodeUtilizationRatio = 3;

// The utilization ratio between the smallest allowed leaf and the largest.
//
constexpr unsigned kMaxLeafUtilizationRatio = 3;

// Controls whether pages are prefetched during batch updates to checkpoints.
//
// 0 == no prefetching
// 1 == (default) prefetch only pages we absolutely *know* we will need immediately
// 2 == prefetch aggressively/speculatively
//
// Default can be overridden at runtime with environment variable TURTLE_TREE_PREFETCH=<level>
//
unsigned tree_prefetch_level() noexcept;

constexpr unsigned kDefaultPrefetchLevel = 1;

}  // namespace turtle_kv
