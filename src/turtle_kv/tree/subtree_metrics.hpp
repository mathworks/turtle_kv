#pragma once
#define TURTLE_KV_TREE_SUBTREE_METRICS_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <array>

namespace turtle_kv {

struct SubtreeMetrics {
  using Self = SubtreeMetrics;

  static constexpr i32 kMaxTreeHeight = 10;
  static constexpr i32 kMaxLevelsPerNode = 6;

  static constexpr i32 node_level_height(i32 node_level)
  {
    return Self::kMaxLevelsPerNode - (node_level + 1);
  }

  static constexpr i32 tree_level_from_height_and_node_level(i32 height, i32 node_level)
  {
    return (height > 1)
               ? (2 + kMaxLevelsPerNode * (height - 2) + Self::node_level_height(node_level))
               : height;
  }

  static constexpr i32 kMaxTreeLevels = kMaxTreeHeight * kMaxLevelsPerNode;

  std::array<CountMetric<u64>, kMaxTreeHeight> flush_count_per_height;
};

// Sanity checks.
//
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(0, 0) == 0);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(0, 999) == 0);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(1, 0) == 1);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(1, 999) == 1);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 5) == 2);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 4) == 3);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 3) == 4);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 2) == 5);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 1) == 6);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(2, 0) == 7);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 5) == 8);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 4) == 9);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 3) == 10);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 2) == 11);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 1) == 12);
static_assert(SubtreeMetrics::tree_level_from_height_and_node_level(3, 0) == 13);

}  // namespace turtle_kv
