#pragma once
#define TURTLE_KV_TREE_SUBTREE_METRICS_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <array>

namespace turtle_kv {

struct SubtreeMetrics {
  using Self = SubtreeMetrics;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr i32 kMaxTreeHeight = 10;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of times a batch update has been applied at a given tree height.
   */
  std::array<CountMetric<u64>, kMaxTreeHeight + 1> batch_count_per_height;
};

}  // namespace turtle_kv
