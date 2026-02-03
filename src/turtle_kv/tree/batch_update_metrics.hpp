#pragma once
#define TURTLE_KV_TREE_BATCH_UPDATE_METRICS_HPP

#include <turtle_kv/config.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct BatchUpdateMetrics {
#if TURTLE_KV_PROFILE_UPDATES

  /** \brief The number of calls to merge compact.
   */
  CountMetric<u64> merge_compact_count;

  /** \brief The number of compactions that failed.
   */
  CountMetric<u64> merge_compact_failures;

  /** \brief The total number of result items for all compactions.
   */
  CountMetric<u64> merge_compact_key_count;

  /** \brief The number of prefix sums computed.
   */
  CountMetric<u64> running_total_count;

  /** \brief The number of flushes during a batch update (node to subtree).  The top level apply
   * batch update does *not* count as a flush.
   */
  CountMetric<u64> flush_count;

  /** \brief The number of node/leaf splits.
   */
  CountMetric<u64> split_count;

  /** \brief Controls the sampling rate for all latency metrics below.
   *
   * NOTE: `0` means collect a sample every 2^0 == 1 times an operation occurs.
   */
  Every2ToThe latency_sample_rate{0};

  /** \brief The time spent merge-compacting update buffer levels.
   */
  LatencyMetric merge_compact_latency;

  /** \brief The time spend computing prefix-sums for segmentation and pending bytes estimation.
   */
  LatencyMetric running_total_latency;

  StatsMetric<u64> merge_compact_key_count_stats;

#endif  // TURTLE_KV_PROFILE_UPDATES
};

}  // namespace turtle_kv
