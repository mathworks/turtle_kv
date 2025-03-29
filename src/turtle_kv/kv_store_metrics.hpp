#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct KVStoreMetrics {
  CountMetric<u64> checkpoint_count{0};
  CountMetric<u64> batch_count{0};
  CountMetric<u64> batch_edits_count{0};

  DerivedMetric<double> avg_edits_per_batch{[this] {
    return (double)this->batch_edits_count.load() / (double)this->batch_count.load();
  }};

  CountMetric<u64> mem_table_get_count{0};
  LatencyMetric mem_table_get_latency;
  std::array<CountMetric<u64>, 8> delta_log2_get_count{{{0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}}};
  LatencyMetric delta_get_latency;
  CountMetric<u64> checkpoint_get_count{0};
  LatencyMetric checkpoint_get_latency;

  LatencyMetric compact_batch_latency;
  LatencyMetric push_batch_latency;
  LatencyMetric finalize_checkpoint_latency;
  LatencyMetric append_job_latency;
};

}  // namespace turtle_kv
