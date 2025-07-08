#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct KVStoreMetrics {
  FastCountMetric<u64> checkpoint_count{0};
  FastCountMetric<u64> batch_count{0};
  FastCountMetric<u64> batch_edits_count{0};

  DerivedMetric<double> avg_edits_per_batch{[this] {
    return (double)this->batch_edits_count.load() / (double)this->batch_count.load();
  }};

  FastCountMetric<u64> mem_table_get_count{0};
  LatencyMetric mem_table_get_latency;
  std::array<FastCountMetric<u64>, 32> delta_log2_get_count;
  LatencyMetric delta_get_latency;
  FastCountMetric<u64> checkpoint_get_count{0};
  LatencyMetric checkpoint_get_latency;

  LatencyMetric compact_batch_latency;
  LatencyMetric push_batch_latency;
  LatencyMetric finalize_checkpoint_latency;
  LatencyMetric append_job_latency;

  StatsMetric<u64> checkpoint_pinned_pages_stats;

  FastCountMetric<u64> mem_table_alloc{0};
  FastCountMetric<u64> mem_table_free{0};

  //----- --- -- -  -  -   -
  // Scan/Scanner related metrics.
  //
  LatencyMetric scan_latency;
  LatencyMetric scan_init_latency;
  LatencyMetric scan_delta_item_latency;
  LatencyMetric scan_checkpoint_item_latency;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 total_get_count() const
  {
    u64 total = this->mem_table_get_count.get();
    for (auto& delta_get_count : this->delta_log2_get_count) {
      total += delta_get_count.get();
    }
    total += this->checkpoint_get_count.get();

    return total;
  }
};

}  // namespace turtle_kv
