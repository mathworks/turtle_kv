#pragma once

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <array>

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

  FastCountMetric<u64> scan_count{0};

  /** \brief The time it takes to compact a finalized MemTable to produce an update batch.
   */
  LatencyMetric compact_batch_latency;

  /** \brief The time spent in CheckpointGenerator::apply_batch.
   */
  LatencyMetric apply_batch_latency;

  /** \brief The time to allocate pages and serialize them.
   */
  LatencyMetric finalize_checkpoint_latency;
  LatencyMetric append_job_latency;

  StatsMetric<u64> checkpoint_pinned_pages_stats;

  StatsMetric<u64> obsolete_state_count_stats;

  CountMetric<i64> mem_table_alloc{0};
  CountMetric<i64> mem_table_free{0};
  StatsMetric<i64> mem_table_count_stats;
  CountMetric<i64> mem_table_log_bytes_allocated{0};
  CountMetric<i64> mem_table_log_bytes_freed{0};

  OvercommitMetrics overcommit;

#if TURTLE_KV_PROFILE_UPDATES

  //----- --- -- -  -  -   -
  // Update metrics.
  //
  FastCountMetric<u64> put_count;
  FastCountMetric<u64> put_retry_count;
  LatencyMetric put_latency;
  FastCountMetric<u64> put_memtable_full_count;
  LatencyMetric put_memtable_latency;
  LatencyMetric put_wait_trim_latency;
  LatencyMetric put_memtable_create_latency;
  LatencyMetric put_memtable_queue_push_latency;

#endif  // TURTLE_KV_PROFILE_UPDATES

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
