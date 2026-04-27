//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_METRICS_HPP

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct MemTableMetrics {
  CountMetric<i64> alloc_count{0};
  CountMetric<i64> free_count{0};
  StatsMetric<i64> count_stats;
  CountMetric<i64> log_bytes_allocated{0};
  CountMetric<i64> log_bytes_freed{0};

  /** \brief The number of times a MemTable did a blocking ChangeLog append_slot operation after
   * failing a non-blocking one due to ChangeLog space exhaustion.
   */
  CountMetric<i64> wait_for_trim_count{0};

  /** \brief The number of times a MemTable had to be finalized before it hit the limit, due to
   * running out of ChangeLog space.
   */
  CountMetric<i64> storage_full_count{0};

  /** \brief Samples the byte size of the MemTable (total packed updates) when finalized for any
   * reason.
   */
  StatsMetric<i64> finalize_size_stats;

  /** \brief Samples the byte size of the MemTable (total packed updates) when finalized due to
   * running out of space in the ChangeLog.
   */
  StatsMetric<i64> storage_full_size_stats;

  /** \brief Metrics about page cache overcommit events.
   */
  OvercommitMetrics& overcommit;

  //----- --- -- -  -  -   -

  explicit MemTableMetrics(OvercommitMetrics* p_overcommit) noexcept : overcommit{*p_overcommit}
  {
  }
};

}  // namespace turtle_kv
