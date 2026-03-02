#pragma once
#define TURTLE_KV_ON_PAGE_CACHE_OVERCOMMIT_HPP

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/import/metrics.hpp>

#include <llfs/page_cache.hpp>

#include <batteries/metrics/metric_collectors.hpp>

#include <functional>
#include <ostream>

namespace turtle_kv {

struct OvercommitMetrics {
  /** \brief The total number of times overcommit was triggered.
   */
  CountMetric<u64> trigger_count;
};

void on_page_cache_overcommit(const std::function<void(std::ostream& out)>& context_fn,
                              llfs::PageCache& cache,
                              OvercommitMetrics& metrics);

}  // namespace turtle_kv
