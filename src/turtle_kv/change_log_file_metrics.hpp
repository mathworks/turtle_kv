#pragma once

#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct ChangeLogFileMetrics {
  CountMetric<u64> freed_blocks_count;
  CountMetric<u64> reserved_blocks_count;
};

}  // namespace turtle_kv
