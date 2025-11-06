#pragma once
#define TURTLE_KV_CHECKPOINT_GENERATOR_METRICS_HPP

#include <turtle_kv/tree/batch_update_metrics.hpp>

#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct CheckpointGeneratorMetrics {
  LatencyMetric serialize_latency;
  LatencyMetric force_flush_all_latency;

  BatchUpdateMetrics batch_update;
};

}  // namespace turtle_kv
