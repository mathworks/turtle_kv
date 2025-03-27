#pragma once

#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/metrics/metric_registry.hpp>

namespace turtle_kv {

using ::batt::CountMetric;
using ::batt::DerivedMetric;
using ::batt::Every2ToThe;
using ::batt::Every2ToTheConst;
using ::batt::global_metric_registry;
using ::batt::HistogramMetric;
using ::batt::LatencyMetric;
using ::batt::LatencyTimer;
using ::batt::RateMetric;
using ::batt::sample_metric_at_rate;
using ::batt::StatsMetric;

#define TURTLE_KV_COLLECT_LATENCY BATT_COLLECT_LATENCY
#define TURTLE_KV_COLLECT_LATENCY_N BATT_COLLECT_LATENCY_N

}  // namespace turtle_kv
