#pragma once

#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/metrics/metric_registry.hpp>

namespace turtle_kv {

using ::batt::CountMetric;
using ::batt::DerivedMetric;
using ::batt::Every2ToThe;
using ::batt::Every2ToTheConst;
using ::batt::FastCountMetric;
using ::batt::global_metric_registry;
using ::batt::HistogramMetric;
using ::batt::LatencyMetric;
using ::batt::LatencyTimer;
using ::batt::RateMetric;
using ::batt::sample_metric_at_rate;
using ::batt::StatsMetric;

}  // namespace turtle_kv
