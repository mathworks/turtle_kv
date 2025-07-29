#pragma once

#include <turtle_kv/config.hpp>
//

#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/metrics/metric_registry.hpp>

namespace turtle_kv {

template <typename T>
struct NullCountMetric {
  NullCountMetric() noexcept
  {
  }

  NullCountMetric(T) noexcept
  {
  }

  void set(T) noexcept
  {
  }

  void store(T) noexcept
  {
  }

  template <typename D>
  void add(D) noexcept
  {
  }

  template <typename D>
  T fetch_add(D) noexcept
  {
    return {};
  }

  T get() const noexcept
  {
    return {};
  }

  T load() const noexcept
  {
    return {};
  }

  void reset() noexcept
  {
  }

  void clamp_min(T) noexcept
  {
  }

  void clamp_max(T) noexcept
  {
  }
};

template <typename T>
struct NullFastCountMetric {
  NullFastCountMetric() noexcept
  {
  }

  explicit NullFastCountMetric(T) noexcept
  {
  }

  usize id() const noexcept
  {
    return 0;
  }

  template <typename D>
  void add(D) noexcept
  {
  }

  T get() const noexcept
  {
    return {};
  }

  void set(T) noexcept
  {
  }

  void reset() noexcept
  {
  }

  T load() const noexcept
  {
    return {};
  }
};

struct NullLatencyMetric {
  void update(std::chrono::steady_clock::time_point, u64 = 1) noexcept
  {
  }

  void update(std::chrono::steady_clock::duration, u64 = 1) noexcept
  {
  }

  double nonzero_count() const noexcept
  {
    return -1;
  }

  double nonzero_total_nanos() const noexcept
  {
    return -1;
  }

  double rate_per_second() const noexcept
  {
    return -1;
  }

  double total_usec() const noexcept
  {
    return -1;
  }

  double total_msec() const noexcept
  {
    return -1;
  }

  double total_seconds() const noexcept
  {
    return -1;
  }

  double average_usec() const noexcept
  {
    return -1;
  }

  void reset() noexcept
  {
  }

  NullCountMetric<u64> total_nanos;
  NullCountMetric<u64> count;
};

struct NullLatencyTimer {
  NullLatencyTimer() = default;

  template <typename... Args>
  NullLatencyTimer(Args&&...) noexcept
  {
  }

  template <typename... Args>
  void start(Args&&...) noexcept
  {
  }

  template <typename... Args>
  void start_sampled(Args&&...) noexcept
  {
  }

  bool is_active() const noexcept
  {
    return false;
  }

  i64 read_nanos() const noexcept
  {
    return -1;
  }

  i64 read_usec() const noexcept
  {
    return -1;
  }

  void stop()
  {
  }
};

template <typename T>
struct NullStatsMetric {
  NullStatsMetric() = default;

  explicit NullStatsMetric(T) noexcept
  {
  }

  void reset()
  {
  }

  template <typename D>
  void update(D)
  {
  }

  T count() const
  {
    return {};
  }

  T total() const
  {
    return {};
  }

  T max() const
  {
    return {};
  }

  T min() const
  {
    return {};
  }

  double average() const
  {
    return 0;
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

#define TURTLE_KV_NULL_METRIC_OUTPUT(type)                                                         \
  inline std::ostream& operator<<(std::ostream& out, const type&) noexcept                         \
  {                                                                                                \
    return out << "(disabled)";                                                                    \
  }

#define TURTLE_KV_NULL_METRIC_OUTPUT_TMPL(type)                                                    \
  template <typename T>                                                                            \
  TURTLE_KV_NULL_METRIC_OUTPUT(type<T>)

TURTLE_KV_NULL_METRIC_OUTPUT_TMPL(NullCountMetric)
TURTLE_KV_NULL_METRIC_OUTPUT_TMPL(NullFastCountMetric)
TURTLE_KV_NULL_METRIC_OUTPUT_TMPL(NullStatsMetric)
TURTLE_KV_NULL_METRIC_OUTPUT(NullLatencyMetric)

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

#if TURTLE_KV_ENABLE_METRICS

using ::batt::CountMetric;
using ::batt::FastCountMetric;
using ::batt::LatencyMetric;
using ::batt::LatencyTimer;
using ::batt::StatsMetric;

#define TURTLE_KV_COLLECT_LATENCY BATT_COLLECT_LATENCY
#define TURTLE_KV_COLLECT_LATENCY_N BATT_COLLECT_LATENCY_N
#define TURTLE_KV_COLLECT_LATENCY_SAMPLE BATT_COLLECT_LATENCY_SAMPLE

#else

template <typename T>
using CountMetric = NullCountMetric<T>;

template <typename T>
using FastCountMetric = NullFastCountMetric<T>;

using LatencyMetric = NullLatencyMetric;
using LatencyTimer = NullLatencyTimer;

template <typename T>
using StatsMetric = NullStatsMetric<T>;

#define TURTLE_KV_COLLECT_LATENCY(metric, expr) expr
#define TURTLE_KV_COLLECT_LATENCY_N(metric, expr, count_delta) expr
#define TURTLE_KV_COLLECT_LATENCY_SAMPLE(rate_spec, metric, expr) expr

#endif

using ::batt::DerivedMetric;
using ::batt::Every2ToThe;
using ::batt::Every2ToTheConst;
using ::batt::global_metric_registry;
using ::batt::HistogramMetric;
using ::batt::RateMetric;
using ::batt::sample_metric_at_rate;

}  // namespace turtle_kv
