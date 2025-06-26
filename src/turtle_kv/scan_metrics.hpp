#pragma once

#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct ScanMetrics {
  static ScanMetrics& instance()
  {
    static ScanMetrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  LatencyMetric checkpoint_set_next_item_latency;
  LatencyMetric memtable_set_next_item_latency;
  LatencyMetric deltas_set_next_item_latency;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset()
  {
    this->checkpoint_set_next_item_latency.reset();
    this->memtable_set_next_item_latency.reset();
    this->deltas_set_next_item_latency.reset();
  }
};

}  // namespace turtle_kv
