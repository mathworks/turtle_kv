#pragma once
#define TURTLE_KV_BENCH_METRIC_EVENT_COLLECTOR_HPP

#include <turtle_kv/kv_store_metrics.hpp>

#include <keyvcr/periodic_event_collector_thread.hpp>

#include <llfs/page_cache_slot.hpp>

namespace turtle_kv {
namespace bench {

class MetricEventCollectorThread : public keyvcr::PeriodicEventCollectorThread
{
 public:
  using Super = keyvcr::PeriodicEventCollectorThread;
  using Self = MetricEventCollectorThread;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  enum EventKind : u32 {
    kMemTableLogAllocBytes = 1,
    kMemTableLogFreeBytes = 2,
    kCacheAdmitBytes = 3,
    kCacheEvictBytes = 4,
    kCacheEraseBytes = 5,
    kCachePinBytes = 6,
    kCacheUnpinBytes = 7,
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MetricEventCollectorThread(std::chrono::microseconds polling_interval,
                                      KVStoreMetrics& metrics) noexcept
      : Super{polling_interval}
      , metrics_{metrics}
  {
  }

  ~MetricEventCollectorThread() noexcept
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  void collect_events_impl(std::chrono::steady_clock::time_point ts,
                           keyvcr::EventLog& event_log) override
  {
    auto& cache = llfs::PageCacheSlot::Pool::Metrics::instance();

    //----- --- -- -  -  -   -
    const auto collect_event = [ts, &event_log](u32 kind, double data) {
      keyvcr::EventLog::Event e;
      e.timestamp = ts;
      e.kind = kind;
      e.data = data;
      event_log.append(e);
    };
    //----- --- -- -  -  -   -

    collect_event(kMemTableLogAllocBytes, this->metrics_.mem_table_log_bytes_allocated.get());
    collect_event(kMemTableLogFreeBytes, this->metrics_.mem_table_log_bytes_freed.get());
    collect_event(kCacheEraseBytes, cache.erase_byte_count.get());
    collect_event(kCacheEvictBytes, cache.evict_byte_count.get());
    collect_event(kCacheUnpinBytes, cache.unpinned_byte_count.get());
    collect_event(kCacheAdmitBytes, cache.admit_byte_count.get());
    collect_event(kCachePinBytes, cache.pinned_byte_count.get());

    VLOG(1) << "events collected!" << BATT_INSPECT((void*)this);
  }

  std::string_view get_param_prefix() const override
  {
    return "turtlekv.stats.memory";
  }

  std::string_view get_event_name(u32 kind) const override
  {
    static const std::array<std::string_view, 7> names_ = {
        "turtlekv.stats.memory.log_alloc_bytes",
        "turtlekv.stats.memory.log_free_bytes",
        "turtlekv.stats.memory.cache_admit_bytes",
        "turtlekv.stats.memory.cache_evict_bytes",
        "turtlekv.stats.memory.cache_erase_bytes",
        "turtlekv.stats.memory.cache_pin_bytes",
        "turtlekv.stats.memory.cache_unpin_bytes",
    };
    BATT_CHECK_GE(kind, 1);
    BATT_CHECK_LE(kind, names_.size());
    return names_[kind - 1];
  }

  void on_start() override
  {
    VLOG(1) << "collector started" << BATT_INSPECT((void*)this);
  }

  void on_stop() override
  {
    VLOG(1) << "collector stopped" << BATT_INSPECT((void*)this);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  KVStoreMetrics& metrics_;
};

}  // namespace bench
}  // namespace turtle_kv
