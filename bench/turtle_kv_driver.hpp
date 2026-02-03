#pragma once
#define TURTLE_KV_TURTLE_KV_DRIVER_HPP

#include "metric_event_collector.hpp"

#include <turtle_kv/kv_store.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <keyvcr/report.hpp>
#include <keyvcr/stats_snapshot.hpp>

#include <batteries/suppress.hpp>

#include <glog/logging.h>

#include <memory>
#include <string_view>
#include <unordered_map>

namespace turtle_kv {
namespace bench {

class KVStoreDriver;

void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class KVStoreDriverConfigBase
{
 public:
  KVStoreDriverConfigBase() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class KVStoreDriver
{
  friend void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst);

 public:
  using Self = KVStoreDriver;

  static constexpr i64 kDefaultMetricEventsPollingIntervalUsec = 1000 * 1000;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct SharedState {
    struct WorkloadMetricEvents {
      std::string workload_basename;
      std::unique_ptr<MetricEventCollectorThread> event_collector;
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    std::filesystem::path kv_store_path;

    KVStore::Config kv_store_config;

    KVStore::RuntimeOptions runtime_options;

    std::unique_ptr<KVStore> kv_store;

    /** \brief Captures the full config used to initialize the KVStore.
     */
    std::map<std::string, std::string> saved_params;

    /** \brief Save the params used to reconfigure based on workload start.
     */
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> reconfig_params;

    //----- --- -- -  -  -   -
    // Custom config params

    bool checkpoint_after_workload = false;

    i64 metric_events_polling_interval_usec = kDefaultMetricEventsPollingIntervalUsec;

    //----- --- -- -  -  -   -
    // Runtime state

    std::string workload_basename;

    keyvcr::StatsSnapshotCollector<double> workload_stats;

    std::vector<WorkloadMetricEvents> workload_metric_events;

    //----- --- -- -  -  -   -

    SharedState() noexcept;

    ~SharedState() noexcept;

    //----- --- -- -  -  -   -

    void start_metric_events_collector()
    {
      // Paranoia: stop any currently running memory stats thread.
      //
      this->stop_metric_events_collector();

      // Only collect if enabled.
      //
      if (this->metric_events_polling_interval_usec > 0) {
        BATT_CHECK_NOT_NULLPTR(this->kv_store);

        SharedState::WorkloadMetricEvents& m = this->workload_metric_events.emplace_back();

        m.workload_basename = this->workload_basename;
        m.event_collector = std::make_unique<MetricEventCollectorThread>(
            std::chrono::microseconds(this->metric_events_polling_interval_usec),
            this->kv_store->metrics());

        m.event_collector->start();
      }
    }

    void stop_metric_events_collector()
    {
      if (!this->workload_metric_events.empty()) {
        this->workload_metric_events.back().event_collector->stop();
      }
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct ThreadState {
    std::shared_ptr<SharedState> shared_;

    Optional<u32> id;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStoreDriver() noexcept;

  explicit KVStoreDriver(Optional<u32> thread_id, std::shared_ptr<SharedState>&& shared) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStore& kv_store()
  {
    return *this->shared_->kv_store;
  }

  Optional<u32> thread_id() const noexcept
  {
    return this->thread_->id;
  }

  //----- --- -- -  -  -   -

  Status begin_workload(std::string_view workload_basename);

  void end_workload();

  Status initialize_database();

  Status param(std::string_view name, std::string_view value);

  Status attach_thread();

  void detach_thread();

  Status put(std::string_view key, std::string_view value)
  {
    return this->kv_store().put(key, ValueView::from_str(value));
  }

  Status get(std::string_view key)
  {
    BATT_REQUIRE_OK(this->kv_store().get(key));
    return OkStatus();
  }

  Status scan_n(std::string_view key, usize count)
  {
    std::array<std::pair<KeyView, ValueView>, 512> out_buf;
    BATT_CHECK_LE(count, out_buf.size());

    StatusOr<usize> n_read =
        this->kv_store().scan(/*min_key=*/key, as_slice(out_buf.data(), count));
    BATT_CHECK_OK(n_read);

    return OkStatus();
  }

  StatusOr<KVStoreDriver> create_thread(u32 child_thread_id);

  Status join_thread(u32 child_thread_id);

  //----- --- -- -  -  -   -

  void emit_report_impl(keyvcr::ReportEmitter& dst);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  static std::map<std::string, double> collect_stats_map(const KVStore& kv_store);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::shared_ptr<SharedState> shared_;

  std::unique_ptr<ThreadState> thread_ = std::make_unique<ThreadState>();
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst)
{
  src.emit_report_impl(dst);
}

}  // namespace bench
}  // namespace turtle_kv
