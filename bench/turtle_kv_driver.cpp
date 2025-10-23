#include "turtle_kv_driver.hpp"
//

#include <turtle_kv/import/constants.hpp>

namespace turtle_kv {
namespace bench {

namespace {

const std::string kDiskPathParamName = "turtlekv.disk_path";
const std::string kParamPrefix = "turtlekv.";
const std::string kMetricPrefix = "turtlekv.";

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreDriverConfigBase::KVStoreDriverConfigBase() noexcept
    : kv_store_path_{"/mnt/kv-bakeoff/turtle_kv_data"}
    , kv_store_config_{KVStore::Config::with_default_values()}
    , runtime_options_{KVStore::RuntimeOptions::with_default_values()}
{
  // KVStore::Config
  //
  this->kv_store_config_.initial_capacity_bytes = 256 * kGiB;
  this->kv_store_config_.change_log_size_bytes = 32 * kGiB;

  // TreeOptions
  //
  {
    TreeOptions& tree_options = this->kv_store_config_.tree_options;

    tree_options.set_buffer_level_trim(3);
    tree_options.set_filter_bits_per_key(20);
    tree_options.set_key_size_hint(8);
    tree_options.set_leaf_size(16 * kMiB);
    tree_options.set_max_flush_factor(2);
    tree_options.set_min_flush_factor(1);
    tree_options.set_node_size(4 * kKiB);
    tree_options.set_size_tiered(false);
    tree_options.set_value_size_hint(120);
  }

  // KVStore::RuntimeOptions
  //
  this->runtime_options_.initial_checkpoint_distance = 16;
  this->runtime_options_.use_threaded_checkpoint_pipeline = true;
  this->runtime_options_.cache_size_bytes = 64 * kGiB;
  this->runtime_options_.memtable_compact_threads = 4;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::initialize_database()
{
  if (!this->kv_store_) {
    BATT_REQUIRE_OK(KVStore::create(this->kv_store_path_,    //
                                    this->kv_store_config_,  //
                                    RemoveExisting{true}));

    VLOG(1) << BATT_INSPECT(this->runtime_options_);

    // Capture all configuration.
    //
    for (const auto& [name, value] :
         config_to_string_list(&this->kv_store_config_, &this->runtime_options_)) {
      this->saved_params_[name] = value;
    }

    // Open the KV store we just created.
    //
    BATT_ASSIGN_OK_RESULT(this->kv_store_,
                          KVStore::open(this->kv_store_path_,                 //
                                        this->kv_store_config_.tree_options,  //
                                        this->runtime_options_));
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::begin_workload(std::string_view workload_basename)
{
  BATT_REQUIRE_OK(this->initialize_database());

  if (!this->workload_stats_) {
    this->workload_stats_ = std::make_shared<std::vector<StatsSnapshot>>();
  }
  auto& snapshot = this->workload_stats_->emplace_back(StatsSnapshot{
      .workload_basename = std::string{workload_basename},
      .before = {},
      .after = {},
  });
  this->kv_store_->collect_stats([&snapshot](std::string_view name, double value) {
    snapshot.before.emplace(name, value);
  });
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreDriver::end_workload()
{
  if (this->workload_stats_ && !this->workload_stats_->empty()) {
    auto& snapshot = this->workload_stats_->back();
    this->kv_store_->collect_stats([&snapshot](std::string_view name, double value) {
      snapshot.after.emplace(name, value);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::param(std::string_view name, std::string_view value)
{
  LOG(INFO) << name << " == " << value;

  if (name == kDiskPathParamName) {
    this->kv_store_path_ = std::string{value};

  } else if (name.starts_with(kParamPrefix)) {
    BATT_REQUIRE_OK(parse_config(name, value, &this->kv_store_config_, &this->runtime_options_));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<KVStoreDriver> KVStoreDriver::create_thread(u32 child_thread_id)
{
  BATT_CHECK_NOT_NULLPTR(this->kv_store_);

  auto child = KVStoreDriver{child_thread_id};
  child.kv_store_ = this->kv_store_;

  return child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::join_thread(u32 child_thread_id)
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::map<std::string, double> KVStoreDriver::StatsSnapshot::get_deltas() const noexcept
{
  std::map<std::string, double> delta;

  for (const auto& [name, before_value] : this->before) {
    auto iter = this->after.find(name);
    if (iter == this->after.end()) {
      continue;
    }
    const double after_value = iter->second;
    delta.emplace(name, after_value - before_value);
  }

  return delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ std::map<std::string, double> KVStoreDriver::collect_stats_map(const KVStore& kv_store)
{
  std::map<std::string, double> m;

  kv_store.collect_stats([&m](std::string_view name, double value) {
    m.emplace(name, value);
  });

  return m;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void KVStoreDriver::report_stats_map(const std::string& workload_basename,
                                                Optional<u32> thread_id,
                                                const std::map<std::string, double>& src,
                                                keyvcr::ReportEmitter& dst)
{
  //----- --- -- -  -  -   -
  const auto emit_metric = [&dst, &workload_basename, &thread_id](std::string_view name,
                                                                  double value) {
    dst.report_metric(
        keyvcr::MetricSpec{
            .workload_basename = workload_basename,
            .thread_id = thread_id,
            .metric_name = batt::to_string(kMetricPrefix, name),
        },
        value);
  };
  //----- --- -- -  -  -   -

  for (const auto& [name, value] : src) {
    emit_metric(name, value);

    static const std::string latency_count = "latency.count";
    static const std::string count = ".count";
    static const std::string seconds = ".seconds";
    static const std::string avg_seconds = ".avg_seconds";

    if (value != 0 && name.ends_with(latency_count)) {
      const std::string stem = name.substr(0, name.size() - count.size());
      auto iter = src.find(stem + seconds);
      const double sample_count = value;
      const double total_seconds = iter->second;
      if (iter != src.end() && total_seconds != 0) {
        const double avg_seconds_value = total_seconds / sample_count;
        emit_metric(stem + avg_seconds, avg_seconds_value);
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreDriver::emit_report_impl(keyvcr::ReportEmitter& dst)
{
  if (this->thread_id_ == None) {
    // Report overall stats for the entire run.
    //
    Self::report_stats_map(/*workload_basename=*/"",
                           this->thread_id_,
                           Self::collect_stats_map(*this->kv_store_),
                           dst);

    // Report workload-specific stats.
    //
    if (this->workload_stats_) {
      for (const KVStoreDriver::StatsSnapshot& snapshot : *this->workload_stats_) {
        Self::report_stats_map(snapshot.workload_basename,
                               this->thread_id_,
                               snapshot.get_deltas(),
                               dst);
      }
    }

    dst.report_param(
        keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = None,
            .param_name = kDiskPathParamName,
        },
        this->kv_store_path_.string());
  }

  for (const auto& [name, value] : this->saved_params_) {
    dst.report_param(
        keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = this->thread_id_,
            .param_name = batt::to_string(kParamPrefix, name),
        },
        value);
  }
}

}  // namespace bench
}  // namespace turtle_kv
