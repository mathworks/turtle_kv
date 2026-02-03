#include "turtle_kv_driver.hpp"
//

#include <turtle_kv/import/constants.hpp>

#include <keyvcr/utility.hpp>

namespace turtle_kv {
namespace bench {

namespace {

const std::string kDiskPathParamName = "turtlekv.disk_path";
const std::string kCheckpointAfterWorkloadParamName = "turtlekv.checkpoint_after_workload";
const std::string kParamPrefix = "turtlekv.";
const std::string kMetricPrefix = "turtlekv.";

const std::string kReconfigurePrefix = "reconfig.";

// Build config settings.
//
const std::string kBloomFilterConfigParamName = "turtlekv.config.bloom_filter";
const std::string kQuotientFilterConfigParamName = "turtlekv.config.quotient_filter";
const std::string kLeafFiltersConfigParamName = "turtlekv.config.leaf_filters";
const std::string kTcmallocConfigParamName = "turtlekv.config.tcmalloc";
const std::string kMetricsConfigParamName = "turtlekv.config.metrics";
const std::string kPackedKVConfigParamName = "turtlekv.config.packed_kv_layout";
const std::string kProfileHeapConfigParamName = "turtlekv.config.profile_heap";
const std::string kProfileUpdatesConfigParamName = "turtlekv.config.profile_updates";
const std::string kProfileQueriesConfigParamName = "turtlekv.config.profile_queries";
const std::string kBigMemTablesConfigParamName = "turtlekv.config.big_mem_tables";

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreDriver::SharedState::SharedState() noexcept
    : kv_store_path{"/mnt/kv-bakeoff/turtle_kv_data"}
    , kv_store_config{KVStore::Config::with_default_values()}
    , runtime_options{KVStore::RuntimeOptions::with_default_values()}
{
  // KVStore::Config
  //
  this->kv_store_config.initial_capacity_bytes = 128 * kGiB;
  this->kv_store_config.max_capacity_bytes = 128 * kGiB;
  this->kv_store_config.change_log_size_bytes = 10 * kGiB;

  // TreeOptions
  //
  {
    TreeOptions& tree_options = this->kv_store_config.tree_options;

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
  this->runtime_options.initial_checkpoint_distance = 16;
  this->runtime_options.use_threaded_checkpoint_pipeline = true;
  this->runtime_options.cache_size_bytes = 64 * kGiB;
  this->runtime_options.memtable_compact_threads = TURTLE_KV_BIG_MEM_TABLES ? 1 : 4;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreDriver::SharedState::~SharedState() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStoreDriver::KVStoreDriver() noexcept : shared_{std::make_shared<SharedState>()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStoreDriver::KVStoreDriver(Optional<u32> thread_id,
                                          std::shared_ptr<SharedState>&& shared) noexcept
    : shared_{std::move(shared)}
{
  this->thread_->id = thread_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::initialize_database()
{
  if (!this->shared_->kv_store) {
    BATT_REQUIRE_OK(KVStore::create(this->shared_->kv_store_path,    //
                                    this->shared_->kv_store_config,  //
                                    RemoveExisting{true}));

    VLOG(1) << BATT_INSPECT(this->shared_->runtime_options);

    // Capture all configuration.
    //
    for (const auto& [name, value] : config_to_string_list(&this->shared_->kv_store_config,  //
                                                           &this->shared_->runtime_options)) {
      const std::string qualified_name = batt::to_string(kParamPrefix, name);
      this->shared_->saved_params[qualified_name] = value;
    }

    // Open the KV store we just created.
    //
    BATT_ASSIGN_OK_RESULT(this->shared_->kv_store,
                          KVStore::open(this->shared_->kv_store_path,                 //
                                        this->shared_->kv_store_config.tree_options,  //
                                        this->shared_->runtime_options));
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::begin_workload(std::string_view workload_basename)
{
  BATT_REQUIRE_OK(this->initialize_database());

  BATT_CHECK_EQ(this->thread_->id, None);

  this->shared_->workload_basename = std::string{workload_basename};
  this->shared_->start_metric_events_collector();

  // Apply any workload-specific reconfigurations.
  //
  {
    auto iter = this->shared_->reconfig_params.find(std::string{workload_basename});
    if (iter != this->shared_->reconfig_params.end()) {
      for (const auto& [name, value] : iter->second) {
        if (name == "turtlekv.chi") {
          BATT_ASSIGN_OK_RESULT(usize chi, keyvcr::parse_param_value<usize>(value));
          LOG(INFO) << BATT_INSPECT(chi);
          this->kv_store().set_checkpoint_distance(chi);

        } else {
          LOG(WARNING) << "unknown reconfig param specified: " << BATT_INSPECT_STR(name)
                       << BATT_INSPECT_STR(value);
        }
      }
    }
  }

  this->shared_->workload_stats.begin_workload(workload_basename,
                                               Self::collect_stats_map(this->kv_store()));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreDriver::end_workload()
{
  BATT_CHECK_EQ(this->thread_->id, None);

  if (this->shared_->checkpoint_after_workload) {
    BATT_CHECK_OK(this->kv_store().force_checkpoint());
  }

  this->shared_->workload_stats.end_workload(this->shared_->workload_basename,
                                             Self::collect_stats_map(this->kv_store()));

  this->shared_->stop_metric_events_collector();

  this->shared_->workload_basename = "";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::param(std::string_view name, std::string_view value)
{
  LOG(INFO) << name << " == " << value;

  if (name == kDiskPathParamName) {
    this->shared_->kv_store_path = std::string{value};

  } else if (name == kCheckpointAfterWorkloadParamName) {
    BATT_ASSIGN_OK_RESULT(this->shared_->checkpoint_after_workload,
                          ::keyvcr::parse_param_value<bool>(value));

  } else if (name.starts_with(kParamPrefix)) {
    BATT_REQUIRE_OK(parse_config(name.substr(kParamPrefix.size()),
                                 value,
                                 &this->shared_->kv_store_config,
                                 &this->shared_->runtime_options));

  } else if (name.starts_with(kReconfigurePrefix)) {
    std::string_view workload_name_start = name.substr(kReconfigurePrefix.size());
    const usize workload_name_len = workload_name_start.find('.');

    if (workload_name_len < workload_name_start.size()) {
      std::string_view workload_name = workload_name_start.substr(0, workload_name_len);
      std::string_view reconfig_param_name = workload_name_start.substr(workload_name_len + 1);

      this->shared_->reconfig_params[std::string{workload_name}][std::string{reconfig_param_name}] =
          std::string{value};
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::attach_thread()
{
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreDriver::detach_thread()
{
  BATT_CHECK_NOT_NULLPTR(this->shared_->kv_store);

  this->shared_->kv_store->reset_thread_context();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<KVStoreDriver> KVStoreDriver::create_thread(u32 child_thread_id)
{
  BATT_CHECK_NOT_NULLPTR(this->shared_->kv_store);

  return KVStoreDriver{child_thread_id, batt::make_copy(this->shared_)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::join_thread(u32 child_thread_id)
{
  return OkStatus();
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
void KVStoreDriver::emit_report_impl(keyvcr::ReportEmitter& dst)
{
  //----- --- -- -  -  -   -
  const auto emit_param = [this, &dst](std::string_view name, auto&& value) {
    auto value_s = batt::to_string(BATT_FORWARD(value));
    dst.report_param(
        ::keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = this->thread_id(),
            .param_name = name,
        },
        value_s);
  };
  //----- --- -- -  -  -   -

  if (this->thread_id() == None) {
    //----- --- -- -  -  -   -
    // Report workload-specific stats.
    //
    this->shared_->workload_stats.set_name_prefix(kMetricPrefix);
    this->shared_->workload_stats.emit_report_impl(this->thread_id(), dst);

    emit_param(kDiskPathParamName, this->shared_->kv_store_path.string());
    emit_param(kCheckpointAfterWorkloadParamName, this->shared_->checkpoint_after_workload);

    emit_param(kBloomFilterConfigParamName, TURTLE_KV_USE_BLOOM_FILTER);
    emit_param(kQuotientFilterConfigParamName, TURTLE_KV_USE_QUOTIENT_FILTER);
    emit_param(kLeafFiltersConfigParamName, TURTLE_KV_ENABLE_LEAF_FILTERS);
    emit_param(kTcmallocConfigParamName, TURTLE_KV_ENABLE_TCMALLOC);
    emit_param(kMetricsConfigParamName, TURTLE_KV_ENABLE_METRICS);
    emit_param(kPackedKVConfigParamName, TURTLE_KV_PACK_KEYS_TOGETHER);
    emit_param(kProfileHeapConfigParamName, TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING);
    emit_param(kProfileUpdatesConfigParamName, TURTLE_KV_PROFILE_UPDATES);
    emit_param(kProfileQueriesConfigParamName, TURTLE_KV_PROFILE_QUERIES);
    emit_param(kBigMemTablesConfigParamName, TURTLE_KV_BIG_MEM_TABLES);

    for (const auto& [name, value] : this->shared_->saved_params) {
      emit_param(name, value);
    }

    for (const auto& [workload, reconfig_map] : this->shared_->reconfig_params) {
      for (const auto& [name, value] : reconfig_map) {
        dst.report_param(
            keyvcr::ParamSpec{
                .workload_basename = workload,
                .thread_id = batt::None,
                .param_name = name,
            },
            value);
      }
    }

    //----- --- -- -  -  -   -
    // Emit events from the memory stats collectors.
    //
    for (const SharedState::WorkloadMetricEvents& wme : this->shared_->workload_metric_events) {
      BATT_CHECK_NOT_NULLPTR(wme.event_collector);
      wme.event_collector->emit_report(wme.workload_basename, this->thread_id(), dst);
    }
  }
}

}  // namespace bench
}  // namespace turtle_kv
