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

  this->workload_basename_ = std::string{workload_basename};

  if (!this->workload_stats_) {
    this->workload_stats_ = std::make_shared<keyvcr::StatsSnapshotCollector<double>>();
  }

  this->workload_stats_->begin_workload(workload_basename,
                                        Self::collect_stats_map(*this->kv_store_));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStoreDriver::end_workload()
{
  if (this->workload_stats_) {
    this->workload_stats_->end_workload(this->workload_basename_,
                                        Self::collect_stats_map(*this->kv_store_));
  }

  this->workload_basename_ = "";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStoreDriver::param(std::string_view name, std::string_view value)
{
  LOG(INFO) << name << " == " << value;

  if (name == kDiskPathParamName) {
    this->kv_store_path_ = std::string{value};

  } else if (name.starts_with(kParamPrefix)) {
    BATT_REQUIRE_OK(parse_config(name.substr(kParamPrefix.size()),
                                 value,
                                 &this->kv_store_config_,
                                 &this->runtime_options_));
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
  if (this->thread_id_ == None) {
    // Report workload-specific stats.
    //
    if (this->workload_stats_) {
      this->workload_stats_->set_name_prefix(kMetricPrefix);
      this->workload_stats_->emit_report_impl(this->thread_id_, dst);
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
