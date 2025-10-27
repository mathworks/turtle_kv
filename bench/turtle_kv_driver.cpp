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
  this->kv_store_config.initial_capacity_bytes = 256 * kGiB;
  this->kv_store_config.change_log_size_bytes = 32 * kGiB;

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
  this->runtime_options.memtable_compact_threads = 4;
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
      this->shared_->saved_params[name] = value;
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
  }

  return OkStatus();
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
  if (this->thread_id() == None) {
    // Report workload-specific stats.
    //
    this->shared_->workload_stats.set_name_prefix(kMetricPrefix);
    this->shared_->workload_stats.emit_report_impl(this->thread_id(), dst);

    dst.report_param(
        keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = None,
            .param_name = kDiskPathParamName,
        },
        this->shared_->kv_store_path.string());

    dst.report_param(
        keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = None,
            .param_name = kCheckpointAfterWorkloadParamName,
        },
        batt::to_string(this->shared_->checkpoint_after_workload));
  }

  for (const auto& [name, value] : this->shared_->saved_params) {
    dst.report_param(
        keyvcr::ParamSpec{
            .workload_basename = "",
            .thread_id = this->thread_id(),
            .param_name = batt::to_string(kParamPrefix, name),
        },
        value);
  }
}

}  // namespace bench
}  // namespace turtle_kv
