#pragma once
#define TURTLE_KV_TURTLE_KV_DRIVER_HPP

#include <turtle_kv/kv_store.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <keyvcr/report.hpp>

#include <batteries/suppress.hpp>

#include <glog/logging.h>

#include <memory>
#include <string_view>
#include <unordered_map>

namespace turtle_kv {
namespace bench {

class KVStoreDriver;

void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst);

class KVStoreDriver
{
  friend void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst);

 public:
  using Self = KVStoreDriver;

  template <typename T>
  T parse_value(const std::string_view& sv)
  {
    std::optional<T> opt_value = batt::from_string<T>(std::string{sv});
    BATT_CHECK(opt_value);
    VLOG(1) << BATT_INSPECT(opt_value);
    return std::move(*opt_value);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStoreDriver() noexcept
  {
    this->kv_store_config_.tree_options = TreeOptions::with_default_values();
    this->kv_store_config_.initial_capacity_bytes = 256 * kGiB;
    this->kv_store_config_.change_log_size_bytes = 32 * kGiB;
    this->runtime_options_ = KVStore::RuntimeOptions::with_default_values();
  }

  explicit KVStoreDriver(Optional<u32> thread_id [[maybe_unused]]) noexcept : thread_id_{thread_id}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  TreeOptions& tree_options() noexcept
  {
    return this->kv_store_config_.tree_options;
  }

  Status initialize_kv_store()
  {
    if (!this->kv_store_) {
      BATT_REQUIRE_OK(KVStore::create(this->kv_store_path_,    //
                                      this->kv_store_config_,  //
                                      RemoveExisting{true}));

      VLOG(1) << BATT_INSPECT(this->runtime_options_);

      // Open the KV store we just created.
      //
      BATT_ASSIGN_OK_RESULT(this->kv_store_,
                            KVStore::open(this->kv_store_path_,                 //
                                          this->kv_store_config_.tree_options,  //
                                          this->runtime_options_));
    }
    return OkStatus();
  }

  KVStore& kv_store()
  {
    return *this->kv_store_;
  }

  Optional<u32> thread_id() const noexcept
  {
    return this->thread_id_;
  }

  //----- --- -- -  -  -   -

  Status handle_buffer_level_trim(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_buffer_level_trim(Self::parse_value<usize>(value));
    return OkStatus();
  }

  Status handle_cache_size_mb(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->runtime_options_.cache_size_bytes = Self::parse_value<usize>(value) * kMiB;
    return OkStatus();
  }

  Status handle_capacity_gb(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->kv_store_config_.initial_capacity_bytes = Self::parse_value<usize>(value) * kGiB;
    return OkStatus();
  }

  Status handle_checkpoint_pipeline(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->runtime_options_.use_threaded_checkpoint_pipeline = Self::parse_value<bool>(value);
    return OkStatus();
  }

  Status handle_chi(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->runtime_options_.initial_checkpoint_distance = Self::parse_value<usize>(value);
    return OkStatus();
  }

  Status handle_disk_path(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->kv_store_path_ = value;
    return OkStatus();
  }

  Status handle_filter_bits(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_filter_bits_per_key(Self::parse_value<usize>(value));

    return OkStatus();
  }

  Status handle_key_size_hint(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_key_size_hint(Self::parse_value<usize>(value));
    return OkStatus();
  }

  Status handle_leaf_size_kb(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_leaf_size(Self::parse_value<usize>(value) * kKiB);
    return OkStatus();
  }

  Status handle_max_flush(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_max_flush_factor(Self::parse_value<usize>(value));
    return OkStatus();
  }

  Status handle_min_flush(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_min_flush_factor(Self::parse_value<usize>(value));
    return OkStatus();
  }

  Status handle_node_size_kb(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_node_size(Self::parse_value<usize>(value) * kKiB);
    return OkStatus();
  }

  Status handle_size_tiered(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_size_tiered(Self::parse_value<bool>(value));
    return OkStatus();
  }

  Status handle_value_size_hint(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->tree_options().set_value_size_hint(Self::parse_value<usize>(value));
    return OkStatus();
  }

  Status handle_wal_size_mb(std::string_view name [[maybe_unused]], std::string_view value)
  {
    this->kv_store_config_.change_log_size_bytes = Self::parse_value<usize>(value) * kMiB;
    return OkStatus();
  }

  Status handle_workload_spec_basename(std::string_view name [[maybe_unused]],
                                       std::string_view value)
  {
    BATT_REQUIRE_OK(this->initialize_kv_store());

    if (!this->thread_id_) {
      if (!value.empty()) {
        if (!this->workload_stats_) {
          this->workload_stats_ = std::make_shared<std::vector<StatsSnapshot>>();
        }
        auto& snapshot = this->workload_stats_->emplace_back(StatsSnapshot{
            .workload_basename = std::string{value},
            .before = {},
            .after = {},
        });
        this->kv_store_->collect_stats([&snapshot](std::string_view name, double value) {
          snapshot.before.emplace(name, value);
        });
      } else {
        if (this->workload_stats_ && !this->workload_stats_->empty()) {
          auto& snapshot = this->workload_stats_->back();
          this->kv_store_->collect_stats([&snapshot](std::string_view name, double value) {
            snapshot.after.emplace(name, value);
          });
        }
      }
    }

    return OkStatus();
  }

  //----- --- -- -  -  -   -

  Status param(std::string_view name, std::string_view value)
  {
    LOG(INFO) << name << " == " << value;
    auto iter = this->param_handlers().find(name);
    if (iter != this->param_handlers().end()) {
      BATT_REQUIRE_OK((this->*iter->second)(name, value));
    }
    return OkStatus();
  }

  Status attach_thread()
  {
    return OkStatus();
  }

  void detach_thread()
  {
  }

  Status put(std::string_view key, std::string_view value)
  {
    return this->kv_store_->put(key, ValueView::from_str(value));
  }

  Status get(std::string_view key)
  {
    BATT_REQUIRE_OK(this->kv_store_->get(key));
    return OkStatus();
  }

  Status scan_n(std::string_view key, usize count)
  {
    std::array<std::pair<KeyView, ValueView>, 512> out_buf;
    BATT_CHECK_LE(count, out_buf.size());

    StatusOr<usize> n_read =
        this->kv_store_->scan(/*min_key=*/key, as_slice(out_buf.data(), count));
    BATT_CHECK_OK(n_read);

    return OkStatus();
  }

  StatusOr<KVStoreDriver> create_thread(u32 child_thread_id)
  {
    BATT_CHECK_NOT_NULLPTR(this->kv_store_);

    auto child = KVStoreDriver{child_thread_id};
    child.kv_store_ = this->kv_store_;

    return child;
  }

  Status join_thread(u32 child_thread_id)
  {
    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  using ParamHandlerMethod = Status (Self::*)(std::string_view, std::string_view);

  struct StatsSnapshot {
    std::string workload_basename;
    std::map<std::string, double> before;
    std::map<std::string, double> after;

    // ----- --- -- -  -  -   -

    std::map<std::string, double> get_deltas() const noexcept
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
  };

  static std::map<std::string, double> collect_stats_map(const KVStore& kv_store)
  {
    std::map<std::string, double> m;

    kv_store.collect_stats([&m](std::string_view name, double value) {
      m.emplace(name, value);
    });

    return m;
  }

  static void report_stats_map(const std::string& workload_basename,
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
              .metric_name = batt::to_string("turtlekv.", name),
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const std::unordered_map<std::string_view, ParamHandlerMethod>& param_handlers()
  {
    static const std::unordered_map<std::string_view, ParamHandlerMethod>& param_handlers_ =  //
        [] {
          static std::unordered_map<std::string_view, ParamHandlerMethod> param_handlers;

          param_handlers["turtlekv.buffer_level_trim"] = &Self::handle_buffer_level_trim;
          param_handlers["turtlekv.cache_size_mb"] = &Self::handle_cache_size_mb;
          param_handlers["turtlekv.capacity_gb"] = &Self::handle_capacity_gb;
          param_handlers["turtlekv.checkpoint_pipeline"] = &Self::handle_checkpoint_pipeline;
          param_handlers["turtlekv.chi"] = &Self::handle_chi;
          param_handlers["turtlekv.disk_path"] = &Self::handle_disk_path;
          param_handlers["turtlekv.filter_bits"] = &Self::handle_filter_bits;
          param_handlers["turtlekv.key_size_hint"] = &Self::handle_key_size_hint;
          param_handlers["turtlekv.leaf_size_kb"] = &Self::handle_leaf_size_kb;
          param_handlers["turtlekv.max_flush"] = &Self::handle_max_flush;
          param_handlers["turtlekv.min_flush"] = &Self::handle_min_flush;
          param_handlers["turtlekv.node_size_kb"] = &Self::handle_node_size_kb;
          param_handlers["turtlekv.size_tiered"] = &Self::handle_size_tiered;
          param_handlers["turtlekv.value_size_hint"] = &Self::handle_value_size_hint;
          param_handlers["turtlekv.wal_size_mb"] = &Self::handle_wal_size_mb;

          param_handlers["workload_spec.basename"] = &Self::handle_workload_spec_basename;

          return param_handlers;
        }();

    return param_handlers_;
  }

  void emit_report_impl(keyvcr::ReportEmitter& dst)
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
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::filesystem::path kv_store_path_;

  KVStore::Config kv_store_config_;

  KVStore::RuntimeOptions runtime_options_;

  std::shared_ptr<KVStore> kv_store_;

  Optional<u32> thread_id_;

  std::shared_ptr<std::vector<StatsSnapshot>> workload_stats_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst)
{
  src.emit_report_impl(dst);
}

}  // namespace bench
}  // namespace turtle_kv
