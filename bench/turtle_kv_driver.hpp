#pragma once
#define TURTLE_KV_TURTLE_KV_DRIVER_HPP

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

 protected:
  std::filesystem::path kv_store_path_;

  KVStore::Config kv_store_config_;

  KVStore::RuntimeOptions runtime_options_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class KVStoreDriver : private KVStoreDriverConfigBase
{
  friend void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst);

 public:
  using Super = KVStoreDriverConfigBase;
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

  KVStoreDriver() noexcept : Super{}
  {
  }

  explicit KVStoreDriver(Optional<u32> thread_id) noexcept : Super{}, thread_id_{thread_id}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStore& kv_store()
  {
    return *this->kv_store_;
  }

  Optional<u32> thread_id() const noexcept
  {
    return this->thread_id_;
  }

  //----- --- -- -  -  -   -

  Status begin_workload(std::string_view workload_basename);

  void end_workload();

  Status initialize_database();

  Status param(std::string_view name, std::string_view value);

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

  StatusOr<KVStoreDriver> create_thread(u32 child_thread_id);

  Status join_thread(u32 child_thread_id);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  static std::map<std::string, double> collect_stats_map(const KVStore& kv_store);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void emit_report_impl(keyvcr::ReportEmitter& dst);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::shared_ptr<KVStore> kv_store_;

  Optional<u32> thread_id_;

  std::map<std::string, std::string> saved_params_;

  std::string workload_basename_;

  std::shared_ptr<keyvcr::StatsSnapshotCollector<double>> workload_stats_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void emit_report(KVStoreDriver& src, keyvcr::ReportEmitter& dst)
{
  src.emit_report_impl(dst);
}

}  // namespace bench
}  // namespace turtle_kv
