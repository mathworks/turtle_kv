#include "turtle_kv_driver.hpp"

#include <turtle_kv/kv_store.hpp>

#include <turtle_kv/util/env_param.hpp>

#include <keyvcr/sql.hpp>
#include <keyvcr/stats_collector.hpp>
#include <keyvcr/workload_player.hpp>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

namespace {

using namespace batt::int_types;

TURTLE_KV_ENV_PARAM(std::string, turtlekv_buffer_level_trim, "3");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_cache_size_mb, "65536");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_capacity_gb, "256");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_checkpoint_pipeline, "1");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_chi, "16");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_disk_path, "/mnt/kv-bakeoff/turtle_kv_data");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_filter_bits, "20");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_key_size_hint, "8");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_leaf_size_kb, "16384");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_max_flush, "2");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_min_flush, "1");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_node_size_kb, "4");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_size_tiered, "0");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_value_size_hint, "120");
TURTLE_KV_ENV_PARAM(std::string, turtlekv_wal_size_mb, "32768");

}  // namespace

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using Driver = keyvcr::StatsCollector<turtle_kv::bench::KVStoreDriver>;
using turtle_kv::getenv_param;
using turtle_kv::Optional;
using turtle_kv::Status;

void usage(int argc, char** argv);

void apply_params(keyvcr::WorkloadPlayer<Driver>& player);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int main(int argc, char** argv)
{
  if (argc < 2) {
    usage(argc, argv);
    return 1;
  }

  std::string workload_file_path = argv[1];
  std::string output_sql_path = "";

  if (argc >= 3) {
    output_sql_path = argv[2];
  }

  // Create the workload player.
  //
  keyvcr::WorkloadPlayer<Driver> player{workload_file_path};

  // Configure database-specific params.
  //
  apply_params(player);

  // Run the workload.
  //
  Status status = player.play();
  std::cerr << BATT_INSPECT(status) << std::endl;

  // Report stats.
  //
  Optional<std::ofstream> ofs;
  std::ostream* out = &std::cout;
  if (!output_sql_path.empty()) {
    ofs.emplace(output_sql_path);
    out = &*ofs;
  }
  keyvcr::SqlDumper report_emitter{keyvcr::generate_run_id(), *out};
  keyvcr::emit_report(player.consumer(), report_emitter);

  return 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void usage(int argc, char** argv)
{
  std::cerr << "usage: " << argv[0] << " WORKLOAD_FILE_PATH" << std::endl;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void apply_params(keyvcr::WorkloadPlayer<Driver>& player)
{
  BATT_CHECK_OK(player.consumer().param("dbname", "turtlekv"));

  BATT_CHECK_OK(player.consumer().param("turtlekv.buffer_level_trim",  //
                                        getenv_param<turtlekv_buffer_level_trim>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.cache_size_mb",  //
                                        getenv_param<turtlekv_cache_size_mb>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.capacity_gb",  //
                                        getenv_param<turtlekv_capacity_gb>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.checkpoint_pipeline",  //
                                        getenv_param<turtlekv_checkpoint_pipeline>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.chi",  //
                                        getenv_param<turtlekv_chi>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.disk_path",  //
                                        getenv_param<turtlekv_disk_path>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.filter_bits",  //
                                        getenv_param<turtlekv_filter_bits>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.key_size_hint",  //
                                        getenv_param<turtlekv_key_size_hint>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.leaf_size_kb",  //
                                        getenv_param<turtlekv_leaf_size_kb>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.max_flush",  //
                                        getenv_param<turtlekv_max_flush>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.min_flush",  //
                                        getenv_param<turtlekv_min_flush>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.node_size_kb",  //
                                        getenv_param<turtlekv_node_size_kb>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.size_tiered",  //
                                        getenv_param<turtlekv_size_tiered>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.value_size_hint",  //
                                        getenv_param<turtlekv_value_size_hint>()));

  BATT_CHECK_OK(player.consumer().param("turtlekv.wal_size_mb",  //
                                        getenv_param<turtlekv_wal_size_mb>()));
}
