#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/import/env.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/kv_store.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/dump_tasks.hpp>
#include <batteries/async/task.hpp>
#include <batteries/cpu_align.hpp>

#include <boost/asio/io_context.hpp>

#include <pcg_random.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using batt::dump_size;

using turtle_kv::as_slice;
using turtle_kv::getenv_as;
using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::RateMetric;
using turtle_kv::RemoveExisting;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::TreeOptions;
using turtle_kv::ValueView;
using turtle_kv::testing::run_workload;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Default Values.
//
constexpr usize kDefaultN = 10 * 1000 * 1000;
constexpr usize kDefaultNodeSizeKb = 4;
constexpr usize kDefaultLeafSizeKb = 2048;
constexpr usize kDefaultWalSizeMb = 256;
constexpr usize kDefaultFilterBits = 12;

// kv_store_config.initial_capacity_bytes = 128 * kGiB;
// kv_store_config.change_log_size_bytes = 256 * kMiB;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Reads configuration from environment variables.
 */
void configure_kv(KVStore::Config* config, KVStore::RuntimeOptions* kv_runtime_options);

/** \brief Reads configuration from environment variables.
 */
void configure_page_cache(const KVStore::Config& config,
                          const KVStore::RuntimeOptions& kv_runtime_options,
                          llfs::StorageContext* storage_context);

/** \brief Runs the load test; argv[1] must be the name of a random data file.
 */
void run_load_bench(int argc, char** argv, KVStore& kv_store);

/** \brief Runs all the workload files in argv.
 */
void run_workloads(int argc, char** argv, KVStore& kv_store);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int main(int argc, char** argv)
{
  batt::enable_dump_tasks();

  boost::asio::io_context io;

  batt::Task main_task{
      io.get_executor(),
      [&] {
        // batt::require_fail_global_default_log_level().store(batt::LogLevel::kInfo);

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Configure the KVStore.
        //
        std::filesystem::path test_kv_store_dir = "/mnt/kv-bakeoff/turtle_kv_bench/kv_store";

        KVStore::Config kv_store_config;
        KVStore::RuntimeOptions kv_runtime_options;

        configure_kv(&kv_store_config, &kv_runtime_options);

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Initialize io_uring.
        //
        StatusOr<llfs::ScopedIoRing> scoped_io_ring =
            llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096}, llfs::ThreadPoolSize{1});

        BATT_CHECK_OK(scoped_io_ring);

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Create the KVStore.
        //
        {
          auto p_storage_context =
              llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                                scoped_io_ring->get_io_ring());

          Status create_status = KVStore::create(*p_storage_context,
                                                 test_kv_store_dir,
                                                 kv_store_config,
                                                 RemoveExisting{true});

          BATT_CHECK_OK(create_status);
        }

        LOG(INFO) << "KVStore created on disk";

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Open the KVStore we just created.
        //
        auto p_storage_context =
            llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                              scoped_io_ring->get_io_ring());

        BATT_CHECK_OK(KVStore::configure_storage_context(*p_storage_context,
                                                         kv_store_config.tree_options,
                                                         kv_runtime_options));

        StatusOr<std::unique_ptr<KVStore>> kv_store_opened =
            KVStore::open(batt::Runtime::instance().default_scheduler(),
                          batt::WorkerPool::default_pool(),
                          *p_storage_context,
                          test_kv_store_dir,
                          kv_store_config.tree_options,
                          kv_runtime_options);

        BATT_CHECK_OK(kv_store_opened);

        LOG(INFO) << "KVStore opened";

        KVStore& kv_store = **kv_store_opened;

        if (false) {
          std::array<usize, 8> main_thread_cores = {8, 9, 10, 11, 12, 13, 14, 15};
          BATT_CHECK_OK(batt::pin_thread_to_cpu_set(main_thread_cores));
        }

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        //
        const bool load_only = getenv_as<bool>("LOAD_ONLY").value_or(false);
        if (load_only) {
          run_load_bench(argc, argv, kv_store);
        } else {
          run_workloads(argc, argv, kv_store);
        }
        LOG(INFO) << kv_store.debug_info();
      },
      "main",
  };

  io.run();
  main_task.join();

  return 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void configure_kv(KVStore::Config* kv_store_config, KVStore::RuntimeOptions* kv_runtime_options)
{
#if 0  // TODO [tastolfi 2025-04-14] 
  auto disk_path =  //
      getenv_as<std::filesystem::path>( "turtlekv_disk_path").value_or_panic();
#endif

  auto node_size =  //
      getenv_as<usize>("turtlekv_node_size_kb").value_or(kDefaultNodeSizeKb) * kKiB;

  auto leaf_size =  //
      getenv_as<usize>("turtlekv_leaf_size_kb").value_or(kDefaultLeafSizeKb) * kKiB;

  auto wal_size =  //
      getenv_as<usize>("turtlekv_wal_size_mb").value_or(kDefaultWalSizeMb) * kMiB;

  auto storage_size =  //
      getenv_as<usize>("turtlekv_storage_size_gb").value_or(256) * kGiB;

  auto cache_size =  //
      getenv_as<usize>("turtlekv_cache_size_mb").value_or(65536) * kMiB;

  auto filter_bits =  //
      getenv_as<usize>("turtlekv_filter_bits").value_or(kDefaultFilterBits);

  auto key_size_hint =  //
      getenv_as<usize>("turtlekv_key_size_hint").value_or(24);

  auto value_size_hint =  //
      getenv_as<usize>("turtlekv_value_size_hint").value_or(100);

  auto buffer_level_trim =  //
      getenv_as<usize>("turtlekv_buffer_level_trim").value_or(1);

  auto min_flush_factor =  //
      getenv_as<double>("turtlekv_min_flush_factor").value_or(1);

  auto max_flush_factor =  //
      getenv_as<double>("turtlekv_max_flush_factor").value_or(1);

  auto checkpoint_distance =  //
      getenv_as<usize>("turtlekv_checkpoint_distance").value_or(8);

  bool checkpoint_pipeline =  //
      getenv_as<bool>("turtlekv_checkpoint_pipeline").value_or(true);

  auto tree_options = turtle_kv::TreeOptions::with_default_values();

  tree_options.set_leaf_size(leaf_size);
  tree_options.set_node_size(node_size);
  tree_options.set_key_size_hint(key_size_hint);
  tree_options.set_value_size_hint(value_size_hint);
  tree_options.set_filter_bits_per_key(filter_bits);
  tree_options.set_buffer_level_trim(buffer_level_trim);
  tree_options.set_min_flush_factor(min_flush_factor);
  tree_options.set_max_flush_factor(max_flush_factor);

  *kv_runtime_options = turtle_kv::KVStore::RuntimeOptions::with_default_values();

  kv_runtime_options->initial_checkpoint_distance = checkpoint_distance;
  kv_runtime_options->use_threaded_checkpoint_pipeline = checkpoint_pipeline;
  kv_runtime_options->cache_size_bytes = cache_size;

  {
    const char* pre = "\n# turtlekv ";

    const usize trie_index_reserve_size = tree_options.trie_index_reserve_size();

    std::cout  //
               // << pre << BATT_INSPECT(disk_path) //
        << pre << BATT_INSPECT(node_size) << " (" << dump_size(node_size) << ")"        //
        << pre << BATT_INSPECT(leaf_size) << " (" << dump_size(leaf_size) << ")"        //
        << pre << BATT_INSPECT(wal_size) << " (" << dump_size(wal_size) << ")"          //
        << pre << BATT_INSPECT(storage_size) << " (" << dump_size(storage_size) << ")"  //
        << pre << BATT_INSPECT(cache_size) << " (" << dump_size(cache_size) << ")"      //
        << pre << BATT_INSPECT(filter_bits)                                             //
        << pre << BATT_INSPECT(key_size_hint)                                           //
        << pre << BATT_INSPECT(value_size_hint)                                         //
        << pre << BATT_INSPECT(buffer_level_trim)                                       //
        << pre << BATT_INSPECT(min_flush_factor)                                        //
        << pre << BATT_INSPECT(max_flush_factor)                                        //
        << pre << BATT_INSPECT(trie_index_reserve_size)                                 //
        << pre << BATT_INSPECT(checkpoint_distance)                                     //
        << pre << BATT_INSPECT(checkpoint_pipeline)                                     //
        << std::endl;
  }

  kv_store_config->tree_options = tree_options;
  kv_store_config->initial_capacity_bytes = storage_size;
  kv_store_config->change_log_size_bytes = wal_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void run_load_bench(int argc, char** argv, KVStore& kv_store)
{
  --argc, ++argv;
  std::filesystem::path random_data_file{*argv};

  LOG(INFO) << "loading data file";
  std::string random_data_str;
  {
    std::ifstream ifs{random_data_file};
    std::ostringstream oss;
    BATT_CHECK(ifs.good());
    oss << ifs.rdbuf();
    random_data_str = std::move(oss).str();
  }
  LOG(INFO) << "data file loaded; size=" << random_data_str.size();

  Slice<const char> random_data = as_slice(random_data_str);

  BATT_CHECK_GT(random_data.size(), 100 * 1000);

  const usize key_size = kv_store.tree_options().key_size_hint();
  const usize value_size = kv_store.tree_options().value_size_hint();
  const usize n_puts = getenv_as<usize>("N").value_or(kDefaultN);
  const usize n_unique = std::min(getenv_as<usize>("UNIQUE").value_or(n_puts),
                                  random_data.size() - std::max(key_size, value_size));

  BATT_CHECK_LE(n_unique, n_puts);

  pcg64_unique rng;
  std::uniform_int_distribution<usize> pick_data{0, n_unique};

  const usize ten_percent = (n_puts * 10 + 99) / 100;

  LOG(INFO) << BATT_INSPECT(n_puts);
  LOG(INFO) << BATT_INSPECT(n_unique);

  RateMetric<i64, /*seconds=*/100> put_rate;
  LatencyMetric put_latency;
  {
    LatencyTimer timer{put_latency, (n_puts + 500) / 1000};

    if (n_unique != n_puts) {
      for (usize j = 0; j < n_puts; ++j) {
        LOG_EVERY_N(INFO, ten_percent)
            << "turtlekv_bench_progress " << j * 100 / n_puts << "%" << [&](std::ostream& out) {
                 put_rate.update(j);
                 out << " puts/second= " << put_rate.get();
               };

        usize i = pick_data(rng);
        const char* src_ptr = random_data.begin() + i;

        const KeyView key{src_ptr, key_size};
        const ValueView value = ValueView::from_str(std::string_view{src_ptr, value_size});

        BATT_CHECK_OK(kv_store.put(key, value));
      }
    } else {
      LOG(INFO) << "Using sequential scan of random data";

      const char* src_ptr = random_data.begin();
      const char* end_ptr = src_ptr + n_puts;

      for (usize j = 0; src_ptr != end_ptr; ++src_ptr, ++j) {
        LOG_EVERY_N(INFO, ten_percent)
            << "turtlekv_bench_progress " << j * 100 / n_puts << "%" << [&](std::ostream& out) {
                 put_rate.update(j);
                 out << " puts/second= " << put_rate.get();
               };

        const KeyView key{src_ptr, key_size};
        const ValueView value = ValueView::from_str(std::string_view{src_ptr, value_size});

        BATT_CHECK_OK(kv_store.put(key, value));
      }
    }
  }

  std::cout << "# Load throughput (KTPS)" << std::endl
            << "[ycsbc output]	turtlekv	workloads/load.spec	1	"
            << put_latency.rate_per_second() << std::endl;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void run_workloads(int argc, char** argv, KVStore& kv_store)
{
  for (--argc, ++argv; argc > 0; --argc, ++argv) {
    std::filesystem::path workload_file{*argv};
    LOG(INFO) << "Loading workload file " << workload_file;

    auto [op_count, time_points] = run_workload(workload_file, kv_store);

    LOG(INFO) << "--";
    LOG(INFO) << workload_file;
    LOG(INFO) << BATT_INSPECT(op_count) << BATT_INSPECT(kv_store.metrics().checkpoint_count);
    {
      auto& m = kv_store.metrics();
      LOG(INFO) << BATT_INSPECT(m.avg_edits_per_batch());
      LOG(INFO) << BATT_INSPECT(m.compact_batch_latency);
      LOG(INFO) << BATT_INSPECT(m.push_batch_latency);
      LOG(INFO) << BATT_INSPECT(m.finalize_checkpoint_latency);
      LOG(INFO) << BATT_INSPECT(m.append_job_latency);
    }

    for (usize i = 1; i < time_points.size(); ++i) {
      double elapsed = (time_points[i].seconds - time_points[i - 1].seconds);
      double rate =
          (time_points[i].op_count - time_points[i - 1].op_count) / std::max(1e-10, elapsed);

      LOG(INFO) << time_points[i].label << " " << rate << " ops/sec";
    }
  }
}
