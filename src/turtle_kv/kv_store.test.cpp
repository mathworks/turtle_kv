#include <turtle_kv/kv_store.hpp>
//
#include <turtle_kv/kv_store.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_root.test.hpp"

#include <turtle_kv/core/testing/generate.hpp>
#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/core/table.hpp>

namespace {

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using llfs::PageSize;

using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::OkStatus;
using turtle_kv::RemoveExisting;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::StdMapTable;
using turtle_kv::Table;
using turtle_kv::TreeOptions;
using turtle_kv::ValueView;
using turtle_kv::testing::get_project_file;
using turtle_kv::testing::RandomStringGenerator;
using turtle_kv::testing::run_workload;
using turtle_kv::testing::SequentialStringGenerator;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(KVStoreTest, CreateAndOpen)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test/kv_scan_stress";

  std::thread test_thread{[&] {
    BATT_CHECK_OK(batt::pin_thread_to_cpu(0));

    for (bool size_tiered : {false, true}) {
      KVStore::Config kv_store_config;

      kv_store_config.initial_capacity_bytes = 512 * kMiB;
      kv_store_config.change_log_size_bytes = 64 * kMiB * 100;

      LOG(INFO) << BATT_INSPECT(kv_store_config.tree_options.filter_bits_per_key());

      TreeOptions& tree_options = kv_store_config.tree_options;

      tree_options.set_node_size(4 * kKiB);
      tree_options.set_leaf_size(1 * kMiB);
      tree_options.set_key_size_hint(24);
      tree_options.set_value_size_hint(10);
      if (!size_tiered) {
        tree_options.set_buffer_level_trim(3);
      }
      tree_options.set_size_tiered(size_tiered);

      auto runtime_options = KVStore::RuntimeOptions::with_default_values();
      runtime_options.use_threaded_checkpoint_pipeline = true;

      for (usize chi : {1, 2, 3, 4, 5, 6, 7, 8}) {
        for (const char* workload_file : {
                 "data/workloads/workload-abcdf.test.txt",
                 "data/workloads/workload-abcdf.txt",
                 "data/workloads/workload-e.test.txt",
                 "data/workloads/workload-e.txt",
             }) {
          if (size_tiered && std::strstr(workload_file, "workload-e")) {
            LOG(INFO) << "Skipping workload-e (scans) for size-tiered config";
            continue;
          }

          StatusOr<llfs::ScopedIoRing> scoped_io_ring =
              llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096},  //
                                           llfs::ThreadPoolSize{1});

          ASSERT_TRUE(scoped_io_ring.ok()) << BATT_INSPECT(scoped_io_ring.status());

          {
            auto p_storage_context =
                llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                                  //
                                                  scoped_io_ring->get_io_ring());

            Status create_status = KVStore::create(*p_storage_context,  //
                                                   test_kv_store_dir,   //
                                                   kv_store_config,     //
                                                   RemoveExisting{true});

            ASSERT_TRUE(create_status.ok()) << BATT_INSPECT(create_status);
          }

          auto p_storage_context =
              llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),  //
                                                scoped_io_ring->get_io_ring());

          BATT_CHECK_OK(KVStore::configure_storage_context(*p_storage_context,
                                                           tree_options,
                                                           runtime_options));

          StatusOr<std::unique_ptr<KVStore>> kv_store_opened =
              KVStore::open(batt::Runtime::instance().default_scheduler(),
                            batt::WorkerPool::default_pool(),
                            *p_storage_context,
                            test_kv_store_dir,
                            kv_store_config.tree_options,
                            runtime_options);

          ASSERT_TRUE(kv_store_opened.ok()) << BATT_INSPECT(kv_store_opened.status());

          KVStore& kv_store = **kv_store_opened;

          kv_store.set_checkpoint_distance(chi);

          auto [op_count, time_points] =
              run_workload(get_project_file(std::filesystem::path{workload_file}), kv_store);

          EXPECT_GT(op_count, 100000);

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

            LOG(INFO) << BATT_INSPECT(chi) << " | " << time_points[i].label << ": " << rate
                      << " ops/sec";
          }
        }
      }
    }
  }};
  test_thread.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(KVStoreTest, StdMapWorkloadTest)
{
  StdMapTable table;

  auto [op_count, _] = run_workload(
      get_project_file(std::filesystem::path{"data/workloads/workload-abcdef.test.txt"}),
      table);

  EXPECT_GT(op_count, 100000);

  LOG(INFO) << BATT_INSPECT(op_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(KVStoreTest, ScanStressTest)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test/kv_scan_stress";

  const usize kNumKeys = 1 * 1000 * 1000;
  const double kNumScansPerKey = 0.15;
  const usize kMinScanLenLog2 = 1;
  const usize kMaxScanLenLog2 = 10;

  std::default_random_engine rng{/*seed=*/1};
  RandomStringGenerator generate_key;
  SequentialStringGenerator generate_value{100};
  std::uniform_int_distribution<usize> pick_scan_len_log2{kMinScanLenLog2, kMaxScanLenLog2};

  StdMapTable expected_table;

  KVStore::Config kv_store_config;

  kv_store_config.initial_capacity_bytes = 0 * kMiB;
  kv_store_config.change_log_size_bytes = 512 * kMiB * 10;

  TreeOptions& tree_options = kv_store_config.tree_options;

  tree_options.set_node_size(4 * kKiB);
  tree_options.set_leaf_size(1 * kMiB);
  tree_options.set_key_size_hint(24);
  tree_options.set_value_size_hint(10);

  Status create_status = KVStore::create(test_kv_store_dir, kv_store_config, RemoveExisting{true});

  ASSERT_TRUE(create_status.ok()) << BATT_INSPECT(create_status);

  StatusOr<std::unique_ptr<KVStore>> open_result = KVStore::open(test_kv_store_dir, tree_options);

  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  KVStore& actual_table = **open_result;

  actual_table.set_checkpoint_distance(5);

  // Keep a histogram of scans per scan length (log scale).
  //
  std::array<usize, kMaxScanLenLog2 + 1> hist;
  hist.fill(0);

  usize n_scans = 0;

  for (usize i = 0; i < kNumKeys; ++i) {
    LOG_EVERY_N(INFO, kNumKeys / 10) << BATT_INSPECT(i) << BATT_INSPECT_RANGE(hist);

    std::string key = generate_key(rng);
    std::string value = generate_value();

    Status expected_put_status = expected_table.put(KeyView{key}, ValueView::from_str(value));
    Status actual_put_status = actual_table.put(KeyView{key}, ValueView::from_str(value));

    ASSERT_TRUE(expected_put_status.ok()) << BATT_INSPECT(expected_put_status);
    ASSERT_TRUE(actual_put_status.ok()) << BATT_INSPECT(actual_put_status);

    const usize target_scans = double(i + 1) * kNumScansPerKey;
    for (; n_scans < target_scans; ++n_scans) {
      std::string min_key = generate_key(rng);

      const usize scan_len_log2 = pick_scan_len_log2(rng);
      std::uniform_int_distribution<usize> pick_scan_len{usize{1} << (scan_len_log2 - 1),
                                                         (usize{1} << scan_len_log2)};
      const usize scan_len = pick_scan_len(rng);

      std::vector<std::pair<KeyView, ValueView>> expected_scan_result(scan_len);
      std::vector<std::pair<KeyView, ValueView>> actual_scan_result(scan_len);

      StatusOr<usize> expected_n = expected_table.scan(min_key, as_slice(expected_scan_result));
      StatusOr<usize> actual_n = actual_table.scan(min_key, as_slice(actual_scan_result));

      ASSERT_TRUE(expected_n.ok());
      ASSERT_TRUE(actual_n.ok());
      ASSERT_EQ(*expected_n, *actual_n);

      const usize n = *expected_n;

      hist[batt::log2_ceil(n)] += 1;

      for (usize k = 0; k < n; ++k) {
        if (actual_scan_result[k] != expected_scan_result[k]) {
          StatusOr<ValueView> v = actual_table.get(expected_scan_result[k].first);
          EXPECT_TRUE(v.ok());
          if (v.ok()) {
            EXPECT_EQ(*v, expected_scan_result[k].second);
          }
        }
        ASSERT_EQ(actual_scan_result[k], expected_scan_result[k])
            << BATT_INSPECT(k) << BATT_INSPECT(i) << BATT_INSPECT(n_scans)
            << BATT_INSPECT_STR(min_key) << BATT_INSPECT(expected_n) << BATT_INSPECT(actual_n)
            << BATT_INSPECT(scan_len);
      }
    }
  }

  LOG(INFO) << BATT_INSPECT(n_scans);
}

}  // namespace
