#include <turtle_kv/kv_store.hpp>
//
#include <turtle_kv/kv_store.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/core/table.hpp>

namespace {

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using llfs::PageSize;

using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::OkStatus;
using turtle_kv::RemoveExisting;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::StdMapTable;
using turtle_kv::Table;
using turtle_kv::ValueView;
using turtle_kv::testing::get_project_file;
using turtle_kv::testing::run_workload;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(KVStoreTest, CreateAndOpen)
{
  std::thread test_thread{[&] {
    BATT_CHECK_OK(batt::pin_thread_to_cpu(0));

    for (bool size_tiered : {false, true}) {
      std::filesystem::path test_kv_store_dir = "/mnt/kv-bakeoff/turtle_kv_Test/kv_store";

      KVStore::Config kv_store_config;

      kv_store_config.initial_capacity_bytes = 512 * kMiB;
      kv_store_config.change_log_size_bytes = 64 * kMiB;

      LOG(INFO) << BATT_INSPECT(kv_store_config.tree_options.filter_bits_per_key());

      auto& tree_options = kv_store_config.tree_options;

      tree_options.set_node_size(4 * kKiB);
      tree_options.set_leaf_size(1 * kMiB);
      tree_options.set_key_size_hint(24);
      tree_options.set_value_size_hint(10);
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
                llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),  //
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

}  // namespace
