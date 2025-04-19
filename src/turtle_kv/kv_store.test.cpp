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

    auto runtime_options = KVStore::RuntimeOptions::with_default_values();
    runtime_options.use_threaded_checkpoint_pipeline = true;

    for (usize chi : {1, 2, 3, 4, 5, 6, 7, 8}) {
      for (const char* workload_file : {
               "data/workloads/workload-abcdf.test.txt",
               "data/workloads/workload-abcdf.txt",
           }) {
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

        {
          auto page_cache_options = llfs::PageCacheOptions::with_default_values();

          const PageSize leaf_size = tree_options.leaf_size();
          const PageSize node_size = tree_options.node_size();
          const PageSize filter_size = tree_options.filter_page_size();
          const PageSize trie_index_size = tree_options.trie_index_sharded_view_size();

          LOG_FIRST_N(INFO, 1) << BATT_INSPECT(leaf_size) << BATT_INSPECT(node_size)
                               << BATT_INSPECT(filter_size) << BATT_INSPECT(trie_index_size);

          page_cache_options  //
              .set_max_cached_pages_per_size(node_size, (32 * kGiB) / node_size)
              .set_max_cached_pages_per_size(leaf_size, (32 * kGiB) / leaf_size)
              .set_max_cached_pages_per_size(filter_size, (4 * kGiB) / filter_size)
              .set_max_cached_pages_per_size(trie_index_size, (4 * kGiB) / trie_index_size)
              .add_sharded_view(leaf_size, PageSize{u32(4 * kKiB)})
              .add_sharded_view(leaf_size, trie_index_size);

          p_storage_context->set_page_cache_options(page_cache_options);
        }

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
