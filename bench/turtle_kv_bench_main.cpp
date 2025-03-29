#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/import/env.hpp>
#include <turtle_kv/kv_store.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/dump_tasks.hpp>
#include <batteries/async/task.hpp>

#include <boost/asio/io_context.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using turtle_kv::getenv_as;
using turtle_kv::KVStore;
using turtle_kv::RemoveExisting;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::testing::run_workload;

int main(int argc, char** argv)
{
  batt::enable_dump_tasks();

  boost::asio::io_context io;

  batt::Task main_task{
      io.get_executor(),
      [&] {
        // batt::require_fail_global_default_log_level().store(batt::LogLevel::kInfo);

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        //
        //
        std::filesystem::path test_kv_store_dir = "/mnt/kv-bakeoff/turtle_kv_bench/kv_store";

        const usize chi = getenv_as<usize>("CHI").value_or(1);

        KVStore::Config kv_store_config;

        kv_store_config.initial_capacity_bytes = 128 * kGiB;
        kv_store_config.change_log_size_bytes = 256 * kMiB;

        StatusOr<llfs::ScopedIoRing> scoped_io_ring =
            llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096}, llfs::ThreadPoolSize{1});

        BATT_CHECK_OK(scoped_io_ring);

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

        auto p_storage_context =
            llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                              scoped_io_ring->get_io_ring());

        StatusOr<std::unique_ptr<KVStore>> kv_store_opened =
            KVStore::open(batt::Runtime::instance().default_scheduler(),
                          batt::WorkerPool::default_pool(),
                          *p_storage_context,
                          test_kv_store_dir,
                          kv_store_config.tree_options);

        BATT_CHECK_OK(kv_store_opened);

        LOG(INFO) << "KVStore opened";

        KVStore& kv_store = **kv_store_opened;

        kv_store.set_checkpoint_distance(chi);

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        //
        //
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

            LOG(INFO) << BATT_INSPECT(chi) << " | " << time_points[i].label << " " << rate
                      << " ops/sec";
          }

          LOG(INFO) << kv_store.debug_info();
        }
      },
      "main",
  };

  io.run();
  main_task.join();

  return 0;
}
