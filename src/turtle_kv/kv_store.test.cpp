#include <turtle_kv/kv_store.hpp>
//
#include <turtle_kv/kv_store.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_root.test.hpp"

#include <turtle_kv/core/testing/generate.hpp>
#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/checkpoint_log.hpp>

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

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test" / "kv_create_and_open";

  std::thread test_thread{[&] {
    BATT_CHECK_OK(batt::pin_thread_to_cpu(0));

    for (bool size_tiered : {false, true}) {
      KVStore::Config kv_store_config = KVStore::Config::with_default_values();

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
            LOG(INFO) << BATT_INSPECT(m.apply_batch_latency);
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

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test" / "kv_scan_stress";

  const usize kNumKeys = 1 * 1000 * 1000;
  const double kNumScansPerKey = 0.15;
  const usize kMinScanLenLog2 = 1;
  const usize kMaxScanLenLog2 = 10;

  std::default_random_engine rng{/*seed=*/1};
  RandomStringGenerator generate_key;
  SequentialStringGenerator generate_value{100};
  std::uniform_int_distribution<usize> pick_scan_len_log2{kMinScanLenLog2, kMaxScanLenLog2};

  StdMapTable expected_table;

  KVStore::Config kv_store_config = KVStore::Config::with_default_values();

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class CheckpointTestParams
{
 public:
  CheckpointTestParams(u64 num_checkpoints_to_create, u64 num_puts)
      : num_checkpoints_to_create(num_checkpoints_to_create)
      , num_puts(num_puts)
  {
  }

  u64 num_checkpoints_to_create;
  u64 num_puts;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class CheckpointTest
    : public ::testing::Test
    , public testing::WithParamInterface<CheckpointTestParams>
{
 public:
  void SetUp() override
  {
    CheckpointTestParams checkpoint_test_params = GetParam();

    this->num_checkpoints_to_create = checkpoint_test_params.num_checkpoints_to_create;
    this->num_puts = checkpoint_test_params.num_puts;

  }  // namespace

  void TearDown() override
  {
    // Cleanup resources if necessary
  }

  u64 num_checkpoints_to_create;
  u64 num_puts;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_P(CheckpointTest, CheckpointRecovery)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test" / "kv_scan_stress";

  std::default_random_engine rng{/*seed=*/1};
  RandomStringGenerator generate_key;
  SequentialStringGenerator generate_value{100};

  auto kv_store_config = KVStore::Config::with_default_values();

  kv_store_config.initial_capacity_bytes = 0 * kMiB;
  kv_store_config.change_log_size_bytes = 512 * kMiB * 10;

  TreeOptions& tree_options = kv_store_config.tree_options;

  tree_options.set_node_size(4 * kKiB);
  tree_options.set_leaf_size(1 * kMiB);
  tree_options.set_key_size_hint(24);
  tree_options.set_value_size_hint(10);

  StatusOr<llfs::ScopedIoRing> scoped_io_ring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096},  //
                                   llfs::ThreadPoolSize{1});

  ASSERT_TRUE(scoped_io_ring.ok()) << BATT_INSPECT(scoped_io_ring.status());

  auto storage_context =
      llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),  //
                                        scoped_io_ring->get_io_ring());

  auto runtime_options = KVStore::RuntimeOptions::with_default_values();

  BATT_CHECK_OK(
      KVStore::configure_storage_context(*storage_context, tree_options, runtime_options));

  Status create_status =
      KVStore::create(*storage_context, test_kv_store_dir, kv_store_config, RemoveExisting{true});

  ASSERT_TRUE(create_status.ok()) << BATT_INSPECT(create_status);

  StatusOr<std::unique_ptr<KVStore>> open_result =
      KVStore::open(batt::Runtime::instance().default_scheduler(),
                    batt::WorkerPool::default_pool(),
                    *storage_context,
                    test_kv_store_dir,
                    tree_options,
                    runtime_options);

  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  KVStore& actual_table = **open_result;

  // Disable automatic checkpoints
  //
  actual_table.set_checkpoint_distance(99999999);

  std::map<std::string, std::string> expected_keys_values;

  u64 num_checkpoints_created = 0;
  u64 keys_per_checkpoint;

  if (this->num_checkpoints_to_create == 0) {
    keys_per_checkpoint = 0;
  } else {
    keys_per_checkpoint = std::floor((double)this->num_puts / this->num_checkpoints_to_create);
  }

  u64 keys_since_checkpoint = 0;

  for (u64 i = 0; i < this->num_puts; ++i) {
    std::string key = generate_key(rng);
    std::string value = generate_value();

    Status actual_put_status = actual_table.put(KeyView{key}, ValueView::from_str(value));

    expected_keys_values[key] = value;

    ASSERT_TRUE(actual_put_status.ok()) << BATT_INSPECT(actual_put_status);

    VLOG(3) << "Put key== " << key << ", value==" << value;

    ++keys_since_checkpoint;

    // Take a checkpoint after every keys_per_checkpoint puts. Skip
    //
    if (keys_since_checkpoint >= keys_per_checkpoint && this->num_checkpoints_to_create != 0) {
      keys_since_checkpoint = 0;
      ++num_checkpoints_created;
      BATT_CHECK_OK(actual_table.force_checkpoint());
      VLOG(2) << "Created " << num_checkpoints_created << " checkpoints";
      if (num_checkpoints_created == this->num_checkpoints_to_create) {
        break;
      }
    }
  }

  // Handle off by one error where we create one less checkpoint than expected
  //
  if (num_checkpoints_created < this->num_checkpoints_to_create) {
    BATT_CHECK_OK(actual_table.force_checkpoint());
    ++num_checkpoints_created;
    VLOG(1) << "Created " << num_checkpoints_created << " checkpoints after rounding error";
  }

  BATT_CHECK_EQ(num_checkpoints_created, this->num_checkpoints_to_create)
      << "Did not take the "
      << "correct number of checkponts. There is a bug in this test.";

  // Test recovering checkpoints after stress test
  //
  // TODO: [Gabe Bornstein 11/5/25] Is there a better way to check if all checkpoints are flushed
  // instead of waiting?
  //
  std::this_thread::sleep_for(std::chrono::seconds(1));
  actual_table.halt();
  actual_table.join();
  open_result->reset();

  batt::StatusOr<std::unique_ptr<llfs::Volume>> checkpoint_log_volume =
      turtle_kv::open_checkpoint_log(*storage_context,  //
                                     test_kv_store_dir / "checkpoint_log.llfs");

  BATT_CHECK_OK(checkpoint_log_volume);

  batt::StatusOr<turtle_kv::Checkpoint> checkpoint =
      KVStore::recover_latest_checkpoint(**checkpoint_log_volume);

  if (!checkpoint.ok()) {
    EXPECT_TRUE(checkpoint.ok());
    return;
  }

  // There is no checkpoint
  //
  if (checkpoint->batch_upper_bound() == turtle_kv::DeltaBatchId::from_u64(0)) {
    LOG(INFO) << "No checkpoint data found. Exiting the test before checking keys.";
    EXPECT_TRUE(this->num_checkpoints_to_create == 0 || this->num_puts == 0)
        << "Expected checkpoint data but found none.";
    return;
  }

  // Iterate over all keys and verify their corresponding value in the checkpoint is correct
  //
  for (const auto& [key, actual_value] : expected_keys_values) {
    turtle_kv::KeyView key_view{key};
    turtle_kv::PageSliceStorage slice_storage;
    std::unique_ptr<llfs::PageCacheJob> page_loader = (*checkpoint_log_volume)->new_job();
    turtle_kv::KeyQuery key_query{*page_loader, slice_storage, tree_options, key_view};

    batt::StatusOr<turtle_kv::ValueView> checkpoint_value = checkpoint->find_key(key_query);

    EXPECT_TRUE(checkpoint_value.ok()) << "Didn't find key: " << key;
  }
}

}  // namespace

// CheckpointTestParams == {num_puts, num_checkpoints_to_create}
//
INSTANTIATE_TEST_SUITE_P(
    RecoveringCheckpoints,
    CheckpointTest,
    testing::Values(
        // TODO: [Gabe Bornstein 11/5/25] Investigate: We aren't getting any
        // checkpoint data for this case, but we are forcing a checkpoint.
        // CheckpointTestParams(1, 1),
        // TODO: [Gabe Bornstein 11/5/25] Investigate: We aren't
        // getting any checkpoint data for this case, but we are
        // forcing a checkpoint. Maybe keys aren't being flushed?
        // CheckpointTestParams(1, 100),
        // TODO: [Gabe Bornstein 11/5/25] Investigate: We ARE
        // getting checkpoint data for this case. Does taking additional checkpoints flush keys?
        CheckpointTestParams(/* num_puts */ 2, /* num_checkpoints_to_create */ 100),
        CheckpointTestParams(/* num_puts */ 100, /* num_checkpoints_to_create */ 100),
        CheckpointTestParams(/* num_puts */ 1, /* num_checkpoints_to_create */ 100000),
        CheckpointTestParams(/* num_puts */ 1, /* num_checkpoints_to_create */ 0),
        CheckpointTestParams(/* num_puts */ 0, /* num_checkpoints_to_create */ 100),
        CheckpointTestParams(/* num_puts */ 5, /* num_checkpoints_to_create */ 100000),
        CheckpointTestParams(/* num_puts */ 10, /* num_checkpoints_to_create */ 100000)
        //  TODO: [Gabe Bornstein 11/6/25] Sporadic Failing. Likely cause by keys not
        //  being flushed before that last checkpoint is taken. Need fsync to resolve.
        /*CheckpointTestParams(101, 100000)*/));
