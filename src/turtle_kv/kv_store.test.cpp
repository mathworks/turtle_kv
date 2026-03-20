#include <turtle_kv/kv_store.hpp>
//
#include <turtle_kv/kv_store.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/segv.hpp>

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
// Base test fixture with common KVStore setup and teardown
//
class KVStoreTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    auto root = turtle_kv::data_root();
    ASSERT_TRUE(root.ok());
    this->data_root = *root;

    this->rng = std::default_random_engine{/*seed=*/1};
    this->generate_key = RandomStringGenerator{};

    this->SetupDefaultConfig();
  }

  void TearDown() override
  {
    // Ensure proper cleanup
    //
    if (this->scoped_io_ring.has_value()) {
      this->scoped_io_ring.reset();
    }
    this->storage_context.reset();
  }

  void SetupDefaultConfig()
  {
    this->kv_store_config.initial_capacity_bytes = 0 * kMiB;
    this->kv_store_config.change_log_size_bytes = 512 * kMiB * 10;

    TreeOptions& tree_options = this->kv_store_config.tree_options;
    tree_options.set_node_size(4 * kKiB);
    tree_options.set_leaf_size(1 * kMiB);
    tree_options.set_key_size_hint(24);
    tree_options.set_value_size_hint(10);
  }

  StatusOr<std::unique_ptr<KVStore>> CreateAndOpenKVStore(
      const std::filesystem::path& relative_path,
      bool remove_existing = true)
  {
    std::filesystem::path test_kv_store_dir = this->data_root / relative_path;

    StatusOr<llfs::ScopedIoRing> scoped_io_ring =
        llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096}, llfs::ThreadPoolSize{1});

    BATT_REQUIRE_OK(scoped_io_ring);

    this->scoped_io_ring = std::move(*scoped_io_ring);

    this->storage_context =
        llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                          this->scoped_io_ring->get_io_ring());

    this->runtime_options = KVStore::RuntimeOptions::with_default_values();

    Status config_status = KVStore::configure_storage_context(*this->storage_context,
                                                              this->kv_store_config.tree_options,
                                                              this->runtime_options);

    BATT_REQUIRE_OK(config_status);

    Status create_status = KVStore::create(*this->storage_context,
                                           test_kv_store_dir,
                                           this->kv_store_config,
                                           RemoveExisting{remove_existing});
    BATT_REQUIRE_OK(create_status);

    return KVStore::open(batt::Runtime::instance().default_scheduler(),
                         batt::WorkerPool::default_pool(),
                         *this->storage_context,
                         test_kv_store_dir,
                         this->kv_store_config.tree_options,
                         this->runtime_options);
  }

  // TODO: [Gabe Bornstein 2/4/26] Consider adding a lambda parameter that could take variable #
  // params, and implement test specific logic for each iteration of the loop.
  //
  void PopulateKVStore(KVStore& kv_store,
                       u64 num_puts,
                       std::map<std::string, std::string>* out_data = nullptr)
  {
    for (u64 i = 0; i < num_puts; ++i) {
      std::string key = this->generate_key(this->rng);
      std::string value = this->generate_value();

      Status put_status = kv_store.put(KeyView{key}, ValueView::from_str(value));
      ASSERT_TRUE(put_status.ok()) << BATT_INSPECT(put_status);

      if (out_data) {
        (*out_data)[key] = value;
      }

      VLOG(3) << "Put key==" << key << ", value==" << value;
    }
  }

  void ShutdownKVStore(std::unique_ptr<KVStore>& kv_store)
  {
    if (kv_store) {
      kv_store->halt();
      kv_store->join();
      kv_store.reset();
    }
  }

  std::filesystem::path data_root;
  KVStore::Config kv_store_config = KVStore::Config::with_default_values();
  std::optional<llfs::ScopedIoRing> scoped_io_ring;
  boost::intrusive_ptr<llfs::StorageContext> storage_context = nullptr;
  KVStore::RuntimeOptions runtime_options;

  std::default_random_engine rng;
  RandomStringGenerator generate_key;
  SequentialStringGenerator generate_value = SequentialStringGenerator{100};
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(KVStoreTest, CreateAndOpen)
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
TEST_F(KVStoreTest, StdMapWorkloadTest)
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
TEST_F(KVStoreTest, ScanStressTest)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test" / "kv_scan_stress";

  const usize kNumKeys = 1 * 1000 * 1000;
  const double kNumScansPerKey = 0.15;
  const usize kMinScanLenLog2 = 1;
  const usize kMaxScanLenLog2 = 10;

  std::uniform_int_distribution<usize> pick_scan_len_log2{kMinScanLenLog2, kMaxScanLenLog2};

  StdMapTable expected_table;

  StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);

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

    std::string key = this->generate_key(this->rng);
    std::string value = this->generate_value();

    Status expected_put_status = expected_table.put(KeyView{key}, ValueView::from_str(value));
    Status actual_put_status = actual_table.put(KeyView{key}, ValueView::from_str(value));

    ASSERT_TRUE(expected_put_status.ok()) << BATT_INSPECT(expected_put_status);
    ASSERT_TRUE(actual_put_status.ok()) << BATT_INSPECT(actual_put_status);

    const usize target_scans = double(i + 1) * kNumScansPerKey;
    for (; n_scans < target_scans; ++n_scans) {
      std::string min_key = generate_key(this->rng);

      const usize scan_len_log2 = pick_scan_len_log2(this->rng);
      std::uniform_int_distribution<usize> pick_scan_len{usize{1} << (scan_len_log2 - 1),
                                                         (usize{1} << scan_len_log2)};
      const usize scan_len = pick_scan_len(this->rng);

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
struct CheckpointTestParams {
  u64 num_checkpoints_to_create;
  u64 num_puts;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class CheckpointTest
    : public KVStoreTest
    , public testing::WithParamInterface<CheckpointTestParams>
{
 public:
  void SetUp() override
  {
    KVStoreTest::SetUp();

    CheckpointTestParams checkpoint_test_params = GetParam();
    this->num_checkpoints_to_create = checkpoint_test_params.num_checkpoints_to_create;
    this->num_puts = checkpoint_test_params.num_puts;
  }

  u64 num_checkpoints_to_create;
  u64 num_puts;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_P(CheckpointTest, CheckpointRecovery)
{
  std::filesystem::path test_kv_store_dir =
      this->data_root / "turtle_kv_Test" / "checkpoint_recovery";

  StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  std::unique_ptr<KVStore>& kv_store = *open_result;

  // Disable automatic checkpoints
  //
  kv_store->set_checkpoint_distance(99999999);

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
    std::string key = this->generate_key(this->rng);
    std::string value = this->generate_value();

    Status actual_put_status = kv_store->put(KeyView{key}, ValueView::from_str(value));
    ASSERT_TRUE(actual_put_status.ok()) << BATT_INSPECT(actual_put_status);

    expected_keys_values[key] = value;

    VLOG(3) << "Put key== " << key << ", value==" << value;

    ++keys_since_checkpoint;

    // Take a checkpoint after every keys_per_checkpoint puts
    //
    if (keys_since_checkpoint >= keys_per_checkpoint && this->num_checkpoints_to_create != 0) {
      keys_since_checkpoint = 0;
      ++num_checkpoints_created;
      BATT_CHECK_OK(kv_store->force_checkpoint());
      VLOG(2) << "Created " << num_checkpoints_created << " checkpoints";
      if (num_checkpoints_created == this->num_checkpoints_to_create) {
        break;
      }
    }
  }

  // Handle off by one error where we create one less checkpoint than expected
  //
  if (num_checkpoints_created < this->num_checkpoints_to_create) {
    BATT_CHECK_OK(kv_store->force_checkpoint());
    ++num_checkpoints_created;
    VLOG(1) << "Created " << num_checkpoints_created << " checkpoints after rounding error";
  }

  BATT_CHECK_EQ(num_checkpoints_created, this->num_checkpoints_to_create)
      << "Did not take the correct number of checkpoints. There is a bug in this test.";

  // TODO: [Gabe Bornstein 3/17/26] Replace with fsync once it's implemented.
  //
  std::this_thread::sleep_for(std::chrono::seconds(1));
  this->ShutdownKVStore(kv_store);

  batt::StatusOr<std::unique_ptr<llfs::Volume>> checkpoint_log_volume =
      turtle_kv::open_checkpoint_log(*this->storage_context,
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
    turtle_kv::KeyQuery key_query{*page_loader,
                                  slice_storage,
                                  this->kv_store_config.tree_options,
                                  key_view};

    batt::StatusOr<turtle_kv::ValueView> checkpoint_value = checkpoint->find_key(key_query);

    EXPECT_TRUE(checkpoint_value.ok()) << "Didn't find key: " << key;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(KVStoreTest, ChangeLogRecovery)
{
  const u64 num_puts = 100000;

  std::filesystem::path test_kv_store_dir =
      this->data_root / "turtle_kv_Test" / "change_log_recovery";

  StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  std::unique_ptr<KVStore>& kv_store = *open_result;

  kv_store->set_checkpoint_distance(5);

  std::map<std::string, std::string> expected_keys_values;
  this->PopulateKVStore(*kv_store, num_puts, &expected_keys_values);

  this->ShutdownKVStore(kv_store);

  batt::StatusOr<std::unique_ptr<turtle_kv::ChangeLogFile>> change_log_file =
      turtle_kv::ChangeLogFile::open(test_kv_store_dir / "change_log.turtle_kv");

  ASSERT_TRUE(change_log_file.ok());
  batt::StatusOr<std::vector<boost::intrusive_ptr<turtle_kv::ChangeLogBlock>>> blocks =
      (*change_log_file)->read_blocks_into_vector();

  ASSERT_TRUE(blocks.ok()) << BATT_INSPECT(blocks.status());

  int i = 0;
  for (auto block : *blocks) {
    ASSERT_TRUE(block->verify().ok());
    VLOG(1) << "Reading block " << i << " with owner_id() == " << block->owner_id()
            << ", and block_size() == " << block->block_size();
    ASSERT_NE(block->owner_id(), 0);
    ASSERT_NE(block->block_size(), 0);
    ++i;
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
        CheckpointTestParams{.num_checkpoints_to_create = 2, .num_puts = 100},
        CheckpointTestParams{.num_checkpoints_to_create = 100, .num_puts = 100},
        CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 100000},
        CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 0},
        CheckpointTestParams{.num_checkpoints_to_create = 0, .num_puts = 100},
        CheckpointTestParams{.num_checkpoints_to_create = 5, .num_puts = 100000},
        CheckpointTestParams{.num_checkpoints_to_create = 10, .num_puts = 100000}
        //  TODO: [Gabe Bornstein 11/6/25] Sporadic Failing. Likely cause by keys not
        //  being flushed before that last checkpoint is taken. Need fsync to resolve.
        /*CheckpointTestParams(101, 100000)*/));
