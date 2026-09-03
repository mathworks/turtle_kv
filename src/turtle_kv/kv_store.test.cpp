//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/kv_store.hpp>
//
#include <turtle_kv/kv_store.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_root.test.hpp"

#include <turtle_kv/checkpoint_log.hpp>
#include <turtle_kv/core/table.hpp>
#include <turtle_kv/core/testing/generate.hpp>
#include <turtle_kv/packed_checkpoint.hpp>
#include <turtle_kv/scan_metrics.hpp>
#include <turtle_kv/testing/workload.test.hpp>

#include <batteries/do_nothing.hpp>
#include <batteries/segv.hpp>

#include <barrier>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace {

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using llfs::PageSize;

using turtle_kv::EditOffset;
using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::None;
using turtle_kv::ObjectThreadStorage;
using turtle_kv::OkStatus;
using turtle_kv::Optional;
using turtle_kv::RemoveExisting;
using turtle_kv::Slice;
using turtle_kv::Snapshot;
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

  void PopulateKVStore(KVStore& kv_store,
                       u64 num_puts,
                       std::map<std::string, std::string>* out_data = nullptr,
                       double delete_proportion = 0.0,
                       std::set<std::string>* out_deleted = nullptr,
                       Optional<KVStore::WriteOptions> write_options = None)
  {
    for (u64 i = 0; i < num_puts; ++i) {
      std::string key = this->generate_key(this->rng);
      std::string value = this->generate_value();

      if (write_options) {
        StatusOr<EditOffset> result =
            kv_store.put(KeyView{key}, ValueView::from_str(value), *write_options);
        ASSERT_TRUE(result.ok()) << BATT_INSPECT(result.status());
      } else {
        Status put_status = kv_store.put(KeyView{key}, ValueView::from_str(value));
        ASSERT_TRUE(put_status.ok()) << BATT_INSPECT(put_status);
      }

      if (out_data) {
        (*out_data)[key] = value;
      }
      VLOG(3) << "Put key==" << key << ", value==" << value;
    }

    if (delete_proportion > 0.0 && out_data) {
      const u64 num_to_delete = static_cast<u64>(num_puts * delete_proportion);
      u64 deleted = 0;

      // TODO [tastolfi 2026-06-16] Add an option to pick keys at random rather than in-order.
      //
      for (const auto& [key, value] : *out_data) {
        if (deleted >= num_to_delete) {
          break;
        }
        if (write_options) {
          StatusOr<EditOffset> result = kv_store.remove(KeyView{key}, *write_options);
          ASSERT_TRUE(result.ok()) << BATT_INSPECT(result.status());
        } else {
          Status result = kv_store.remove(KeyView{key});
          ASSERT_TRUE(result.ok()) << BATT_INSPECT(result);
        }
        if (out_deleted) {
          out_deleted->insert(key);
        }
        ++deleted;
      }
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
  constexpr bool kQuiet = true;

  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_kv_store_dir = *root / "turtle_kv_Test" / "kv_create_and_open";

  std::thread test_thread{[&] {
    BATT_CHECK_OK(batt::pin_thread_to_cpu(0));

    for (bool size_tiered : {false, true}) {
      KVStore::Config kv_store_config = KVStore::Config::with_default_values();

      kv_store_config.initial_capacity_bytes = 512 * kMiB;
      kv_store_config.change_log_size_bytes = 64 * kMiB * 100;

      TreeOptions& tree_options = kv_store_config.tree_options;

      tree_options.set_node_size(4 * kKiB);
      tree_options.set_leaf_size(1 * kMiB);
      tree_options.set_key_size_hint(24);
      tree_options.set_value_size_hint(10);
      if (!size_tiered) {
        tree_options.set_buffer_level_trim(3);
      }
      tree_options.set_size_tiered(size_tiered);

      if constexpr (!kQuiet) {
        LOG(INFO) << BATT_INSPECT(tree_options.filter_bits_per_key())
                  << BATT_INSPECT(tree_options.filter_page_size());
      }

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
            if constexpr (!kQuiet) {
              LOG(INFO) << "Skipping workload-e (scans) for size-tiered config";
            }
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

            ASSERT_TRUE(create_status.ok())
                << BATT_INSPECT(create_status) << BATT_INSPECT(test_kv_store_dir);
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

          if constexpr (!kQuiet) {
            LOG(INFO) << "--";
            LOG(INFO) << workload_file;
            LOG(INFO) << BATT_INSPECT(op_count)
                      << BATT_INSPECT(kv_store.metrics().checkpoint_count);
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
              double rate = (time_points[i].op_count - time_points[i - 1].op_count) /
                            std::max(1e-10, elapsed);

              LOG(INFO) << BATT_INSPECT(chi) << " | " << time_points[i].label << ": " << rate
                        << " ops/sec";
            }
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
  EditOffset last_checkpoint_bound{0};
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
      StatusOr<EditOffset> checkpoint_bound = kv_store->force_checkpoint();
      BATT_CHECK_OK(checkpoint_bound);
      last_checkpoint_bound = *checkpoint_bound;
      VLOG(2) << "Created " << num_checkpoints_created << " checkpoints";
      if (num_checkpoints_created == this->num_checkpoints_to_create) {
        break;
      }
    }
  }

  // Handle off by one error where we create one less checkpoint than expected
  //
  if (num_checkpoints_created < this->num_checkpoints_to_create) {
    StatusOr<EditOffset> checkpoint_bound = kv_store->force_checkpoint();
    BATT_CHECK_OK(checkpoint_bound);
    last_checkpoint_bound = *checkpoint_bound;
    ++num_checkpoints_created;
    VLOG(1) << "Created " << num_checkpoints_created << " checkpoints after rounding error";
  }

  BATT_CHECK_EQ(num_checkpoints_created, this->num_checkpoints_to_create)
      << "Did not take the correct number of checkpoints. There is a bug in this test.";

  BATT_CHECK_OK(kv_store->wait_for_checkpoint(last_checkpoint_bound));
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
  if (checkpoint->is_empty()) {
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
    EXPECT_EQ(checkpoint_value->as_str(), actual_value);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class KVStoreRecoveryTest
    : public KVStoreTest
    , public testing::WithParamInterface<u64>
{
 public:
  void SetUp() override
  {
    KVStoreTest::SetUp();
    this->num_puts = GetParam();
  }

  u64 num_puts;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_P(KVStoreRecoveryTest, KVStoreRecovery)
{
  std::filesystem::path test_kv_store_dir = this->data_root / "turtle_kv_Test" / "kvstore_recovery";
  std::map<std::string, std::string> expected_keys_values;

  {
    StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
    ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

    std::unique_ptr<KVStore>& kv_store = *open_result;

    kv_store->set_checkpoint_distance(1);

    this->PopulateKVStore(*kv_store, this->num_puts, &expected_keys_values);

    BATT_CHECK_OK(kv_store->sync());
    this->ShutdownKVStore(kv_store);
  }

  {
    StatusOr<std::unique_ptr<KVStore>> recovered_kv_store =
        turtle_kv::KVStore::open(test_kv_store_dir,
                                 this->kv_store_config.tree_options,
                                 this->runtime_options);

    ASSERT_TRUE(recovered_kv_store.ok()) << BATT_INSPECT(recovered_kv_store.status());

    for (const auto& [key, expected_value] : expected_keys_values) {
      turtle_kv::KeyView key_view{key};
      batt::StatusOr<turtle_kv::ValueView> actual_value = (*recovered_kv_store)->get(key_view);

      EXPECT_TRUE(actual_value.ok()) << "Didn't find key: " << key;
      EXPECT_EQ(actual_value->as_str(), expected_value)
          << "Didn't find the correct value for key: " << key;
    }
  }
}

// TODO: [Gabe Bornstein 3/18/26] Add some test points where we test recovery with updates and
// deletes, not just inserts.
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CheckpointReadOldKeysParams {
  u64 num_keys;
  u64 num_checkpoints;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class CheckpointReadOldKeysTest
    : public KVStoreTest
    , public testing::WithParamInterface<CheckpointReadOldKeysParams>
{
 public:
  void SetUp() override
  {
    KVStoreTest::SetUp();

    CheckpointReadOldKeysParams params = GetParam();
    this->num_keys = params.num_keys;
    this->num_checkpoints = params.num_checkpoints;
  }

  u64 num_keys;
  u64 num_checkpoints;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_P(CheckpointReadOldKeysTest, CheckpointReadOldKeys)
{
  ASSERT_GT(this->num_checkpoints, u64{0});

  std::filesystem::path test_kv_store_dir =
      this->data_root / "turtle_kv_Test" / "checkpoint_read_old_keys";

  StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  std::unique_ptr<KVStore>& kv_store = *open_result;

  kv_store->set_checkpoint_distance(99999999);

  auto make_key = [](i64 i) -> std::string {
    return batt::to_string(i);
  };

  auto make_value = [](i64 i) -> std::string {
    return batt::to_string(i);
  };

  const u64 keys_per_checkpoint = this->num_keys / this->num_checkpoints;

  std::vector<EditOffset> checkpoint_offsets;

  for (u64 cp = 0; cp < this->num_checkpoints; ++cp) {
    const i64 batch_start = cp * keys_per_checkpoint;
    const i64 batch_end = (cp == this->num_checkpoints - 1)
                              ? static_cast<i64>(this->num_keys)
                              : static_cast<i64>((cp + 1) * keys_per_checkpoint);

    for (i64 i = batch_start; i < batch_end; ++i) {
      std::string key = make_key(i);
      std::string value = make_value(i);
      Status put_status = kv_store->put(KeyView{key}, ValueView::from_str(value));
      ASSERT_TRUE(put_status.ok()) << BATT_INSPECT(put_status);
    }

    StatusOr<EditOffset> checkpoint_bound = kv_store->force_checkpoint();
    ASSERT_TRUE(checkpoint_bound.ok()) << BATT_INSPECT(checkpoint_bound.status());
    ASSERT_TRUE(kv_store->wait_for_checkpoint(*checkpoint_bound).ok());

    checkpoint_offsets.push_back(*checkpoint_bound);
  }

  // Checkpoints older than MAX_ACTIVE_CHECKPOINTS get trimmed; verify that querying them fails.
  //
  const u64 num_expired = (this->num_checkpoints > turtle_kv::MAX_ACTIVE_CHECKPOINTS)
                              ? this->num_checkpoints - turtle_kv::MAX_ACTIVE_CHECKPOINTS
                              : 0;

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  for (u64 cp = 0; cp < num_expired; ++cp) {
    StatusOr<Snapshot> snapshot = kv_store->get_snapshot(checkpoint_offsets[cp]);
    EXPECT_FALSE(snapshot.ok()) << "Expired checkpoint " << cp << " should not be queryable";
  }

  // Verify each living checkpoint can see exactly the keys written up to and including its batch.
  //
  for (u64 cp = num_expired; cp < this->num_checkpoints; ++cp) {
    StatusOr<Snapshot> snapshot = kv_store->get_snapshot(checkpoint_offsets[cp]);
    ASSERT_TRUE(snapshot.ok()) << "Failed to get snapshot for checkpoint " << cp;

    const i64 visible_end = (cp == this->num_checkpoints - 1)
                                ? static_cast<i64>(this->num_keys)
                                : static_cast<i64>((cp + 1) * keys_per_checkpoint);

    // Keys [0, visible_end) should be visible.
    //
    for (i64 i = 0; i < visible_end; ++i) {
      std::string key = make_key(i);
      StatusOr<ValueView> result = snapshot->get(KeyView{key});
      ASSERT_TRUE(result.ok()) << "Checkpoint " << cp << " missing key: " << key;
      EXPECT_EQ(result->as_str(), make_value(i));
    }

    // Keys [visible_end, num_keys) should NOT be visible.
    //
    for (i64 i = visible_end; i < static_cast<i64>(this->num_keys); ++i) {
      std::string key = make_key(i);
      StatusOr<ValueView> result = snapshot->get(KeyView{key});
      EXPECT_FALSE(result.ok()) << "Checkpoint " << cp << " should NOT contain key: " << key;
    }
  }

  this->ShutdownKVStore(kv_store);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(KVStoreTest, SyncWriteOptions)
{
  std::filesystem::path test_kv_store_dir = this->data_root / "turtle_kv_Test" / "sync_write_opts";

  std::map<std::string, std::string> expected_keys_values;
  std::set<std::string> deleted_keys;

  {
    StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
    ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

    std::unique_ptr<KVStore>& kv_store = *open_result;

    KVStore::WriteOptions opts{.sync = true};
    this->PopulateKVStore(*kv_store, 50, &expected_keys_values, 0.25, &deleted_keys, opts);

    this->ShutdownKVStore(kv_store);
  }

  // Recover and verify state.
  //
  {
    StatusOr<std::unique_ptr<KVStore>> recovered_kv_store =
        turtle_kv::KVStore::open(test_kv_store_dir,
                                 this->kv_store_config.tree_options,
                                 this->runtime_options);

    ASSERT_TRUE(recovered_kv_store.ok()) << BATT_INSPECT(recovered_kv_store.status());

    for (const auto& [key, expected_value] : expected_keys_values) {
      StatusOr<ValueView> actual_value = (*recovered_kv_store)->get(KeyView{key});

      if (deleted_keys.count(key)) {
        ASSERT_EQ(actual_value.status(), batt::StatusCode::kNotFound);
      } else {
        ASSERT_TRUE(actual_value.ok()) << "Didn't find key after recovery: " << key;
        EXPECT_EQ(actual_value->as_str(), expected_value)
            << "Wrong value for key after recovery: " << key;
      }
    }

    (*recovered_kv_store)->halt();
    (*recovered_kv_store)->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(KVStoreTest, SyncExplicit)
{
  std::filesystem::path test_kv_store_dir =
      this->data_root / "turtle_kv_Test" / "sync_explicit_recovery";

  std::map<std::string, std::string> expected_keys_values;
  std::set<std::string> deleted_keys;

  {
    StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
    ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

    std::unique_ptr<KVStore>& kv_store = *open_result;

    this->PopulateKVStore(*kv_store, 200, &expected_keys_values, 0.25, &deleted_keys);

    BATT_CHECK_OK(kv_store->sync());

    this->ShutdownKVStore(kv_store);
  }

  // Recover and verify state.
  //
  {
    StatusOr<std::unique_ptr<KVStore>> recovered_kv_store =
        turtle_kv::KVStore::open(test_kv_store_dir,
                                 this->kv_store_config.tree_options,
                                 this->runtime_options);

    ASSERT_TRUE(recovered_kv_store.ok()) << BATT_INSPECT(recovered_kv_store.status());

    for (const auto& [key, expected_value] : expected_keys_values) {
      StatusOr<ValueView> actual_value = (*recovered_kv_store)->get(KeyView{key});

      if (deleted_keys.count(key)) {
        ASSERT_EQ(actual_value.status(), batt::StatusCode::kNotFound);
      } else {
        ASSERT_TRUE(actual_value.ok()) << "Didn't find key after recovery: " << key;
        EXPECT_EQ(actual_value->as_str(), expected_value)
            << "Wrong value for key after recovery: " << key;
      }
    }

    (*recovered_kv_store)->halt();
    (*recovered_kv_store)->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(KVStoreTest, SyncMultithreadedStress)
{
  std::filesystem::path test_kv_store_dir =
      this->data_root / "turtle_kv_Test" / "sync_multithread_stress";

  StatusOr<std::unique_ptr<KVStore>> open_result = this->CreateAndOpenKVStore(test_kv_store_dir);
  ASSERT_TRUE(open_result.ok()) << BATT_INSPECT(open_result.status());

  std::unique_ptr<KVStore>& kv_store = *open_result;

  const usize num_threads = std::thread::hardware_concurrency();
  const usize ops_per_thread = 5000;

  struct PerThreadState {
    std::unordered_map<std::string, std::string> live_keys;
    std::unordered_set<std::string> removed_keys;
    usize sync_ok_count = 0;
    usize sync_error_count = 0;
    usize put_ok_count = 0;
    usize put_error_count = 0;
    usize remove_ok_count = 0;
    usize remove_error_count = 0;
  };

  ObjectThreadStorage<PerThreadState>::ScopedSlot per_thread_state;

  std::vector<std::thread> threads;

  std::barrier<batt::DoNothing> workload_done{isize(num_threads + 1), batt::DoNothing{}};
  std::barrier<batt::DoNothing> ok_to_exit{isize(num_threads + 1), batt::DoNothing{}};

  for (usize t = 0; t < num_threads; ++t) {
    threads.emplace_back(
        [&kv_store, &per_thread_state, &workload_done, &ok_to_exit, this, thread_id = t]() {
          std::default_random_engine thread_rng{(usize)(42 + thread_id)};
          RandomStringGenerator gen_key{};

          PerThreadState& state = per_thread_state.get();

          for (usize i = 0; i < ops_per_thread; ++i) {
            std::string key = gen_key(thread_rng);
            std::string value = this->generate_value();

            // Alternate between sync put, non-sync put + explicit sync, non-sync put, and remove.
            //
            const usize op = i % 4;
            if (op == 0) {
              // Sync put.
              //
              KVStore::WriteOptions opts{.sync = true};
              StatusOr<EditOffset> result =
                  kv_store->put(KeyView{key}, ValueView::from_str(value), opts);

              if (result.ok()) {
                state.put_ok_count += 1;
                state.sync_ok_count += 1;
                state.live_keys[key] = value;
                state.removed_keys.erase(key);
              } else {
                state.put_error_count += 1;
                state.sync_error_count += 1;
              }

            } else if (op == 1) {
              // Non-sync put followed by explicit sync.
              //
              Status put_result = kv_store->put(KeyView{key}, ValueView::from_str(value));

              if (!put_result.ok()) {
                state.put_error_count += 1;
              } else {
                state.put_ok_count += 1;
                state.live_keys[key] = value;
                state.removed_keys.erase(key);

                Status sync_result = kv_store->sync();
                if (sync_result.ok()) {
                  state.sync_ok_count += 1;
                } else {
                  state.sync_error_count += 1;
                }
              }
            } else if (op == 2) {
              // Non-sync put (no sync at all).
              //
              Status put_result = kv_store->put(KeyView{key}, ValueView::from_str(value));

              if (put_result.ok()) {
                state.put_ok_count += 1;
                state.live_keys[key] = value;
                state.removed_keys.erase(key);
              } else {
                state.put_error_count += 1;
              }

            } else {
              // Remove a key that this thread previously inserted.
              //
              if (state.live_keys.empty()) {
                continue;
              }
              std::string remove_key = state.live_keys.begin()->first;

              KVStore::WriteOptions opts{.sync = true};
              StatusOr<EditOffset> result = kv_store->remove(KeyView{remove_key}, opts);
              if (result.ok()) {
                state.remove_ok_count += 1;
                state.sync_ok_count += 1;
                state.live_keys.erase(remove_key);
                state.removed_keys.insert(remove_key);
              } else {
                state.remove_error_count += 1;
                state.sync_error_count += 1;
              }
            }
          }

          // Signal to the main test thread that we are done.
          //
          workload_done.arrive_and_wait();

          // Wait for the test thread to finish inspecting per-thread state before exiting.
          //
          ok_to_exit.arrive_and_wait();
        });
  }

  workload_done.arrive_and_wait();

  // Final sync to ensure everything is flushed.
  //
  Status final_sync = kv_store->sync();
  ASSERT_TRUE(final_sync.ok()) << BATT_INSPECT(final_sync);

  // Verify all live keys are readable with correct values, and removed keys are gone.
  //
  usize visit_count = 0;
  per_thread_state.visit_each([&](PerThreadState& state) -> bool {
    ++visit_count;

    // Each time through the loop does exactly one put or remove.
    //
    EXPECT_EQ(state.put_ok_count + state.remove_ok_count, ops_per_thread);

    // Syncs happen on 3 of 4 ops; but removes don't always happen.
    //
    EXPECT_GE(state.sync_ok_count, ops_per_thread / 2);
    EXPECT_LE(state.sync_ok_count, ops_per_thread * 3 / 4);

    // No errors, please!
    //
    EXPECT_EQ(state.put_error_count, 0);
    EXPECT_EQ(state.sync_error_count, 0);
    EXPECT_EQ(state.remove_error_count, 0);

    for (const auto& [key, expected_value] : state.live_keys) {
      StatusOr<ValueView> actual_value = kv_store->get(KeyView{key});
      EXPECT_TRUE(actual_value.ok()) << "Missing key: " << key;
      if (actual_value.ok()) {
        EXPECT_EQ(actual_value->as_str(), expected_value) << "Wrong value for key: " << key;
      }
    }

    for (const std::string& key : state.removed_keys) {
      StatusOr<ValueView> actual_value = kv_store->get(KeyView{key});
      EXPECT_EQ(actual_value.status(), batt::StatusCode::kNotFound)
          << "Key should have been removed: " << key;
    }

    return false;
  });
  EXPECT_EQ(visit_count, num_threads);

  // Allow the threads to continue past the second barrier, then join all.
  //
  ok_to_exit.arrive_and_wait();
  for (auto& t : threads) {
    t.join();
  }

  this->ShutdownKVStore(kv_store);
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string format_checkpoint_recovery_test_name(
    const ::testing::TestParamInfo<CheckpointTestParams>& info)
{
  return batt::to_string("NumCheckpoints",
                         info.param.num_checkpoints_to_create,
                         "NumPuts",
                         info.param.num_puts);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// CheckpointTestParams == {num_puts, num_checkpoints_to_create}
//
INSTANTIATE_TEST_SUITE_P(
    RecoveringCheckpoints,
    CheckpointTest,
    testing::Values(CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 1},
                    CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 100},
                    CheckpointTestParams{.num_checkpoints_to_create = 2, .num_puts = 100},
                    CheckpointTestParams{.num_checkpoints_to_create = 100, .num_puts = 100},
                    CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 100000},
                    CheckpointTestParams{.num_checkpoints_to_create = 1, .num_puts = 0},
                    CheckpointTestParams{.num_checkpoints_to_create = 0, .num_puts = 100},
                    CheckpointTestParams{.num_checkpoints_to_create = 5, .num_puts = 100000},
                    CheckpointTestParams{.num_checkpoints_to_create = 10, .num_puts = 100000},
                    CheckpointTestParams{.num_checkpoints_to_create = 101, .num_puts = 100000}),
    format_checkpoint_recovery_test_name);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string format_kv_store_recovery_test_name(const ::testing::TestParamInfo<u64>& info)
{
  return batt::to_string("NumPuts", info.param);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
INSTANTIATE_TEST_SUITE_P(RecoveringKVStore,
                         KVStoreRecoveryTest,
                         testing::Values(u64{0}, u64{1}, u64{100}, u64{1000}, u64{100000}),
                         format_kv_store_recovery_test_name);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string format_checkpoint_read_old_keys_test_name(
    const ::testing::TestParamInfo<CheckpointReadOldKeysParams>& info)
{
  return batt::to_string("NumKeys",
                         info.param.num_keys,
                         "NumCheckpoints",
                         info.param.num_checkpoints);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
INSTANTIATE_TEST_SUITE_P(
    ReadingOldCheckpointKeys,
    CheckpointReadOldKeysTest,
    testing::Values(CheckpointReadOldKeysParams{.num_keys = 1000, .num_checkpoints = 1},
                    CheckpointReadOldKeysParams{.num_keys = 1000, .num_checkpoints = 2},
                    CheckpointReadOldKeysParams{.num_keys = 1000, .num_checkpoints = 5},
                    CheckpointReadOldKeysParams{.num_keys = 10000, .num_checkpoints = 2},
                    CheckpointReadOldKeysParams{.num_keys = 10000, .num_checkpoints = 8},
                    CheckpointReadOldKeysParams{.num_keys = 10000, .num_checkpoints = 10}),
    format_checkpoint_read_old_keys_test_name);
