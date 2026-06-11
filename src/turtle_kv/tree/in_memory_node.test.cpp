#include <turtle_kv/tree/in_memory_node.hpp>
//
#include <turtle_kv/tree/in_memory_node.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/kv_store_scanner.hpp>

#include <turtle_kv/tree/memory_storage.hpp>
#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/tree/subtree_table.hpp>
#include <turtle_kv/tree/the_key.hpp>

#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <llfs/testing/test_config.hpp>
//
#include <llfs/testing/scenario_runner.hpp>

#include <llfs/appendable_job.hpp>

#include <absl/container/btree_map.h>

#include <array>
#include <atomic>
#include <random>
#include <unordered_set>
#include <utility>

namespace {

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

template <bool kDecayToItems>
using ResultSet = turtle_kv::MergeCompactor::ResultSet<kDecayToItems>;

using turtle_kv::BatchUpdate;
using turtle_kv::BatchUpdateContext;
using turtle_kv::bit_count;
using turtle_kv::DecayToItem;
using turtle_kv::EditView;
using turtle_kv::global_max_key;
using turtle_kv::global_min_key;
using turtle_kv::InMemoryNode;
using turtle_kv::IsRoot;
using turtle_kv::ItemView;
using turtle_kv::KeyView;
using turtle_kv::KVStoreScanner;
using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::make_memory_page_cache;
using turtle_kv::NeedsMerge;
using turtle_kv::NeedsSplit;
using turtle_kv::None;
using turtle_kv::OkStatus;
using turtle_kv::Optional;
using turtle_kv::PageSliceStorage;
using turtle_kv::ParentNodeHeight;
using turtle_kv::PinningPageLoader;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::StdMapTable;
using turtle_kv::Subtree;
using turtle_kv::SubtreeTable;
using turtle_kv::Table;
using turtle_kv::THE_KEY;
using turtle_kv::TreeOptions;
using turtle_kv::TreeSerializeContext;
using turtle_kv::ValueView;
using turtle_kv::testing::RandomResultSetGenerator;
using turtle_kv::testing::RandomStringGenerator;

using llfs::get_key;

using batt::getenv_as;
using batt::StableStringStore;

constexpr usize kMinScanSize = 1;
constexpr usize kMaxScanSize = 100;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status update_table(Table& table, const ResultSet<false>& result_set)
{
  for (const EditView& edit : result_set.get()) {
    if (edit.value.is_delete()) {
      BATT_REQUIRE_OK(table.remove(edit.key));
    } else {
      BATT_REQUIRE_OK(table.put(edit.key, edit.value));
    }
  }

  return OkStatus();
}

template <typename Rng>
void verify_table_point_queries(Table& expected_table, Table& actual_table, Rng&& rng, u32 skip)
{
  u32 mask = ((u32{1} << skip) - 1);
  std::array<std::pair<KeyView, ValueView>, 256> buffer;

  bool first_time = true;
  KeyView min_key = global_min_key();
  for (;;) {
    StatusOr<usize> n_read = expected_table.scan(min_key, as_slice(buffer));
    ASSERT_TRUE(n_read.ok()) << BATT_INSPECT(n_read);

    Slice<std::pair<KeyView, ValueView>> read_items = as_slice(buffer.data(), *n_read);
    if (first_time) {
      first_time = false;
    } else {
      read_items.drop_front();
    }

    if (read_items.empty()) {
      break;
    }

    for (const auto& [key, value] : read_items) {
      if ((rng() & mask) == 0) {
        StatusOr<ValueView> actual_value = actual_table.get(key);
        ASSERT_TRUE(actual_value.ok()) << BATT_INSPECT(actual_value) << BATT_INSPECT_STR(key);
        EXPECT_EQ(*actual_value, value);
      }
      min_key = key;
    }
  }
}

void verify_range_scan(LatencyMetric* scan_latency,
                       Table& expected_table,
                       const Slice<std::pair<KeyView, ValueView>>& actual_read_items,
                       const KeyView& min_key,
                       usize scan_len)
{
  std::array<std::pair<KeyView, ValueView>, kMaxScanSize> buffer;
  Optional<LatencyTimer> timer;
  if (scan_latency) {
    timer.emplace(*scan_latency);
  }
  StatusOr<usize> n_read = expected_table.scan(min_key, as_slice(buffer.data(), scan_len));
  timer = None;
  ASSERT_TRUE(n_read.ok()) << BATT_INSPECT(n_read);
  ASSERT_EQ(*n_read, actual_read_items.size());
  EXPECT_LE(*n_read, scan_len);

  Slice<std::pair<KeyView, ValueView>> expected_read_items = as_slice(buffer.data(), *n_read);

  auto expected_item_iter = expected_read_items.begin();
  auto actual_item_iter = actual_read_items.begin();

  for (usize i = 0; i < actual_read_items.size(); ++i) {
    BATT_CHECK_NE(expected_item_iter, expected_read_items.end());
    BATT_CHECK_NE(actual_item_iter, actual_read_items.end());

    ASSERT_EQ(expected_item_iter->first, actual_item_iter->first)
        << BATT_INSPECT(i) << BATT_INSPECT_STR(min_key);
    ASSERT_EQ(expected_item_iter->second, actual_item_iter->second) << BATT_INSPECT(i);

    ++expected_item_iter;
    ++actual_item_iter;
  }
}

void perform_range_scan(Table& expected_table,
                        Subtree& tree,
                        const TreeOptions& tree_options,
                        PinningPageLoader& page_loader,
                        const KeyView& min_key,
                        usize scan_len,
                        LatencyMetric* scan_latency,
                        usize iteration)
{
  auto root_ptr = std::make_shared<Subtree>(tree.clone_serialized_or_panic());

  std::array<std::pair<KeyView, ValueView>, kMaxScanSize> scan_items_buffer;

  PageSliceStorage page_slice_storage;

  KVStoreScanner kv_scanner{
      page_loader,
      root_ptr->page_id_slot_or_panic(),
      BATT_OK_RESULT_OR_PANIC(root_ptr->get_height(page_loader,  //
                                                   llfs::PageCacheOvercommit::not_allowed())),
      min_key,
      tree_options.trie_index_sharded_view_size(),
      &page_slice_storage};

  usize n_read = 0;
  {
    LatencyTimer timer{*scan_latency};
    BATT_CHECK_OK(kv_scanner.start());
    for (auto& kv_pair : scan_items_buffer) {
      Optional<EditView> item = kv_scanner.next();
      if (!item) {
        break;
      }
      kv_pair.first = item->key;
      kv_pair.second = item->value;
      ++n_read;
      if (n_read == scan_len) {
        break;
      }
    }
  }

  ASSERT_NO_FATAL_FAILURE(verify_range_scan(nullptr,
                                            expected_table,
                                            as_slice(scan_items_buffer.data(), n_read),
                                            min_key,
                                            scan_len))
      << BATT_INSPECT(iteration) << BATT_INSPECT_STR(min_key) << BATT_INSPECT(scan_len);
}

struct SubtreeBatchUpdateScenario {
  static std::atomic<usize>& size_tiered_count()
  {
    static std::atomic<usize> count_{0};
    return count_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::RandomSeed seed;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SubtreeBatchUpdateScenario(llfs::RandomSeed seed_arg) noexcept : seed{seed_arg}
  {
  }

  void run();
};

struct BatchUpdateGenerator {
  StableStringStore strings;
  RandomResultSetGenerator result_set_generator;
  std::vector<KeyView> pending_deletes;
  std::vector<KeyView> already_deleted;
  usize delete_frequency;

  explicit BatchUpdateGenerator(usize delete_frequency_param,
                                const RandomResultSetGenerator& gen) noexcept
      : result_set_generator{gen}
      , delete_frequency{delete_frequency_param}
  {
  }

  template <typename Rng>
  ResultSet<false> next_batch(usize batch_i, Rng& rng, bool update_pending_deletes = false)
  {
    ResultSet<false> result_set =
        result_set_generator(DecayToItem<false>{}, rng, this->strings, this->pending_deletes);

    this->already_deleted = this->pending_deletes;

    if (update_pending_deletes) {
      if (!this->pending_deletes.empty()) {
        this->pending_deletes.clear();
      }

      if (batch_i % this->delete_frequency == 0) {
        BATT_CHECK(this->pending_deletes.empty());
        for (const EditView& edit : result_set.get()) {
          pending_deletes.emplace_back(edit.key);
        }
      }
    }

    return result_set;
  }

  void verify_deleted_point_queries(Table& expected_table, Table& actual_table)
  {
    for (const KeyView& key : this->already_deleted) {
      EXPECT_EQ(expected_table.get(key).status(), batt::StatusCode::kNotFound);
      EXPECT_EQ(actual_table.get(key).status(), batt::StatusCode::kNotFound);
    }
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(InMemoryNodeTest, DISABLED_Subtree)
{
  llfs::testing::ScenarioRunner runner;

  u32 n_seeds = getenv_as<u32>("TURTLE_TREE_TEST_N_SEEDS").value_or(64);
  usize n_threads = getenv_as<usize>("TURTLE_TREE_TEST_N_THREADS").value_or(0);

  if (n_threads != 0) {
    runner.n_threads(n_threads);
  }
  runner.n_seeds(n_seeds);

  if (n_seeds < 128) {
    runner.n_updates(0);
  } else {
    runner.n_updates(n_seeds / 64);
  }
  runner.run(batt::StaticType<SubtreeBatchUpdateScenario>{});

  LOG(INFO) << BATT_INSPECT(SubtreeBatchUpdateScenario::size_tiered_count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SubtreeBatchUpdateScenario::run()
{
  static const bool verbose_output = getenv_as<bool>("TURTLE_TREE_TEST_VERBOSE").value_or(false);
  static std::atomic<int> id{verbose_output ? 0 : 1};
  thread_local int my_id = id.fetch_add(1);

  BATT_DEBUG_INFO(BATT_INSPECT(this->seed));

  LatencyMetric scan_latency;

  std::default_random_engine rng{this->seed};

  std::uniform_int_distribution<int> pick_bool{0, 1};
  std::uniform_int_distribution<usize> pick_scan_len{1, 100};

  const usize max_i = getenv_as<usize>("TURTLE_TREE_TEST_BATCH_COUNT").value_or(225);
  const bool size_tiered =
      getenv_as<bool>("TURTLE_TREE_TEST_SIZE_TIERED").value_or(pick_bool(rng) != 0);
  const usize chi = 4;
  const usize key_size = 24;
  const usize value_size = 100;
  const usize key_overhead = 4;
  const usize value_overhead = 5;
  const usize packed_item_size = key_size + key_overhead + value_size + value_overhead;

  if (size_tiered) {
    size_tiered_count().fetch_add(1);
  }

  TreeOptions tree_options = TreeOptions::with_default_values()  //
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(24)
                                 .set_value_size_hint(100)
                                 .set_size_tiered(size_tiered);

  const usize items_per_leaf = tree_options.flush_size() / packed_item_size;

  if (my_id == 0) {
    std::cout << BATT_INSPECT(items_per_leaf) << BATT_INSPECT(tree_options.flush_size())
              << BATT_INSPECT(tree_options.max_item_size()) << std::endl;
  }

  std::shared_ptr<llfs::PageCache> page_cache =
      make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                             tree_options,
                             /*byte_capacity=*/1500 * kMiB);

  RandomResultSetGenerator result_set_generator;
  result_set_generator.set_key_size(24).set_value_size(100).set_size(items_per_leaf);
  BatchUpdateGenerator update_generator{/*delete_frequency=*/5, /*gen=*/result_set_generator};

  turtle_kv::OrderedMapTable<absl::btree_map<std::string_view, std::string_view>> expected_table;

  Subtree tree = Subtree::make_empty();

  ASSERT_TRUE(tree.is_serialized());

  SubtreeTable actual_table{*page_cache, tree_options, tree};

  if (my_id == 0) {
    std::cout << BATT_INSPECT(tree.dump()) << std::endl;
  }

  batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool();

  // batt::require_fail_global_default_log_level() = batt::LogLevel::kInfo;

  Optional<PinningPageLoader> page_loader{*page_cache};

  usize total_items = 0;

  turtle_kv::BatchUpdateMetrics metrics;

  for (usize i = 0; i < max_i; ++i) {
    BatchUpdate update{
        .context =
            BatchUpdateContext{
                .worker_pool = worker_pool,
                .page_loader = *page_loader,
                .cancel_token = batt::CancelToken{},
                .metrics = metrics,
                .overcommit = llfs::PageCacheOvercommit::not_allowed(),
            },
        .result_set = update_generator.next_batch(i, rng, /*update_pending_deletes=*/true),
        .edit_size_totals = None,
    };
    update.update_edit_size_totals();
    total_items += update.result_set.size();

    if (update.result_set.find_key(THE_KEY).ok()) {
      LOG(INFO) << BATT_INSPECT(i) << " contains THE KEY";
    }

    Status table_update_status = update_table(expected_table, update.result_set);
    ASSERT_TRUE(table_update_status.ok()) << BATT_INSPECT(table_update_status);

    StatusOr<i32> tree_height =
        tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
    ASSERT_TRUE(tree_height.ok()) << BATT_INSPECT(tree_height);

    Status status =  //
        tree.apply_batch_update(tree_options,
                                ParentNodeHeight{*tree_height + 1},
                                update,
                                /*key_upper_bound=*/global_max_key(),
                                IsRoot{true});

    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status) << BATT_INSPECT(this->seed) << BATT_INSPECT(i);
    ASSERT_FALSE(batt::is_case<NeedsSplit>(tree.get_viability()));

    if (my_id == 0) {
      std::cout << std::setw(4) << i << "/" << max_i << " (items=" << total_items
                << "):" << BATT_INSPECT(tree.dump()) << std::endl;
    }

    ASSERT_NO_FATAL_FAILURE(
        verify_table_point_queries(expected_table, actual_table, rng, batt::log2_ceil(i)))
        << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

    ASSERT_NO_FATAL_FAILURE(
        update_generator.verify_deleted_point_queries(expected_table, actual_table))
        << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

    if (((i + 1) % chi) == 0) {
      if (my_id == 0) {
        LOG(INFO) << "taking checkpoint...";
      }

      std::unique_ptr<llfs::PageCacheJob> page_job = page_cache->new_job();
      TreeSerializeContext context{
          tree_options,
          *page_job,
          worker_pool,
          llfs::PageCacheOvercommit::not_allowed(),
          turtle_kv::FilterPageWriteState::make_new(),
      };

      Status start_status = tree.start_serialize(context);
      ASSERT_TRUE(start_status.ok()) << BATT_INSPECT(start_status);

      Status build_status = context.build_all_pages();
      ASSERT_TRUE(build_status.ok()) << BATT_INSPECT(build_status);

      StatusOr<llfs::PageId> finish_status = tree.finish_serialize(context);
      ASSERT_TRUE(finish_status.ok()) << BATT_INSPECT(finish_status);

      if (my_id == 0) {
        LOG(INFO) << "checkpoint OK; verifying checkpoint...";
      }

      page_job->new_root(*finish_status);
      Status commit_status = llfs::unsafe_commit_job(std::move(page_job));
      ASSERT_TRUE(commit_status.ok()) << BATT_INSPECT(commit_status);

      ASSERT_NO_FATAL_FAILURE(
          verify_table_point_queries(expected_table, actual_table, rng, batt::log2_ceil(i)))
          << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

      ASSERT_NO_FATAL_FAILURE(
          update_generator.verify_deleted_point_queries(expected_table, actual_table))
          << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

      {
        auto root_ptr = std::make_shared<Subtree>(tree.clone_serialized_or_panic());

        const usize scan_len = pick_scan_len(rng);
        KeyView min_key = update.result_set.get_min_key();

        perform_range_scan(expected_table,
                           *root_ptr,
                           tree_options,
                           *page_loader,
                           min_key,
                           scan_len,
                           &scan_latency,
                           i);
      }

      if (my_id == 0) {
        LOG(INFO) << "checkpoint verified!";
      }

      // Release the pinned pages from the previous checkpoint.
      //
      page_loader.emplace(*page_cache);
    }
  }

  if (my_id == 1) {
    LOG(INFO) << BATT_INSPECT(scan_latency);
  }
}

TEST(InMemoryNodeTest, DISABLED_SubtreeDeletions)
{
  LatencyMetric scan_latency;

  const usize key_size = 24;
  const usize value_size = 100;
  const usize chi = 4;

  TreeOptions tree_options = TreeOptions::with_default_values()  //
                                 .set_leaf_size(32 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(key_size)
                                 .set_value_size_hint(value_size);

  usize items_per_leaf = tree_options.flush_size() / tree_options.expected_item_size();
  usize total_batches = 100;

  std::vector<KeyView> keys;
  keys.reserve(total_batches * items_per_leaf);

  std::string value_str = std::string(value_size, 'a');
  ValueView value = ValueView::from_str(value_str);

  std::default_random_engine rng{/*seed=*/3};
  RandomStringGenerator generate_key;
  StableStringStore store;
  std::unordered_set<KeyView> keys_set;
  while (keys.size() < total_batches * items_per_leaf) {
    KeyView key = generate_key(rng, store);
    if (keys_set.contains(key)) {
      continue;
    }
    keys_set.emplace(key);
    keys.emplace_back(key);
  }
  std::sort(keys.begin(), keys.end(), llfs::KeyOrder{});

  std::shared_ptr<llfs::PageCache> page_cache =
      make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                             tree_options,
                             /*byte_capacity=*/1500 * kMiB);

  Subtree tree = Subtree::make_empty();
  ASSERT_TRUE(tree.is_serialized());

  turtle_kv::OrderedMapTable<absl::btree_map<std::string_view, std::string_view>> expected_table;
  SubtreeTable actual_table{*page_cache, tree_options, tree};

  batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool();
  turtle_kv::BatchUpdateMetrics metrics;

  Optional<PinningPageLoader> page_loader{*page_cache};

  const auto create_insertion_batch = [&](usize batch_number) -> std::vector<EditView> {
    std::vector<EditView> current_batch;
    current_batch.reserve(items_per_leaf);
    for (usize j = 0; j < items_per_leaf; ++j) {
      current_batch.emplace_back(keys[(batch_number * items_per_leaf) + j], value);
    }

    return current_batch;
  };

  const auto create_deletion_batch = [&](usize batch_number) -> std::vector<EditView> {
    std::vector<EditView> current_batch;
    current_batch.reserve(items_per_leaf);

    for (usize i = 0; i < items_per_leaf; ++i) {
      usize key_i = batch_number + i * total_batches;
      if (key_i < keys.size()) {
        current_batch.emplace_back(keys[key_i], ValueView::deleted());
      }
    }

    BATT_CHECK_LE(current_batch.size(), items_per_leaf) << BATT_INSPECT(batch_number);

    return current_batch;
  };

  const auto apply_tree_updates = [&](auto batch_creation_func) {
    for (usize i = 0; i < total_batches; ++i) {
      std::vector<EditView> current_batch = batch_creation_func(i);

      ResultSet<false> result;
      result.append(std::move(current_batch));

      BatchUpdate update{
          .context =
              BatchUpdateContext{
                  .worker_pool = worker_pool,
                  .page_loader = *page_loader,
                  .cancel_token = batt::CancelToken{},
                  .metrics = metrics,
                  .overcommit = llfs::PageCacheOvercommit::not_allowed(),
              },
          .result_set = std::move(result),
          .edit_size_totals = None,
      };
      update.update_edit_size_totals();

      Status table_update_status = update_table(expected_table, update.result_set);
      ASSERT_TRUE(table_update_status.ok()) << BATT_INSPECT(table_update_status);

      StatusOr<i32> tree_height_before =
          tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
      ASSERT_TRUE(tree_height_before.ok()) << BATT_INSPECT(tree_height_before);

      Status status =  //
          tree.apply_batch_update(tree_options,
                                  ParentNodeHeight{*tree_height_before + 1},
                                  update,
                                  /*key_upper_bound=*/global_max_key(),
                                  IsRoot{true});

      ASSERT_TRUE(status.ok()) << BATT_INSPECT(status) << BATT_INSPECT(i);

      StatusOr<i32> tree_height_after =
          tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
      ASSERT_TRUE(tree_height_after.ok()) << BATT_INSPECT(tree_height_after);

      if (*tree_height_after == 0) {
        ASSERT_LT(*tree_height_after, *tree_height_before);
        ASSERT_TRUE(tree.is_serialized());
        break;
      } else {
        ASSERT_FALSE(tree.is_serialized());
      }

      ASSERT_FALSE(batt::is_case<NeedsSplit>(tree.get_viability()));

      ASSERT_NO_FATAL_FAILURE(
          verify_table_point_queries(expected_table, actual_table, rng, batt::log2_ceil(i)))
          << BATT_INSPECT(i);

      if (((i + 1) % chi) == 0) {
        std::unique_ptr<llfs::PageCacheJob> page_job = page_cache->new_job();
        TreeSerializeContext context{tree_options,
                                     *page_job,
                                     worker_pool,
                                     llfs::PageCacheOvercommit::not_allowed(),
                                     turtle_kv::FilterPageWriteState::make_new()};

        Status start_status = tree.start_serialize(context);
        ASSERT_TRUE(start_status.ok()) << BATT_INSPECT(start_status);

        Status build_status = context.build_all_pages();
        ASSERT_TRUE(build_status.ok()) << BATT_INSPECT(build_status);

        StatusOr<llfs::PageId> finish_status = tree.finish_serialize(context);
        ASSERT_TRUE(finish_status.ok()) << BATT_INSPECT(finish_status);

        page_job->new_root(*finish_status);
        Status commit_status = llfs::unsafe_commit_job(std::move(page_job));
        ASSERT_TRUE(commit_status.ok()) << BATT_INSPECT(commit_status);

        ASSERT_NO_FATAL_FAILURE(
            verify_table_point_queries(expected_table, actual_table, rng, batt::log2_ceil(i)))
            << BATT_INSPECT(i);

        {
          auto root_ptr = std::make_shared<Subtree>(tree.clone_serialized_or_panic());

          const usize scan_len = 20;
          KeyView min_key = update.result_set.get_min_key();

          perform_range_scan(expected_table,
                             *root_ptr,
                             tree_options,
                             *page_loader,
                             min_key,
                             scan_len,
                             &scan_latency,
                             i);
        }

        page_loader.emplace(*page_cache);
      }
    }
  };

  LOG(INFO) << "Inserting key/value pairs into tree...";
  apply_tree_updates(create_insertion_batch);

  LOG(INFO) << "Deleting key/value pairs from tree...";
  StatusOr<i32> tree_height =
      tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
  ASSERT_TRUE(tree_height.ok()) << BATT_INSPECT(tree_height);
  for (;;) {
    if (*tree_height > 0) {
      apply_tree_updates(create_deletion_batch);
    } else {
      break;
    }
    tree_height = tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
    ASSERT_TRUE(tree_height.ok()) << BATT_INSPECT(tree_height);
  }

  LOG(INFO) << BATT_INSPECT(InMemoryNode::metrics().merge_latency);

  LOG(INFO) << BATT_INSPECT(InMemoryNode::metrics().merge_then_split_count);

  LOG(INFO) << BATT_INSPECT(scan_latency);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// InMemoryNodeLevel::merge tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using turtle_kv::ActivePivotsSet128;
using turtle_kv::InMemoryNodeEmptyLevel;
using turtle_kv::InMemoryNodeHybridLevel;
using turtle_kv::InMemoryNodeLevel;
using turtle_kv::InMemoryNodeMergedLevel;
using turtle_kv::InMemoryNodeSegment;
using turtle_kv::InMemoryNodeSegmentedLevel;
using turtle_kv::KeyOrder;
using turtle_kv::MergeCompactor;
using turtle_kv::PiecewiseFilter;

using EmptyLevel = InMemoryNodeEmptyLevel;
using MergedLevel = InMemoryNodeMergedLevel;
using SegmentedLevel = InMemoryNodeSegmentedLevel;
using HybridLevel = InMemoryNodeHybridLevel;
using Segment = InMemoryNodeSegment;
using Level = InMemoryNodeLevel;

class InMemoryNodeLevelMergeTest : public ::testing::Test
{
 public:
  Segment make_segment(u64 page_id, const std::vector<i32>& active_pivot_indices)
  {
    Segment segment;
    segment.page_id_slot = llfs::PageIdSlot::from_page_id(llfs::PageId{page_id});
    for (i32 pivot_i : active_pivot_indices) {
      segment.active_pivots.set(pivot_i, true);
    }
    return segment;
  }

  SegmentedLevel make_segmented_level(const std::vector<Segment>& segments)
  {
    SegmentedLevel level;
    for (auto& seg : segments) {
      level.segments.emplace_back(std::move(seg));
    }
    return level;
  }

  MergedLevel make_merged_level(usize item_count, std::string_view key_prefix)
  {
    const usize value_size = 100;
    std::vector<EditView> items;
    items.reserve(item_count);

    RandomStringGenerator key_gen;
    key_gen.set_size(24);

    std::unordered_set<KeyView> seen;
    while (items.size() < item_count) {
      std::string key_str(key_prefix);
      key_str += key_gen(this->rng_);

      KeyView key = this->string_store_.store(key_str);
      if (seen.count(key)) {
        continue;
      }
      seen.emplace(key);

      char ch = 'a' + (items.size() % 26);
      std::string_view value = this->string_store_.store(std::string(value_size, ch));
      items.emplace_back(key, ValueView::from_str(value));
    }

    std::sort(items.begin(), items.end(), KeyOrder{});

    MergedLevel level;
    level.result_set.append(std::move(items));
    return level;
  }

  HybridLevel make_hybrid_level(const std::vector<HybridLevel::SubLevel>& sub_levels)
  {
    HybridLevel level;
    for (auto& sl : sub_levels) {
      level.sub_levels.emplace_back(std::move(sl));
    }
    return level;
  }

  u64 random_active_pivots(i32 max_pivot)
  {
    std::uniform_int_distribution<i32> dist(0, 1);
    u64 pivots = 0;
    for (i32 i = 0; i < max_pivot; ++i) {
      if (dist(this->rng_)) {
        pivots = turtle_kv::set_bit(pivots, i, true);
      }
    }
    if (pivots == 0) {
      // Make sure at leas one pivot in the bit set is active.
      //
      std::uniform_int_distribution<i32> fallback(0, max_pivot - 1);
      pivots = turtle_kv::set_bit(pivots, fallback(this->rng_), true);
    }
    return pivots;
  }

  static constexpr u32 kFilterItemCount = 1000;

  void apply_random_filter_drops(Segment& seg, usize max_drops)
  {
    std::uniform_int_distribution<usize> count_dist(0, max_drops);
    usize n_drops = count_dist(this->rng_);

    for (usize i = 0; i < n_drops; ++i) {
      std::uniform_int_distribution<u32> start_dist(0, kFilterItemCount - 1);
      u32 start = start_dist(this->rng_);

      std::uniform_int_distribution<u32> end_dist(start, kFilterItemCount);
      u32 end = end_dist(this->rng_);

      seg.filter.drop_index_range({start, end});
    }
  }

  Segment make_random_segment(u64 page_id, i32 max_pivot)
  {
    u64 pivots = this->random_active_pivots(max_pivot);
    Segment seg = this->make_segment(page_id, {});
    for (i32 p = 0; p < max_pivot; ++p) {
      seg.active_pivots.set(p, turtle_kv::get_bit(pivots, p));
    }
    this->apply_random_filter_drops(seg, /*max_drops=*/3);
    return seg;
  }

  SegmentedLevel make_random_segmented_level(u64 base_page_id,
                                             i32 max_pivot,
                                             usize min_count = 1,
                                             usize max_count = 32)
  {
    std::uniform_int_distribution<usize> count_dist(min_count, max_count);
    usize n_segments = count_dist(this->rng_);

    std::vector<Segment> segments;
    for (usize i = 0; i < n_segments; ++i) {
      segments.push_back(this->make_random_segment(base_page_id + i, max_pivot));
    }
    return this->make_segmented_level(std::move(segments));
  }

  void verify_pivots_shifted(const std::vector<Segment>& originals,
                             const SegmentedLevel& result,
                             usize node_pivot_count,
                             usize offset = 0,
                             bool check_original_inactive = false)
  {
    for (usize i = 0; i < originals.size(); ++i) {
      const auto& original = originals[i];
      const auto& seg = result.get_segment(offset + i);
      EXPECT_EQ(seg.get_leaf_page_id().page_id, original.get_leaf_page_id().page_id);

      for (i32 p = 0; p < 64; ++p) {
        if (original.is_pivot_active(p)) {
          i32 shifted = p + static_cast<i32>(node_pivot_count);
          EXPECT_TRUE(seg.is_pivot_active(shifted))
              << "Segment " << i << " pivot " << p << " should be shifted to " << shifted;
          if (check_original_inactive) {
            EXPECT_FALSE(seg.is_pivot_active(p))
                << "Segment " << i << " original pivot " << p << " should not be active";
          }
        }
      }
    }
  }

  void verify_pivots_unchanged(const std::vector<Segment>& originals,
                               const SegmentedLevel& result,
                               usize offset = 0)
  {
    for (usize i = 0; i < originals.size(); ++i) {
      const auto& original = originals[i];
      const auto& seg = result.get_segment(offset + i);
      EXPECT_EQ(seg.get_leaf_page_id().page_id, original.get_leaf_page_id().page_id);

      for (i32 p = 0; p < 64; ++p) {
        if (original.is_pivot_active(p)) {
          EXPECT_TRUE(seg.is_pivot_active(p))
              << "Segment " << i << " pivot " << p << " should be active";
        }
      }
    }
  }

  void verify_segmented_merge_no_duplicates(const std::vector<Segment>& left_originals,
                                            const std::vector<Segment>& right_originals,
                                            const SegmentedLevel& result,
                                            usize node_pivot_count)
  {
    ASSERT_EQ(result.segment_count(), left_originals.size() + right_originals.size());
    this->verify_pivots_unchanged(left_originals, result);
    this->verify_pivots_shifted(right_originals,
                                result,
                                node_pivot_count,
                                /*offset=*/left_originals.size(),
                                /*check_original_inactive=*/true);
  }

  void verify_pivots_deduplication(const Segment& left_original,
                                   const Segment& right_original,
                                   const Segment& deduped,
                                   usize node_pivot_count)
  {
    for (i32 p = 0; p < 64; ++p) {
      if (left_original.is_pivot_active(p)) {
        EXPECT_TRUE(deduped.is_pivot_active(p))
            << "Left pivot " << p << " should remain active after deduplication";
      }
      if (right_original.is_pivot_active(p)) {
        i32 shifted = p + static_cast<i32>(node_pivot_count);
        EXPECT_TRUE(deduped.is_pivot_active(shifted))
            << "Right pivot " << p << " should be shifted to " << shifted << " after deduplication";
      }
    }
  }

  void verify_filter_union(const Segment& left_original,
                           const Segment& right_original,
                           const Segment& result_seg,
                           u32 check_range = 100)
  {
    for (u32 idx = 0; idx < check_range; ++idx) {
      bool expected =
          left_original.filter.live_at_index(idx) || right_original.filter.live_at_index(idx);
      EXPECT_EQ(result_seg.filter.live_at_index(idx), expected)
          << "Filter mismatch at index " << idx;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::default_random_engine rng_{330};
  StableStringStore string_store_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// EmptyLevel::merge tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeLevelMergeTest, EmptyEmpty)
{
  EmptyLevel left;
  Level right = EmptyLevel{};

  Level result = std::move(left).merge(std::move(right), /*node_pivot_count=*/4);

  EXPECT_TRUE(batt::is_case<EmptyLevel>(result));
}

TEST_F(InMemoryNodeLevelMergeTest, EmptyMerged)
{
  EmptyLevel left;
  MergedLevel right = this->make_merged_level(64, "b_");
  const usize expected_size = right.result_set.size();

  Level result = std::move(left).merge(Level{std::move(right)}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<MergedLevel>(result));
  auto& merged = std::get<MergedLevel>(result);
  EXPECT_EQ(merged.result_set.size(), expected_size);
}

TEST_F(InMemoryNodeLevelMergeTest, EmptySegmented)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    SegmentedLevel right = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> right_originals;
    for (usize i = 0; i < right.segment_count(); ++i) {
      right_originals.push_back(right.get_segment(i));
    }

    Level result = std::move(EmptyLevel{}).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<SegmentedLevel>(result));
    auto& segmented = std::get<SegmentedLevel>(result);

    ASSERT_EQ(segmented.segment_count(), right_originals.size());
    this->verify_pivots_shifted(right_originals,
                                segmented,
                                node_pivot_count,
                                /*offset=*/0,
                                /*check_original_inactive=*/true);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, EmptyHybrid)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    SegmentedLevel sub_segmented = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> sub_originals;
    for (usize i = 0; i < sub_segmented.segment_count(); ++i) {
      sub_originals.push_back(sub_segmented.get_segment(i));
    }

    HybridLevel right = this->make_hybrid_level({std::move(sub_segmented)});

    Level result = std::move(EmptyLevel{}).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);
    ASSERT_EQ(hybrid.sub_levels.size(), 1u);
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[0]));

    auto& sub = std::get<SegmentedLevel>(hybrid.sub_levels[0]);
    ASSERT_EQ(sub.segment_count(), sub_originals.size());
    this->verify_pivots_shifted(sub_originals,
                                sub,
                                node_pivot_count,
                                /*offset=*/0,
                                /*check_original_inactive=*/true);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// MergedLevel::merge tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeLevelMergeTest, MergedEmpty)
{
  MergedLevel left = this->make_merged_level(64, "a_");
  const usize expected_size = left.result_set.size();

  Level result = std::move(left).merge(Level{EmptyLevel{}}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<MergedLevel>(result));
  auto& merged = std::get<MergedLevel>(result);
  EXPECT_EQ(merged.result_set.size(), expected_size);
}

TEST_F(InMemoryNodeLevelMergeTest, MergedMerged)
{
  MergedLevel left = this->make_merged_level(128, "a_");
  MergedLevel right = this->make_merged_level(64, "b_");

  auto left_items = left.result_set.get();
  auto right_items = right.result_set.get();
  std::vector<std::string> all_keys;
  for (const auto& item : left_items) {
    all_keys.emplace_back(get_key(item));
  }
  for (const auto& item : right_items) {
    all_keys.emplace_back(get_key(item));
  }

  Level result = std::move(left).merge(Level{std::move(right)}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<MergedLevel>(result));
  auto& merged = std::get<MergedLevel>(result);
  EXPECT_EQ(merged.result_set.size(), all_keys.size());

  auto result_items = merged.result_set.get();
  std::vector<std::string> result_keys;
  for (const auto& item : result_items) {
    result_keys.emplace_back(get_key(item));
  }

  EXPECT_EQ(result_keys, all_keys);
}

TEST_F(InMemoryNodeLevelMergeTest, MergedSegmented)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    MergedLevel left = this->make_merged_level(32, "a_");
    usize left_size = left.result_set.size();

    SegmentedLevel right = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> right_originals;
    for (usize i = 0; i < right.segment_count(); ++i) {
      right_originals.push_back(right.get_segment(i));
    }

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);

    ASSERT_EQ(hybrid.sub_levels.size(), 2u);
    ASSERT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[0]));
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[1]));

    auto& merged_sub = std::get<MergedLevel>(hybrid.sub_levels[0]);
    EXPECT_EQ(merged_sub.result_set.size(), left_size);

    auto& seg_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    ASSERT_EQ(seg_sub.segment_count(), right_originals.size());
    this->verify_pivots_shifted(right_originals, seg_sub, node_pivot_count);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, MergedHybrid)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    MergedLevel left = this->make_merged_level(32, "a_");
    usize left_size = left.result_set.size();

    SegmentedLevel sub_seg = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> sub_originals;
    for (usize i = 0; i < sub_seg.segment_count(); ++i) {
      sub_originals.push_back(sub_seg.get_segment(i));
    }

    HybridLevel right = this->make_hybrid_level({std::move(sub_seg)});

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);
    ASSERT_EQ(hybrid.sub_levels.size(), 2u);

    ASSERT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[0]));
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[1]));

    auto& merged_sub = std::get<MergedLevel>(hybrid.sub_levels[0]);
    EXPECT_EQ(merged_sub.result_set.size(), left_size);

    auto& seg_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    ASSERT_EQ(seg_sub.segment_count(), sub_originals.size());
    this->verify_pivots_shifted(sub_originals, seg_sub, node_pivot_count);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// SegmentedLevel::merge tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeLevelMergeTest, SegmentedEmpty)
{
  SegmentedLevel left = this->make_segmented_level({
      this->make_segment(500, {0, 1}),
      this->make_segment(501, {1, 2}),
  });

  Level result = std::move(left).merge(Level{EmptyLevel{}}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<SegmentedLevel>(result));
  auto& segmented = std::get<SegmentedLevel>(result);
  EXPECT_EQ(segmented.segment_count(), 2u);
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(0));
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(1));
}

TEST_F(InMemoryNodeLevelMergeTest, SegmentedMerged)
{
  SegmentedLevel left = this->make_segmented_level({this->make_segment(600, {0})});
  MergedLevel right = this->make_merged_level(32, "b_");

  Level result = std::move(left).merge(Level{std::move(right)}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<HybridLevel>(result));
  auto& hybrid = std::get<HybridLevel>(result);
  EXPECT_EQ(hybrid.sub_levels.size(), 2u);
  EXPECT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[0]));
  EXPECT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[1]));
}

TEST_F(InMemoryNodeLevelMergeTest, SegmentedSegmented_NoDuplicates)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    SegmentedLevel left = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    SegmentedLevel right = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100 + 50,
        max_pivot);

    std::vector<Segment> left_originals;
    for (usize i = 0; i < left.segment_count(); ++i) {
      left_originals.push_back(left.get_segment(i));
    }
    std::vector<Segment> right_originals;
    for (usize i = 0; i < right.segment_count(); ++i) {
      right_originals.push_back(right.get_segment(i));
    }

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<SegmentedLevel>(result));
    auto& segmented = std::get<SegmentedLevel>(result);

    this->verify_segmented_merge_no_duplicates(left_originals,
                                               right_originals,
                                               segmented,
                                               node_pivot_count);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, SegmentedSegmented_WithDuplicate)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    constexpr u64 kSharedPageId = 99999;

    Segment left_shared = this->make_random_segment(kSharedPageId, max_pivot);
    Segment right_shared = this->make_random_segment(kSharedPageId, max_pivot);

    Segment left_original = left_shared;
    Segment right_original = right_shared;

    SegmentedLevel left = this->make_segmented_level({std::move(left_shared)});
    SegmentedLevel right = this->make_segmented_level({std::move(right_shared)});

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<SegmentedLevel>(result));
    auto& segmented = std::get<SegmentedLevel>(result);

    ASSERT_EQ(segmented.segment_count(), 1u);

    auto& deduped = segmented.get_segment(0);
    EXPECT_EQ(deduped.get_leaf_page_id().page_id, llfs::PageId{kSharedPageId});
    this->verify_pivots_deduplication(left_original, right_original, deduped, node_pivot_count);
    this->verify_filter_union(left_original,
                              right_original,
                              deduped,
                              /*check_range=*/kFilterItemCount);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, SegmentedHybrid)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    SegmentedLevel left = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> left_originals;
    for (usize i = 0; i < left.segment_count(); ++i) {
      left_originals.push_back(left.get_segment(i));
    }

    SegmentedLevel sub_seg = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100 + 50,
        max_pivot);

    std::vector<Segment> right_sub_originals;
    for (usize i = 0; i < sub_seg.segment_count(); ++i) {
      right_sub_originals.push_back(sub_seg.get_segment(i));
    }

    HybridLevel right = this->make_hybrid_level({std::move(sub_seg)});

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);
    ASSERT_EQ(hybrid.sub_levels.size(), 2u);
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[0]));
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[1]));

    auto& left_sub = std::get<SegmentedLevel>(hybrid.sub_levels[0]);
    this->verify_pivots_unchanged(left_originals, left_sub);

    auto& right_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    ASSERT_EQ(right_sub.segment_count(), right_sub_originals.size());
    this->verify_pivots_shifted(right_sub_originals, right_sub, node_pivot_count);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// HybridLevel::merge tests
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeLevelMergeTest, HybridEmpty)
{
  MergedLevel sub_merged = this->make_merged_level(32, "a_");
  const usize expected_size = sub_merged.result_set.size();
  HybridLevel left = this->make_hybrid_level({std::move(sub_merged)});

  Level result = std::move(left).merge(Level{EmptyLevel{}}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<HybridLevel>(result));
  auto& hybrid = std::get<HybridLevel>(result);
  EXPECT_EQ(hybrid.sub_levels.size(), 1u);
  auto& merged_sub = std::get<MergedLevel>(hybrid.sub_levels[0]);
  EXPECT_EQ(merged_sub.result_set.size(), expected_size);
}

TEST_F(InMemoryNodeLevelMergeTest, HybridMerged)
{
  SegmentedLevel sub_seg = this->make_segmented_level({this->make_segment(1000, {4})});
  HybridLevel left = this->make_hybrid_level({std::move(sub_seg)});

  MergedLevel right = this->make_merged_level(32, "b_");

  Level result = std::move(left).merge(Level{std::move(right)}, /*node_pivot_count=*/4);

  ASSERT_TRUE(batt::is_case<HybridLevel>(result));
  auto& hybrid = std::get<HybridLevel>(result);
  EXPECT_EQ(hybrid.sub_levels.size(), 2u);
  EXPECT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[0]));
  EXPECT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[1]));
}

TEST_F(InMemoryNodeLevelMergeTest, HybridSegmented)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    MergedLevel sub_merged = this->make_merged_level(16, "a_");
    HybridLevel left = this->make_hybrid_level({std::move(sub_merged)});

    SegmentedLevel right = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> right_originals;
    for (usize i = 0; i < right.segment_count(); ++i) {
      right_originals.push_back(right.get_segment(i));
    }

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);

    ASSERT_EQ(hybrid.sub_levels.size(), 2u);
    EXPECT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[0]));
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[1]));

    auto& seg_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    ASSERT_EQ(seg_sub.segment_count(), right_originals.size());
    this->verify_pivots_shifted(right_originals, seg_sub, node_pivot_count);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, HybridHybrid_NoDuplicate)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    MergedLevel left_sub = this->make_merged_level(16, "a_");
    usize left_sub_size = left_sub.result_set.size();
    HybridLevel left = this->make_hybrid_level({std::move(left_sub)});

    SegmentedLevel right_sub = this->make_random_segmented_level(
        /*base_page_id=*/iter * 100,
        max_pivot);

    std::vector<Segment> right_sub_originals;
    for (usize i = 0; i < right_sub.segment_count(); ++i) {
      right_sub_originals.push_back(right_sub.get_segment(i));
    }

    HybridLevel right = this->make_hybrid_level({std::move(right_sub)});

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);
    ASSERT_EQ(hybrid.sub_levels.size(), 2u);
    ASSERT_TRUE(batt::is_case<MergedLevel>(hybrid.sub_levels[0]));
    ASSERT_TRUE(batt::is_case<SegmentedLevel>(hybrid.sub_levels[1]));

    auto& merged_sub = std::get<MergedLevel>(hybrid.sub_levels[0]);
    EXPECT_EQ(merged_sub.result_set.size(), left_sub_size);

    auto& seg_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    ASSERT_EQ(seg_sub.segment_count(), right_sub_originals.size());
    this->verify_pivots_shifted(right_sub_originals, seg_sub, node_pivot_count);
  }
}

TEST_F(InMemoryNodeLevelMergeTest, HybridHybrid_WithDuplicate)
{
  constexpr usize kIterations = 100;

  for (usize iter = 0; iter < kIterations; ++iter) {
    this->rng_ = std::default_random_engine{iter};

    std::uniform_int_distribution<usize> pivot_count_dist(4, 64);
    usize node_pivot_count = pivot_count_dist(this->rng_);
    i32 max_pivot = static_cast<i32>(node_pivot_count);

    constexpr u64 kSharedPageId = 1400;

    Segment left_shared = this->make_random_segment(kSharedPageId, max_pivot);
    Segment right_shared = this->make_random_segment(kSharedPageId, max_pivot);

    Segment left_original = left_shared;
    Segment right_original = right_shared;

    SegmentedLevel left_sub_seg = this->make_segmented_level({std::move(left_shared)});
    HybridLevel left = this->make_hybrid_level({std::move(left_sub_seg)});

    Segment extra_seg = this->make_random_segment(/*page_id=*/iter * 100 + 1401, max_pivot);
    SegmentedLevel right_sub_seg = this->make_segmented_level({
        std::move(right_shared),
        std::move(extra_seg),
    });
    HybridLevel right = this->make_hybrid_level({std::move(right_sub_seg)});

    Level result = std::move(left).merge(Level{std::move(right)}, node_pivot_count);

    ASSERT_TRUE(batt::is_case<HybridLevel>(result));
    auto& hybrid = std::get<HybridLevel>(result);

    ASSERT_EQ(hybrid.sub_levels.size(), 2u);

    auto& left_final_sub = std::get<SegmentedLevel>(hybrid.sub_levels[0]);
    EXPECT_EQ(left_final_sub.segment_count(), 0u);

    auto& right_final_sub = std::get<SegmentedLevel>(hybrid.sub_levels[1]);
    EXPECT_EQ(right_final_sub.segment_count(), 2u);

    auto& deduped = right_final_sub.get_segment(0);
    EXPECT_EQ(deduped.get_leaf_page_id().page_id, llfs::PageId{kSharedPageId});
    this->verify_pivots_deduplication(left_original, right_original, deduped, node_pivot_count);
    this->verify_filter_union(left_original,
                              right_original,
                              deduped,
                              /*check_range=*/kFilterItemCount);
  }
}

}  // namespace
