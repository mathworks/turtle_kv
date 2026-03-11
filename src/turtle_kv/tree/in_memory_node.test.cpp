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
using llfs::StableStringStore;

using batt::getenv_as;

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
    for (const KeyView& key : this->pending_deletes) {
      EXPECT_EQ(expected_table.get(key).status(), batt::StatusCode::kNotFound);
      EXPECT_EQ(actual_table.get(key).status(), batt::StatusCode::kNotFound);
    }
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(InMemoryNodeTest, Subtree)
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
        // TODO [vsilai 2026-01-09] Enable delete support for batch generation.
        //
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
    ASSERT_FALSE(tree.is_serialized());
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
        std::unique_ptr<llfs::PageCacheJob> scanner_page_job = page_cache->new_job();

        const usize scan_len = pick_scan_len(rng);
        std::array<std::pair<KeyView, ValueView>, kMaxScanSize> scan_items_buffer;
        KeyView min_key = update.result_set.get_min_key();

        PageSliceStorage page_slice_storage;

        KVStoreScanner kv_scanner{
            *page_loader,
            root_ptr->page_id_slot_or_panic(),
            BATT_OK_RESULT_OR_PANIC(root_ptr->get_height(*page_loader,  //
                                                         llfs::PageCacheOvercommit::not_allowed())),
            min_key,
            tree_options.trie_index_sharded_view_size(),
            &page_slice_storage};

        usize n_read = 0;
        {
          LatencyTimer timer{scan_latency};
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
            << BATT_INSPECT(i) << BATT_INSPECT_STR(min_key) << BATT_INSPECT(scan_len);
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

TEST(InMemoryNodeTest, SubtreeDeletions)
{
  const usize key_size = 24;
  const usize value_size = 100;
  const usize chi = 4;

  TreeOptions tree_options = TreeOptions::with_default_values()  //
                                 .set_leaf_size(32 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(key_size)
                                 .set_value_size_hint(value_size);

  usize items_per_leaf = tree_options.flush_size() / tree_options.expected_item_size();
  usize total_batches = 81;

  std::vector<KeyView> keys;
  keys.reserve(total_batches * items_per_leaf);

  std::string value_str = std::string(value_size, 'a');
  ValueView value = ValueView::from_str(value_str);

  std::default_random_engine rng{/*seed=*/3};
  RandomStringGenerator generate_key;
  llfs::StableStringStore store;
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

    usize per_batch = items_per_leaf / total_batches;
    usize batch_remainder = items_per_leaf % total_batches;
    usize total_amount_per_batch = per_batch + (batch_number < batch_remainder ? 1 : 0);

    for (usize i = 0; i < total_batches; ++i) {
      usize base_i = i * items_per_leaf;
      usize offset = batch_number * per_batch + std::min(batch_number, batch_remainder);

      for (usize j = 0; j < total_amount_per_batch; ++j) {
        current_batch.emplace_back(keys[base_i + offset + j], ValueView::deleted());
      }
    }
    BATT_CHECK_LE(current_batch.size(), items_per_leaf) << BATT_INSPECT(batch_number);

    return current_batch;
  };

  const auto apply_tree_updates = [&](auto batch_creation_func, bool perform_scan) {
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
                                     llfs::PageCacheOvercommit::not_allowed()};

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

        if (perform_scan) {
          auto root_ptr = std::make_shared<Subtree>(tree.clone_serialized_or_panic());
          std::unique_ptr<llfs::PageCacheJob> scanner_page_job = page_cache->new_job();

          const usize scan_len = 20;
          std::array<std::pair<KeyView, ValueView>, kMaxScanSize> scan_items_buffer;
          KeyView min_key = update.result_set.get_min_key();

          PageSliceStorage page_slice_storage;

          KVStoreScanner kv_scanner{*page_loader,
                                    root_ptr->page_id_slot_or_panic(),
                                    BATT_OK_RESULT_OR_PANIC(root_ptr->get_height(
                                        *page_loader,  //
                                        llfs::PageCacheOvercommit::not_allowed())),
                                    min_key,
                                    tree_options.trie_index_sharded_view_size(),
                                    &page_slice_storage};

          usize n_read = 0;
          {
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
              << BATT_INSPECT(i) << BATT_INSPECT_STR(min_key) << BATT_INSPECT(scan_len);
        }

        page_loader.emplace(*page_cache);
      }
    }
  };

  LOG(INFO) << "Inserting key/value pairs into tree...";
  apply_tree_updates(create_insertion_batch, false);

  LOG(INFO) << "Deleting key/value pairs from tree...";
  for (usize i = 0; i < total_batches; ++i) {
    bool perform_scan = i == 0 ? true : false;
    StatusOr<i32> tree_height =
        tree.get_height(*page_loader, llfs::PageCacheOvercommit::not_allowed());
    ASSERT_TRUE(tree_height.ok()) << BATT_INSPECT(tree_height);
    if (*tree_height > 0) {
      apply_tree_updates(create_deletion_batch, perform_scan);
    } else {
      break;
    }
  }
}

}  // namespace
