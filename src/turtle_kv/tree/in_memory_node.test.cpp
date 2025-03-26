#include <turtle_kv/tree/in_memory_node.hpp>
//
#include <turtle_kv/tree/in_memory_node.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/tree/memory_storage.hpp>
#include <turtle_kv/tree/subtree_table.hpp>
#include <turtle_kv/tree/the_key.hpp>
#include <turtle_kv/tree/tree_builder.hpp>

#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <llfs/testing/test_config.hpp>
//
#include <llfs/testing/scenario_runner.hpp>

#include <llfs/appendable_job.hpp>

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
using turtle_kv::bit_count;
using turtle_kv::DecayToItem;
using turtle_kv::EditView;
using turtle_kv::global_max_key;
using turtle_kv::global_min_key;
using turtle_kv::InMemoryNode;
using turtle_kv::IsRoot;
using turtle_kv::KeyView;
using turtle_kv::make_memory_page_cache;
using turtle_kv::NeedsSplit;
using turtle_kv::None;
using turtle_kv::OkStatus;
using turtle_kv::Optional;
using turtle_kv::PinningPageLoader;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::StdMapTable;
using turtle_kv::Subtree;
using turtle_kv::SubtreeTable;
using turtle_kv::Table;
using turtle_kv::THE_KEY;
using turtle_kv::TreeBuilder;
using turtle_kv::TreeOptions;
using turtle_kv::TreeSerializeContext;
using turtle_kv::ValueView;
using turtle_kv::testing::RandomResultSetGenerator;

using llfs::StableStringStore;

using batt::getenv_as;

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

void verify_table_point_queries(Table& expected_table, Table& actual_table)
{
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
      StatusOr<ValueView> actual_value = actual_table.get(key);
      ASSERT_TRUE(actual_value.ok()) << BATT_INSPECT(actual_value) << BATT_INSPECT_STR(key);
      EXPECT_EQ(*actual_value, value);
      min_key = key;
    }
  }
}

struct SubtreeBatchUpdateScenario {
  llfs::RandomSeed seed;

  explicit SubtreeBatchUpdateScenario(llfs::RandomSeed seed_arg) noexcept : seed{seed_arg}
  {
  }

  void run();
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(InMemoryNodeTest, Segment)
{
  InMemoryNode::UpdateBuffer::Segment segment;
  InMemoryNode::UpdateBuffer::SegmentedLevel level;

  // Verify initial state.
  //
  segment.check_invariants(__FILE__, __LINE__);
  for (i32 pivot_i = 0; pivot_i < 64; ++pivot_i) {
    EXPECT_EQ(segment.get_flushed_item_upper_bound(level, pivot_i), 0);
    EXPECT_FALSE(segment.is_pivot_active(pivot_i));
  }
  EXPECT_EQ(segment.get_active_pivots(), u64{0});
  EXPECT_EQ(segment.get_flushed_pivots(), u64{0});

  // Keep a baseline to verify observed results.
  //
  std::array<u32, 64> expected_flushed_item_upper_bound;
  expected_flushed_item_upper_bound.fill(0);

  const auto get_expected_flushed_count = [&expected_flushed_item_upper_bound]() -> usize {
    usize total = 0;
    for (u32 value : expected_flushed_item_upper_bound) {
      if (value != 0) {
        ++total;
      }
    }
    return total;
  };

  for (i32 pivot_i = 0; pivot_i < 64; ++pivot_i) {
    segment.set_pivot_active(pivot_i, true);
  }

  // Perform random modifications to `segment`, verifying the resulting state at each step.
  //
  std::default_random_engine rng{/*seed=*/1};
  std::uniform_int_distribution<int> pick_percent{0, 99};
  std::uniform_int_distribution<i32> pick_bit{0, 63};
  std::uniform_int_distribution<u32> pick_upper_bound{1, 10};

  for (usize i = 0; i < 1000000; ++i) {
    // Reset the segment with probability 1%.
    //
    if (pick_percent(rng) < 1) {
      expected_flushed_item_upper_bound.fill(0);
      segment.flushed_pivots = 0;
      segment.flushed_item_upper_bound_.clear();

    } else {
      // Pick a pivot to change.
      //
      const i32 pivot_i = pick_bit(rng);

      // Set to zero with probability 20%.
      //
      if (pick_percent(rng) < 20) {
        expected_flushed_item_upper_bound[pivot_i] = 0;
        segment.set_flushed_item_upper_bound(pivot_i, 0);

      } else {
        // Pick a new non-zero upper bound.
        //
        const u32 new_upper_bound = pick_upper_bound(rng);

        expected_flushed_item_upper_bound[pivot_i] = new_upper_bound;
        segment.set_flushed_item_upper_bound(pivot_i, new_upper_bound);
      }
    }

    EXPECT_EQ(bit_count(segment.get_flushed_pivots()), get_expected_flushed_count());
    EXPECT_EQ(segment.flushed_item_upper_bound_.size(), get_expected_flushed_count());
    for (i32 pivot_i = 0; pivot_i < 64; ++pivot_i) {
      EXPECT_EQ(segment.get_flushed_item_upper_bound(level, pivot_i),
                expected_flushed_item_upper_bound[pivot_i]);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(InMemoryNodeTest, Subtree)
{
  llfs::testing::ScenarioRunner runner;

  // runner.n_threads(1);
  runner.n_seeds(64);
  runner.n_updates(0);
  runner.run(batt::StaticType<SubtreeBatchUpdateScenario>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SubtreeBatchUpdateScenario::run()
{
  static std::atomic<int> id{1};
  thread_local int my_id = id.fetch_add(1);

  const usize max_i = getenv_as<usize>("TURTLE_TREE_TEST_BATCH_COUNT").value_or(250);
  const usize chi = 4;
  const usize key_size = 24;
  const usize value_size = 100;
  const usize key_overhead = 4;
  const usize value_overhead = 5;
  const usize packed_item_size = key_size + key_overhead + value_size + value_overhead;

  TreeOptions tree_options = TreeOptions::with_default_values()  //
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB);

  const usize items_per_leaf = tree_options.flush_size() / packed_item_size;

  if (my_id == 0) {
    std::cout << BATT_INSPECT(items_per_leaf) << BATT_INSPECT(tree_options.flush_size())
              << BATT_INSPECT(tree_options.max_item_size()) << std::endl;
  }

  std::shared_ptr<llfs::PageCache> page_cache =
      make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                             tree_options,
                             /*byte_capacity=*/1500 * kMiB);

  StableStringStore strings;
  RandomResultSetGenerator result_set_generator;
  StdMapTable expected_table;

  result_set_generator.set_key_size(24).set_value_size(100).set_size(items_per_leaf);

  BATT_DEBUG_INFO(BATT_INSPECT(this->seed));

  std::default_random_engine rng{this->seed};

  Subtree tree = Subtree::make_empty();

  ASSERT_TRUE(tree.is_serialized());

  SubtreeTable actual_table{*page_cache, tree};

  if (my_id == 0) {
    std::cout << BATT_INSPECT(tree.dump()) << std::endl;
  }

  batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool();

  // batt::require_fail_global_default_log_level() = batt::LogLevel::kInfo;

  Optional<PinningPageLoader> page_loader{*page_cache};

  usize total_items = 0;

  for (usize i = 0; i < max_i; ++i) {
    BatchUpdate update{
        .worker_pool = worker_pool,
        .page_loader = *page_loader,
        .cancel_token = batt::CancelToken{},
        .result_set = result_set_generator(DecayToItem<false>{}, rng, strings),
        .edit_size_totals = None,
    };
    update.update_edit_size_totals();
    total_items += update.result_set.size();

    if (update.result_set.find_key(THE_KEY).ok()) {
      LOG(INFO) << BATT_INSPECT(i) << " contains THE KEY";
    }

    Status table_update_status = update_table(expected_table, update.result_set);
    ASSERT_TRUE(table_update_status.ok()) << BATT_INSPECT(table_update_status);

    StatusOr<i32> tree_height = tree.get_height(*page_loader);
    ASSERT_TRUE(tree_height.ok()) << BATT_INSPECT(tree_height);

    Status status =  //
        tree.apply_batch_update(tree_options,
                                /*parent_height=*/*tree_height + 1,
                                update,
                                /*key_upper_bound=*/global_max_key(),
                                IsRoot{true});

    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status) << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

    ASSERT_FALSE(tree.is_serialized());

    if (batt::is_case<NeedsSplit>(tree.get_viability())) {
      StatusOr<Subtree> next_sibling = tree.try_split(*page_loader);

      ASSERT_TRUE(next_sibling.ok())
          << BATT_INSPECT(next_sibling.status()) << BATT_INSPECT(tree.dump())
          << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

      StatusOr<std::unique_ptr<InMemoryNode>> new_root =
          InMemoryNode::from_subtrees(*page_loader,
                                      tree_options,
                                      std::move(tree),
                                      std::move(*next_sibling),
                                      global_max_key(),
                                      IsRoot{true});

      ASSERT_TRUE(new_root.ok()) << BATT_INSPECT(new_root.status());

      tree = Subtree{
          .impl = std::move(*new_root),
      };
    }

    if (my_id == 0) {
      std::cout << std::setw(4) << i << "/" << max_i << " (items=" << total_items
                << "):" << BATT_INSPECT(tree.dump()) << std::endl;
    }

    ASSERT_NO_FATAL_FAILURE(verify_table_point_queries(expected_table, actual_table))
        << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

    if (((i + 1) % chi) == 0) {
      if (my_id == 0) {
        std::cout << "taking checkpoint..." << std::endl;
      }

      std::unique_ptr<llfs::PageCacheJob> page_job = page_cache->new_job();
      TreeSerializeContext context{tree_options, *page_job, worker_pool};

      Status start_status = tree.start_serialize(context);
      ASSERT_TRUE(start_status.ok()) << BATT_INSPECT(start_status);

      Status build_status = context.build_all_pages();
      ASSERT_TRUE(build_status.ok()) << BATT_INSPECT(build_status);

      StatusOr<llfs::PinnedPage> finish_status = tree.finish_serialize(context);
      ASSERT_TRUE(finish_status.ok()) << BATT_INSPECT(finish_status);

      if (my_id == 0) {
        std::cout << "checkpoint OK; verifying checkpoint..." << std::endl;
      }

      page_job->new_root(finish_status->page_id());
      Status commit_status = llfs::unsafe_commit_job(std::move(page_job));
      ASSERT_TRUE(commit_status.ok()) << BATT_INSPECT(commit_status);

      ASSERT_NO_FATAL_FAILURE(verify_table_point_queries(expected_table, actual_table))
          << BATT_INSPECT(this->seed) << BATT_INSPECT(i);

      if (my_id == 0) {
        std::cout << "checkpoint verified!" << std::endl;
      }

      // Release the pinned pages from the previous checkpoint.
      //
      page_loader.emplace(*page_cache);
    }
  }
}

}  // namespace
