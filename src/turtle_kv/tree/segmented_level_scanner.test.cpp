#include <turtle_kv/tree/segmented_level_scanner.hpp>
//
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/tree/testing/fake_node.hpp>
#include <turtle_kv/tree/testing/random_leaf_generator.hpp>

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segmented_levels.hpp>

#include <turtle_kv/tree/segmented_level_builder.hpp>

#include <turtle_kv/util/bit_ops.hpp>

#include <llfs/testing/test_config.hpp>
//
#include <llfs/testing/scenario_runner.hpp>

#include <batteries/checked_cast.hpp>

#include <map>
#include <random>

namespace {

using namespace turtle_kv::int_types;
namespace seq = batt::seq;

using turtle_kv::CInterval;
using turtle_kv::DecayToItem;
using turtle_kv::EditSlice;
using turtle_kv::in_segmented_level;
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::MergeCompactor;
using turtle_kv::Optional;
using turtle_kv::PackedLeafPage;
using turtle_kv::SegmentedLevelBuilder;
using turtle_kv::SegmentedLevelScanner;
using turtle_kv::set_bit;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::testing::FakeLevel;
using turtle_kv::testing::FakeNode;
using turtle_kv::testing::FakePageLoader;
using turtle_kv::testing::FakePinnedPage;
using turtle_kv::testing::FakeSegment;
using turtle_kv::testing::RandomLeafGenerator;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SegmentedLevelScannerTest : public ::testing::Test
{
 public:
  struct Scenario {
    explicit Scenario(llfs::RandomSeed seed) noexcept
    {
      this->set_random_seed(seed);
    }

    void set_random_seed(u32 seed) noexcept
    {
      rng.emplace(seed);
      for (usize i = 0; i < 10; ++i) {
        (*rng)();
      }
    }

    void set_key_size(usize n) noexcept
    {
      this->leaf_generator.items_generator().key_generator().set_size(n);
    }

    void set_value_size(usize n) noexcept
    {
      this->leaf_generator.items_generator().set_value_size(n);
    }

    void set_item_count(usize n) noexcept
    {
      this->leaf_generator.items_generator().set_size(n);
    }

    void set_leaf_size(usize n) noexcept
    {
      this->fake_page_loader.emplace(llfs::PageSize{BATT_CHECKED_CAST(u32, n)});
    }

    template <bool kDecayToItems>
    RandomLeafGenerator::Result<kDecayToItems> generate_leaf_pages() noexcept
    {
      return this->leaf_generator(DecayToItem<kDecayToItems>{}, *this->rng, *this->fake_page_loader,
                                  *this->string_store);
    }

    template <bool kDecayToItems>
    FakeNode make_fake_node(const RandomLeafGenerator::Result<kDecayToItems>& generated,
                            usize pivot_count) noexcept
    {
      // The final result object.
      //
      FakeNode fake_node;

      // Select the pivot keys by evenly subdividing the items.
      //
      const auto items_slice = generated.result_set.get();
      const usize item_count = items_slice.size();
      {
        usize div = item_count / pivot_count;
        usize mod = item_count % pivot_count;
        for (usize i = 0, j = 0; i < item_count; ++j) {
          usize items_in_this_pivot = div + ((j < mod) ? 1 : 0);

          fake_node.pivot_keys_.emplace_back(get_key(items_slice[i]));
          fake_node.items_per_pivot_.emplace_back(items_in_this_pivot);

          i += items_in_this_pivot;
          BATT_CHECK_LE(i, item_count);
          BATT_CHECK_EQ(j, fake_node.pivot_count());
        }

        fake_node.pivot_keys_.emplace_back("~");
      }

      // Verify pivot count and FakeNode::items_per_pivot_.
      //
      {
        EXPECT_EQ(fake_node.pivot_count(), pivot_count);

        std::vector<usize> prefix_sum;
        for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
          prefix_sum.push_back(std::distance(
              items_slice.begin(), std::lower_bound(items_slice.begin(), items_slice.end(),
                                                    fake_node.get_pivot_key(pivot_i), KeyOrder{})));
        }
        prefix_sum.push_back(items_slice.size());

        EXPECT_EQ(prefix_sum[0], 0);
        for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
          EXPECT_EQ(prefix_sum[pivot_i + 1] - prefix_sum[pivot_i],
                    fake_node.items_per_pivot_[pivot_i])
              << BATT_INSPECT(pivot_i) << BATT_INSPECT(prefix_sum[pivot_i])
              << BATT_INSPECT(prefix_sum[pivot_i + 1]);
        }
      }

      // Create the FakeLevel.
      //
      FakeLevel& fake_level = fake_node.level_;

      SegmentedLevelBuilder<FakeNode, FakeLevel, FakeSegment, FakePageLoader, FakePinnedPage>
          level_builder{fake_node, fake_level};

      level_builder.add_segments_for_leaf_pages(generated.leaf_pages);

      return fake_node;
    }

    template <bool kDecayToItems>
    void verify_items(const MergeCompactor::ResultSet<kDecayToItems>& expected,
                      const FakeNode& actual) noexcept
    {
      std::vector<std::string_view> expected_keys, actual_keys;

      // Collect expected keys.
      //
      for (const auto& item : expected.get()) {
        expected_keys.emplace_back(get_key(item));
      }

      // Collect actual keys, using SegmentedLevelScanner.
      //
      {
        SegmentedLevelScanner<const FakeNode, const FakeLevel, FakePageLoader> scanner{
            actual, actual.level_, *this->fake_page_loader, llfs::PinPageToJob::kDefault};

        std::move(scanner) | seq::for_each([&](const EditSlice& edit_slice) {
          batt::case_of(   //
              edit_slice,  //
              [&](const auto& edits) {
                for (const auto& edit : edits) {
                  actual_keys.emplace_back(get_key(edit));
                }
              });
        });
      }
      ASSERT_EQ(expected_keys.size(), actual_keys.size());

      i32 pivot_i = 0;
      for (usize i = 0; i < expected_keys.size(); ++i) {
        ASSERT_EQ(expected_keys[i], actual_keys[i]) << BATT_INSPECT(i);

        // Advance pivot_i.
        //
        const i32 old_pivot_i = pivot_i;
        while (actual.get_pivot_key(pivot_i + 1) <= expected_keys[i]) {
          ++pivot_i;
        }

        // When we enter the range of a new pivot, run a new scan with min_pivot set.
        //
        if (pivot_i != old_pivot_i) {
          std::vector<std::string_view> actual_keys2;
          {
            SegmentedLevelScanner<const FakeNode, const FakeLevel, FakePageLoader> scanner2{
                actual, actual.level_, *this->fake_page_loader, llfs::PinPageToJob::kDefault,
                /*min_pivot=*/pivot_i};

            std::move(scanner2) | seq::for_each([&](const EditSlice& edit_slice) {
              batt::case_of(   //
                  edit_slice,  //
                  [&](const auto& edits) {
                    for (const auto& edit : edits) {
                      actual_keys2.emplace_back(get_key(edit));
                    }
                  });
            });
          }

          EXPECT_EQ(expected_keys.size() - i, actual_keys2.size());
          for (usize j = i; j < expected_keys.size(); ++j) {
            ASSERT_EQ(expected_keys[j], actual_keys2[j - i]);
          }
        }
      }
    }

    void run_with_pivot_count(usize pivot_count);

    void run()
    {
      for (usize pivot_count = 2; pivot_count <= 64; ++pivot_count) {
        ASSERT_NO_FATAL_FAILURE(this->run_with_pivot_count(pivot_count));
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Optional<llfs::StableStringStore> string_store;
    Optional<std::default_random_engine> rng{std::random_device{}()};
    RandomLeafGenerator leaf_generator;
    Optional<FakePageLoader> fake_page_loader{llfs::PageSize{256 * 1024}};
  };
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(SegmentedLevelScannerTest, Test)
{
  llfs::testing::ScenarioRunner runner;

  runner.n_seeds(64);
  runner.n_updates(4);
  runner.run(batt::StaticType<Scenario>{});
}

void SegmentedLevelScannerTest::Scenario::run_with_pivot_count(usize pivot_count)
{
  constexpr usize kMaxPivotCount = 64;

  // Configure the test.
  //
  const bool debug_output = false;
  const usize key_size = 24;
  const usize value_size = 100;
  const usize item_count = 20 * 1000;
  const usize leaf_size = 256 * 1024;
  const usize leaf_overhead = 16 * 1024 + 64 + 100;
  const usize space_per_leaf = leaf_size - leaf_overhead;
  const usize size_per_item = key_size + value_size + 4 /*key_overhead*/ + 5 /*value_overhead*/;
  const usize leaf_count = (size_per_item * item_count + space_per_leaf - 1) / space_per_leaf;

  this->string_store.emplace();
  // this->set_random_seed(std::random_device{}());
  this->set_key_size(key_size);
  this->set_value_size(value_size);
  this->set_item_count(item_count);
  this->set_leaf_size(leaf_size);

  // Generate a set of leaf pages.
  //
  auto generated = this->generate_leaf_pages<false>();

  EXPECT_EQ(generated.result_set.size(), item_count);
  EXPECT_EQ(generated.leaf_pages.size(), leaf_count);
  EXPECT_EQ(generated.leaf_page_ids.size(), generated.leaf_pages.size());

  // Verify that the result set and packed leaf pages contain exactly the same keys.
  //
  {
    std::vector<std::string_view> result_set_keys, leaf_page_keys;

    for (const auto& item : generated.result_set.get()) {
      result_set_keys.emplace_back(get_key(item));
    }
    for (FakePinnedPage& page : generated.leaf_pages) {
      auto& leaf_view = PackedLeafPage::view_of(page.get_page_buffer());
      for (const auto& item : leaf_view.items_slice()) {
        leaf_page_keys.emplace_back(get_key(item));
      }
    }

    ASSERT_EQ(result_set_keys.size(), leaf_page_keys.size());
    ASSERT_EQ(result_set_keys.size(), item_count);

    for (usize i = 0; i < item_count; ++i) {
      ASSERT_EQ(result_set_keys[i], leaf_page_keys[i]) << BATT_INSPECT(i);
    }
  }

  // Make FakeNode and FakeLevel for the generated pages.
  //
  FakeNode fake_node = this->make_fake_node(generated, pivot_count);

  if (debug_output) {
    for (usize pivot_i = 0; pivot_i < fake_node.pivot_count(); ++pivot_i) {
      std::cout << BATT_INSPECT(pivot_i) << ": "
                << batt::c_str_literal(fake_node.get_pivot_key(pivot_i)) << std::endl;
    }
    std::cout << BATT_INSPECT_RANGE(fake_node.items_per_pivot_) << std::endl;
  }

  for (usize segment_i = 0; segment_i < fake_node.level_.segment_count(); ++segment_i) {
    FakeSegment& fake_segment = fake_node.level_.get_segment(segment_i);
    auto& leaf_view = PackedLeafPage::view_of(generated.leaf_pages[segment_i].get_page_buffer());

    if (debug_output) {
      std::cout << BATT_INSPECT(segment_i) << BATT_INSPECT(leaf_view.get_key_crange()) << "\t"
                << std::bitset<kMaxPivotCount>{fake_segment.get_active_pivots()} << " "
                << batt::dump_range(fake_segment.pivot_items_count_) << std::endl;
    }
  }

  // Baseline: scanner returns correct items on unmodified set.
  //
  ASSERT_NO_FATAL_FAILURE(this->verify_items(generated.result_set, fake_node));

  // Make a copy of the result set so we can drop items from the pivots.
  //
  auto test_item_set = generated.result_set;
  const auto all_items_slice = generated.result_set.get();

  std::vector<KeyView> min_unflushed_key(fake_node.pivot_count());
  std::vector<usize> pivots_with_items;
  for (usize pivot_i = 0; pivot_i < fake_node.pivot_count(); ++pivot_i) {
    if (fake_node.items_per_pivot_[pivot_i] > 0) {
      pivots_with_items.emplace_back(pivot_i);
    }
    min_unflushed_key[pivot_i] = fake_node.get_pivot_key(pivot_i);
  }

  while (!pivots_with_items.empty()) {
    std::uniform_int_distribution<usize> pick_pivot{0, pivots_with_items.size() - 1};
    const usize i = pick_pivot(*this->rng);
    const usize pivot_i = pivots_with_items[i];
    const usize n_items = fake_node.items_per_pivot_[pivot_i];

    std::uniform_int_distribution<usize> pick_n_items_to_drop{(n_items + 2) / 3, n_items};
    const usize n_items_to_drop = pick_n_items_to_drop(*this->rng);

    if (debug_output) {
      std::cout << "Flushing " << n_items_to_drop << "/" << n_items << " from pivot " << pivot_i
                << std::endl;
    }

    auto unflushed_begin = std::lower_bound(all_items_slice.begin(), all_items_slice.end(),
                                            min_unflushed_key[pivot_i], KeyOrder{});

    auto flush_end = unflushed_begin + n_items_to_drop;

    KeyView min_key_to_flush = min_unflushed_key[pivot_i];
    KeyView max_key_to_flush = get_key(*std::prev(flush_end));

    Status flush_status = in_segmented_level(fake_node, fake_node.level_, *this->fake_page_loader)
                              .flush_pivot_up_to_key(pivot_i, max_key_to_flush);

    ASSERT_TRUE(flush_status.ok()) << BATT_INSPECT(flush_status);

    test_item_set.drop_key_range(CInterval<KeyView>{min_key_to_flush, max_key_to_flush});

    fake_node.items_per_pivot_[pivot_i] -= n_items_to_drop;
    if (fake_node.items_per_pivot_[pivot_i] == 0) {
      std::swap(pivots_with_items[i], pivots_with_items.back());
      pivots_with_items.pop_back();
    } else {
      BATT_CHECK_NE(flush_end, all_items_slice.end());
      min_unflushed_key[pivot_i] = get_key(*flush_end);
    }

    if (debug_output) {
      std::cout << std::endl << "New state: " << std::endl;
    }
    for (usize segment_i = 0; segment_i < fake_node.level_.segment_count(); ++segment_i) {
      FakeSegment& segment = fake_node.level_.get_segment(segment_i);

      if (debug_output) {
        std::cout << std::setw(3) << segment_i
                  << ": active=" << std::bitset<kMaxPivotCount>{segment.get_active_pivots()}
                  << " flushed=" << std::bitset<kMaxPivotCount>{segment.get_flushed_pivots()}
                  << std::endl;
      }
    }
    if (debug_output) {
      std::cout << std::endl << "Verifying..." << std::endl;
    }

    ASSERT_NO_FATAL_FAILURE(this->verify_items(test_item_set, fake_node));

    if (debug_output) {
      std::cout << "OK! (continuing)" << std::endl << std::endl;
    }
  }

  if (debug_output) {
    std::cout << "DONE (no more to flush)" << std::endl;
  }
}

}  // namespace
