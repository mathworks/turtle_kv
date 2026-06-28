//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>
//
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.ipp>
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.item_iterator.hpp>
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.sharded_live_ranges.hpp>
#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.sharded_live_ranges.ipp>
#include <turtle_kv/tree/random_str.hpp>

#include <turtle_kv/util/piecewise_filter.ipp>
#include <turtle_kv/util/piecewise_filter.test.hpp>

#include <turtle_kv/import/constants.hpp>

#include <batteries/bit_ops/bit_count.hpp>
#include <batteries/stable_string_store.hpp>

#include <random>
#include <unordered_set>
#include <vector>

namespace {

using namespace batt::int_types;
using namespace batt::constants;

using batt::MutableBuffer;
using batt::StableStringStore;
using batt::StatusOr;

using turtle_kv::EditView;
using turtle_kv::Interval;
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::Optional;
using turtle_kv::pack_blocked_leaf_page;
using turtle_kv::PackedBlockedLeafPage;
using turtle_kv::PackedKeyValueSlotPtr;
using turtle_kv::PiecewiseFilter;
using turtle_kv::random_str;
using turtle_kv::ValueView;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Plan:
//  1. For different random seeds:
//     - generate random set of prefixes (~10% of total keys)
//     - generate keys using prefixes, with random values
//     - sort
//     - pack leaf; verify:
//       a. all packed keys present and have right values
//       b. any unpacked keys at end missing
//       c. randomly generated non-present keys not found
//
TEST(TreePackedBlockedLeafPageTest, Random)
{
  const usize kFirstSeed = 0;
  const usize kNumSeeds = 1000;
  const usize kLastSeed = kFirstSeed + kNumSeeds;
  const usize kLeafPageSize = 1 * kMiB;
  const usize kNumPrefixes = 1000;
  const usize kMinPrefixSize = 0;
  const usize kMaxPrefixSize = 8;
  const usize kMinKeySize = 4;
  const usize kMaxKeySize = 48;
  const usize kMinValueSize = 0;
  const usize kMaxValueSize = 200;
  const usize kBlockSize = 8192;

  BATT_CHECK_EQ(batt::bit_count(kLeafPageSize), 1);

  std::uniform_int_distribution<i32> pick_pct{0, 99};
  std::geometric_distribution<usize> pick_prefix_size{0.5};
  std::uniform_int_distribution<usize> pick_prefix{0, kNumPrefixes - 1};
  std::geometric_distribution<usize> pick_key_size{0.7};
  std::uniform_int_distribution<usize> pick_value_size{0, kMaxValueSize - kMinValueSize};

  for (usize seed = kFirstSeed; seed < kLastSeed; ++seed) {
    LOG_EVERY_N(INFO, 25) << BATT_INSPECT(seed);

    std::default_random_engine rng{seed};

    StableStringStore strings;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Generate prefixes
    //
    std::vector<std::string_view> prefixes;
    {
      std::unordered_set<std::string_view> used_prefixes;
      while (prefixes.size() < kNumPrefixes) {
        std::string_view prefix =
            random_str(rng, pick_prefix_size, kMinPrefixSize, kMaxPrefixSize, strings);

        if (used_prefixes.count(prefix)) {
          continue;
        }
        prefixes.push_back(prefix);
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Generate edits.
    //
    std::vector<EditView> edits;
    {
      usize max_edit_size = 0;
      usize max_key_size = 0;
      usize total_edits_size = 0;

      std::unordered_set<std::string_view> used_keys;
      for (;;) {
        std::string_view prefix = prefixes[pick_prefix(rng)];

        std::string_view key =
            random_str(rng, pick_key_size, kMinKeySize, kMaxKeySize, strings, prefix);

        if (used_keys.count(key)) {
          continue;
        }
        used_keys.insert(key);

        std::string_view value =
            random_str(rng, pick_value_size, kMinValueSize, kMaxValueSize, strings);

        EditView edit{key, ValueView::from_str(value)};

        const usize edit_size = PackedBlockedLeafPage::packed_edit_size(edit);

        const usize new_max_edit_size = std::max(max_edit_size, edit_size);
        const usize new_max_key_size = std::max(max_key_size, key.size());

        const usize space_available = PackedBlockedLeafPage::estimate_capacity(kLeafPageSize,
                                                                               kBlockSize,
                                                                               new_max_key_size,
                                                                               new_max_edit_size);

        // Stop as soon as adding the next key would exceed the estimated space.
        //
        if (edit_size + total_edits_size > space_available) {
          break;
        }

        edits.push_back(edit);
        total_edits_size += edit_size;
        max_edit_size = new_max_edit_size;
        max_key_size = new_max_key_size;
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Sort edits by key.
    //
    std::sort(edits.begin(), edits.end(), KeyOrder{});

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Pack a blocked leaf page.
    //
    using StorageUnit = std::aligned_storage_t<4096, 4096>;
    std::vector<StorageUnit> leaf_storage(kLeafPageSize / sizeof(StorageUnit));
    ASSERT_EQ(sizeof(StorageUnit) * leaf_storage.size(), kLeafPageSize);

    MutableBuffer leaf_buffer{leaf_storage.data(), kLeafPageSize};

    StatusOr<PackedBlockedLeafPage*> status_or_packed_leaf =
        pack_blocked_leaf_page(kBlockSize, edits, leaf_buffer);

    ASSERT_TRUE(status_or_packed_leaf.ok()) << BATT_INSPECT(status_or_packed_leaf.status());

    const PackedBlockedLeafPage& packed_leaf = PackedBlockedLeafPage::view_of(leaf_buffer);

    ASSERT_EQ(&packed_leaf, *status_or_packed_leaf);
    ASSERT_EQ(packed_leaf.min_key(), get_key(edits.front()));
    ASSERT_EQ(packed_leaf.max_key(), get_key(edits.back()));

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Scan over all items in the packed leaf to make sure they are all there.
    //
    {
      PackedBlockedLeafPage::ItemIterator item_iter = packed_leaf.items_begin();
      PackedBlockedLeafPage::ItemIterator items_end = packed_leaf.items_end();

      std::vector<std::pair<PackedBlockedLeafPage::ItemIterator, isize>> past_items;

      auto packed_items = packed_leaf.items_seq();
      using Item = decltype(*packed_items.peek());
      Optional<KeyView> prev_key;
      Optional<PackedBlockedLeafPage::ItemIterator> prev_item_iter;

      isize position = 0;

      for (const EditView& edit : edits) {
        Optional<Item> next_packed = packed_items.next();

        if (prev_key) {
          ASSERT_GT(get_key(edit), *prev_key);
        }
        prev_key = get_key(edit);

        ASSERT_TRUE(next_packed.has_value());
        ASSERT_EQ(get_key(*next_packed), get_key(edit));
        ASSERT_EQ(get_value(*next_packed), get_value(edit));

        // Test PackedBlockedLeafPage::find_key.
        //
        const PackedKeyValueSlotPtr* found = packed_leaf.find_key(get_key(edit));

        ASSERT_NE(found, nullptr);
        ASSERT_EQ(found, std::addressof(*item_iter));
        ASSERT_EQ(get_key(*found), get_key(edit));
        ASSERT_EQ(get_value(*found), get_value(edit))
            << BATT_INSPECT_STR(get_key(*found)) << BATT_INSPECT(edit);

        // Test PackedBlockedLeafPage::lower_bound.
        //
        {
          PackedBlockedLeafPage::ItemIterator lb_iter = packed_leaf.lower_bound(get_key(edit));

          ASSERT_NE(lb_iter, items_end);
          ASSERT_EQ(get_key(*lb_iter), get_key(edit));
          ASSERT_EQ(get_value(*lb_iter), get_value(edit));
        }

        ASSERT_EQ(packed_leaf.item_at(position), item_iter);

        ASSERT_NE(item_iter, items_end);
        ASSERT_LT(item_iter, items_end);
        ASSERT_EQ(std::distance(packed_leaf.items_begin(), item_iter), position);

        if (pick_pct(rng) < 1) {
          past_items.push_back(std::make_pair(item_iter, position));
        }

        for (const auto& [past_iter, past_position] : past_items) {
          ASSERT_EQ(std::distance(past_iter, item_iter), position - past_position);
          ASSERT_EQ(std::distance(item_iter, past_iter), past_position - position);
          ASSERT_EQ(past_iter + (position - past_position), item_iter);
          ASSERT_EQ(item_iter - (position - past_position), past_iter);
          ASSERT_LE(past_iter, item_iter);
          ASSERT_GE(item_iter, past_iter) << BATT_INSPECT(position) << BATT_INSPECT(past_position);
        }

        if (prev_item_iter) {
          ASSERT_EQ(std::next(*prev_item_iter), item_iter);
          ASSERT_EQ(*prev_item_iter, std::prev(item_iter));
        }

        prev_item_iter = item_iter;
        ++item_iter;
        ++position;
      }
      ASSERT_FALSE(packed_items.peek().has_value());
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Test ShardedLiveRanges.
    //
    {
      for (usize j = 0; j < 10000; ++j) {
        // Drop up to 64 sub-ranges of the leaf.
        //
        for (usize drop_count = 0; drop_count < 64; ++drop_count) {
          PiecewiseFilter<u32> leaf_filter;

          std::vector<Interval<u32>> dropped_ranges;
          u32 items_dropped = 0;
          const u32 item_count = packed_leaf.item_count();

          std::tie(items_dropped, dropped_ranges) =
              turtle_kv::testing::drop_n_disjoint_intervals_from(&leaf_filter,
                                                                 drop_count,
                                                                 Interval<u32>{0, item_count},
                                                                 rng);

          // Verify the number of expected live items.
          //
          const u32 expected_live_count = item_count - items_dropped;

          if (drop_count > 1) {
            ASSERT_GT(expected_live_count, 0)
                << BATT_INSPECT_RANGE(dropped_ranges) << BATT_INSPECT(drop_count)
                << BATT_INSPECT(item_count);
          }

          u32 actual_live_count = 0;
          u32 next_possible_live = 0;
          u32 next_possible_block = 0;

          packed_leaf.sharded_live_ranges(leaf_filter, Interval<u32>{0, item_count}) |
              batt::seq::for_each([&](const std::pair<u32, Interval<u32>>& live_pair) {
                const auto [block_index, live_range] = live_pair;

                BATT_CHECK_GE(block_index, next_possible_block);
                BATT_CHECK_LT(block_index, packed_leaf.block_count());
                BATT_CHECK_GE(live_range.lower_bound, next_possible_live)
                    << BATT_INSPECT(j) << BATT_INSPECT(drop_count) << BATT_INSPECT(live_range)
                    << BATT_INSPECT(item_count);
                BATT_CHECK_LT(live_range.lower_bound, live_range.upper_bound);
                BATT_CHECK_LE(live_range.upper_bound, item_count);

                const Interval<u32> block_range =
                    packed_leaf.item_index_range_of_block(block_index);

                BATT_CHECK_GE(live_range.lower_bound, block_range.lower_bound);
                BATT_CHECK_LE(live_range.upper_bound, block_range.upper_bound);

                next_possible_live = live_range.upper_bound;
                next_possible_block = block_index;

                actual_live_count += live_range.size();
              });

          ASSERT_EQ(actual_live_count, expected_live_count)
              << BATT_INSPECT(j) << BATT_INSPECT(drop_count);
        }
      }
    }
  }
}

}  // namespace
