//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/tree/packed_blocked_leaf_page.hpp>
//
#include <turtle_kv/tree/packed_blocked_leaf_page.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "random_str.hpp"

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
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::pack_blocked_leaf_page;
using turtle_kv::PackedBlockedLeafPage;
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
  const usize kNumSeeds = 1000;
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

  std::geometric_distribution<usize> pick_prefix_size{0.5};
  std::uniform_int_distribution<usize> pick_prefix{0, kNumPrefixes - 1};
  std::geometric_distribution<usize> pick_key_size{0.7};
  std::uniform_int_distribution<usize> pick_value_size{0, kMaxValueSize - kMinValueSize};

  for (usize seed = 0; seed < kNumSeeds; ++seed) {
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

    StatusOr<PackedBlockedLeafPage*> packed_leaf =
        pack_blocked_leaf_page(kBlockSize, edits, leaf_buffer);

    ASSERT_TRUE(packed_leaf.ok()) << BATT_INSPECT(packed_leaf.status());
  }
}

}  // namespace
