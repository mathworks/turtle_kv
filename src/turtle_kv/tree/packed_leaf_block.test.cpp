//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/tree/packed_leaf_block.hpp>
//
#include <turtle_kv/tree/packed_leaf_block.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "random_str.hpp"

#include <batteries/stable_string_store.hpp>

#include <random>
#include <unordered_set>
#include <vector>

namespace {

using namespace batt::int_types;

using batt::MutableBuffer;
using batt::StableStringStore;
using batt::StatusOr;

using turtle_kv::EditView;
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::random_str;
using turtle_kv::ValueView;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(TreePackedLeafBlockTest, Random)
{
  const usize kNumSeeds = 1000;
  const usize kNumNotFoundQueries = 100;
  const usize kNumLowerBoundQueries = 500;
  const usize kMinPrefixSize = 0;
  const usize kMaxPrefixSize = 8;
  const usize kMinKeySize = 4;
  const usize kMaxKeySize = 48;
  const usize kMinValueSize = 0;
  const usize kMaxValueSize = 200;
  const usize kBlockSize = 8192;

  std::geometric_distribution<usize> pick_prefix_size{0.5};
  std::geometric_distribution<usize> pick_key_size{0.7};
  std::uniform_int_distribution<usize> pick_value_size{0, kMaxValueSize - kMinValueSize};

  for (usize seed = 0; seed < kNumSeeds; ++seed) {
    std::default_random_engine rng{seed};

    StableStringStore strings;

    std::string_view prefix =
        random_str(rng, pick_prefix_size, kMinPrefixSize, kMaxPrefixSize, strings);

    std::unordered_set<std::string_view> used_keys;
    std::vector<EditView> src_edits;

    // Generate enough random edits to fill a block.
    //
    usize src_size = 0;
    while (src_size < kBlockSize) {
      std::string_view key =
          random_str(rng, pick_key_size, kMinKeySize, kMaxKeySize, strings, prefix);
      if (used_keys.count(key)) {
        continue;
      }
      used_keys.insert(key);

      std::string_view value =
          random_str(rng, pick_value_size, kMinValueSize, kMaxValueSize, strings);

      src_edits.push_back(EditView{key, ValueView::from_str(value)});
      src_size += key.size() + value.size();
    }
    std::sort(src_edits.begin(), src_edits.end(), KeyOrder{});

    // Pack a block.
    //
    std::array<char, kBlockSize * 2> block_buffer;
    block_buffer.fill('!');
    auto dst_buffer = MutableBuffer{block_buffer.data(), kBlockSize};

    StatusOr<std::vector<EditView>::const_iterator> consumed_src_end =
        turtle_kv::pack_leaf_block(src_edits, dst_buffer);

    ASSERT_TRUE(consumed_src_end.ok());

    for (usize i = kBlockSize; i < kBlockSize * 2; ++i) {
      ASSERT_EQ(block_buffer[i], '!') << BATT_INSPECT(i);
    }

    const usize packed_count = *consumed_src_end - src_edits.begin();
    const auto& packed_block = turtle_kv::PackedLeafBlock::view_of(dst_buffer);

    ASSERT_EQ(packed_block.shared_prefix_size.value(), prefix.size());

    usize found_count = 0;
    for (const EditView& src_edit : src_edits) {
      auto* found_ptr = packed_block.find_key(get_key(src_edit));
      if (found_count < packed_count) {
        ASSERT_NE(found_ptr, nullptr)
            << BATT_INSPECT(src_edit) << BATT_INSPECT(found_count) << BATT_INSPECT(packed_count);
        ++found_count;

        ASSERT_EQ(get_key(*found_ptr), get_key(src_edit));
        ASSERT_EQ(get_value(*found_ptr), get_value(src_edit));
      } else {
        ASSERT_EQ(found_ptr, nullptr);
      }
    }

    for (usize i = 0; i < kNumNotFoundQueries; ++i) {
      std::string_view key;
      for (;;) {
        key = random_str(rng,
                         pick_key_size,
                         kMinKeySize + prefix.size(),
                         kMaxKeySize + prefix.size(),
                         strings);
        if (!used_keys.count(key)) {
          break;
        }
      }

      ASSERT_EQ(packed_block.find_key(key), nullptr);
    }

    for (usize i = 0; i < kNumLowerBoundQueries; ++i) {
      std::string_view key = (i % 2) ? random_str(rng,
                                                  pick_key_size,
                                                  kMinKeySize + prefix.size(),
                                                  kMaxKeySize + prefix.size(),
                                                  strings)
                                     : random_str(rng,  //
                                                  pick_key_size,
                                                  kMinKeySize,
                                                  kMaxKeySize,
                                                  strings,
                                                  prefix);

      const auto expected_iter =
          std::lower_bound(src_edits.begin(), src_edits.end(), key, KeyOrder{});

      const usize expected_i = std::distance(src_edits.begin(), expected_iter);

      const auto actual_iter = packed_block.lower_bound(key);

      const usize actual_i = std::distance(packed_block.items_begin(), actual_iter);

      if (expected_i >= packed_count) {
        ASSERT_EQ(actual_i, packed_count);
      } else {
        ASSERT_EQ(actual_i, expected_i);
      }
    }
  }
}

}  // namespace
