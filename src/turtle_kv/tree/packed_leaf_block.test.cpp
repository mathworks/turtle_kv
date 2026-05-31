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
using turtle_kv::KeyView;
using turtle_kv::ValueView;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view random_str(std::default_random_engine& rng,
                            usize min_size,
                            usize max_size,
                            StableStringStore& strings) noexcept
{
  std::uniform_int_distribution<usize> pick_size{min_size, max_size};
  std::uniform_int_distribution<i8> pick_char{'a', 'z'};

  const usize n = pick_size(rng);
  MutableBuffer buf = strings.allocate(n);
  char* chars = static_cast<char*>(buf.data());

  for (usize i = 0; i < n; ++i, ++chars) {
    *chars = pick_char(rng);
  }

  return std::string_view{static_cast<const char*>(buf.data()), buf.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(TreePackedLeafBlockTest, Random)
{
  const usize kNumSeeds = 1000;
  const usize kMinKeySize = 4;
  const usize kMaxKeySize = 48;
  const usize kMinValueSize = 0;
  const usize kMaxValueSize = 200;
  const usize kBlockSize = 8192;

  for (usize seed = 0; seed < kNumSeeds; ++seed) {
    std::default_random_engine rng{seed};

    StableStringStore strings;
    std::unordered_set<std::string_view> used_keys;
    std::vector<EditView> src_edits;

    // Generate enough random edits to fill a block.
    //
    usize src_size = 0;
    while (src_size < kBlockSize) {
      std::string_view key = random_str(rng, kMinKeySize, kMaxKeySize, strings);
      if (used_keys.count(key)) {
        continue;
      }
      used_keys.insert(key);
      std::string_view value = random_str(rng, kMinValueSize, kMaxValueSize, strings);
      EditView edit{key, ValueView::from_str(value)};
      src_edits.push_back(edit);
      src_size += key.size() + value.size();
    }
    std::sort(src_edits.begin(), src_edits.end(), turtle_kv::KeyOrder{});

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

    usize found_count = 0;
    for (const EditView& src_edit : src_edits) {
      auto* found_ptr = packed_block.find_key(get_key(src_edit));
      if (found_count < packed_count) {
        ASSERT_NE(found_ptr, nullptr)
            << BATT_INSPECT(src_edit) << BATT_INSPECT(found_count) << BATT_INSPECT(packed_count);
        ++found_count;
      } else {
        ASSERT_EQ(found_ptr, nullptr);
      }
    }
  }
}

}  // namespace
