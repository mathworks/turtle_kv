#include <turtle_kv/core/testing/generate.hpp>
//
#include <turtle_kv/core/testing/generate.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using batt::int_types::usize;

using turtle_kv::DecayToItem;
using turtle_kv::ItemView;
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::StatusOr;
using turtle_kv::ValueView;
using turtle_kv::testing::RandomResultSetGenerator;

template <bool kDecayToItems>
using ResultSet = turtle_kv::MergeCompactor::ResultSet<kDecayToItems>;

TEST(GenerateTest, Test)
{
  std::default_random_engine rng{1};

  RandomResultSetGenerator g;
  llfs::StableStringStore store;

  g.set_size(200);

  std::vector<KeyView> to_delete;
  ResultSet<true> result_set = g(DecayToItem<true>{}, rng, store, to_delete);

  EXPECT_TRUE(std::is_sorted(result_set.get().begin(), result_set.get().end(), KeyOrder{}));
  EXPECT_EQ(result_set.get().size(), 200u);

  auto result_set_slice = result_set.get();
  usize i = 0;
  for (const ItemView& edit : result_set_slice) {
    if (i % 2) {
      to_delete.emplace_back(edit.key);
    }
    ++i;
  }

  ResultSet<false> result_set_with_deletes = g(DecayToItem<false>{}, rng, store, to_delete);
  for (const KeyView& deleted_key : to_delete) {
    StatusOr<ValueView> deleted_value = result_set_with_deletes.find_key(deleted_key);
    EXPECT_TRUE(deleted_value.ok());
    EXPECT_EQ(*deleted_value, ValueView::deleted());
  }
  EXPECT_EQ(to_delete.size(), result_set_with_deletes.size() / 2);
}

}  // namespace