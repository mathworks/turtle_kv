#include <turtle_kv/core/testing/generate.hpp>
//
#include <turtle_kv/core/testing/generate.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using turtle_kv::DecayToItem;
using turtle_kv::ItemView;
using turtle_kv::KeyOrder;
using turtle_kv::testing::RandomResultSetGenerator;

template <bool kDecayToItems>
using ResultSet = turtle_kv::MergeCompactor::ResultSet<kDecayToItems>;

TEST(GenerateTest, Test)
{
  std::default_random_engine rng{1};

  RandomResultSetGenerator g;
  llfs::StableStringStore store;

  g.set_size(200);

  ResultSet<true> result_set = g(DecayToItem<true>{}, rng, store);

  EXPECT_TRUE(std::is_sorted(result_set.get().begin(), result_set.get().end(), KeyOrder{}));
  EXPECT_EQ(result_set.get().size(), 200u);
}

}  // namespace
