#include <turtle_kv/util/art.hpp>
//
#include <turtle_kv/util/art.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

namespace {

using namespace batt::int_types;

using turtle_kv::ART;
using turtle_kv::testing::RandomStringGenerator;

TEST(ArtTest, Test)
{
  const usize num_keys = 1e6;

  ART index;

  std::default_random_engine rng{/*seed=*/1};

  RandomStringGenerator generate_key;

  std::set<std::string> true_keys;

  for (usize i = 0; i < num_keys; ++i) {
    std::string key = generate_key(rng);
    auto [iter, inserted] = true_keys.emplace(key);

    if (!inserted) {
      EXPECT_TRUE(index.contains(key));
    } else {
      EXPECT_FALSE(index.contains(key));
    }

    index.put(key);

    ASSERT_TRUE(index.contains(key)) << BATT_INSPECT_STR(key);
  }
}

}  // namespace
