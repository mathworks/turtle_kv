#include <turtle_kv/mem_table_ordered_index.hpp>
//
#include <turtle_kv/mem_table_ordered_index.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/finally.hpp>

#include <xxhash.h>

namespace {

using namespace batt::int_types;

TEST(MemTableOrderedIndexTest, Test)
{
  const u64 seed = 177;
  std::string str = "hello, world!";

  std::aligned_storage_t<4096, 64> state_buffer;

  XXH3_state_t* a = XXH3_createState();
  XXH3_state_t* b = XXH3_createState();
  XXH3_state_t* c = (XXH3_state_t*)&state_buffer;

  auto on_scope_exit = batt::finally([&] {
    XXH3_freeState(a);
    XXH3_freeState(b);
  });

  XXH3_64bits_reset_withSeed(a, seed);
  XXH3_64bits_reset_withSeed(b, seed);
  XXH3_64bits_reset_withSeed(c, seed);

  EXPECT_EQ(XXH3_64bits_digest(a), XXH3_64bits_digest(b));
  EXPECT_EQ(XXH3_64bits_digest(a), XXH3_64bits_digest(c));

  XXH3_64bits_update(a, str.data(), str.size());
  XXH3_64bits_update(c, str.data(), str.size());

  for (char ch : str) {
    XXH3_64bits_update(b, &ch, 1);
  }

  EXPECT_EQ(XXH3_64bits_digest(a), XXH3_64bits_digest(b));
  EXPECT_EQ(XXH3_64bits_digest(a), XXH3_64bits_digest(c));
  EXPECT_EQ(XXH3_64bits_digest(a), XXH3_64bits_withSeed(str.data(), str.size(), seed));
}

}  // namespace
