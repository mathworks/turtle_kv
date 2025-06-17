#include <turtle_kv/util/art.hpp>
//
#include <turtle_kv/util/art.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/import/metrics.hpp>

#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace batt::int_types;

using turtle_kv::ART;
using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::testing::RandomStringGenerator;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, PutContainsTest)
{
  const usize num_keys = 1000;

  std::vector<std::string> keys;
  std::unordered_set<std::string_view> inserted;
  {
    std::default_random_engine rng{/*seed=*/1};
    RandomStringGenerator generate_key;

    for (usize i = 0; i < num_keys; ++i) {
      keys.emplace_back(generate_key(rng));
    }
  }

  ART index;

  usize i = 0;
  for (const std::string& key : keys) {
    if (inserted.count(key)) {
      continue;
    }

    inserted.emplace(key);

    EXPECT_FALSE(index.contains(key));

    index.put(key);

    EXPECT_TRUE(index.contains(key)) << BATT_INSPECT(i) << BATT_INSPECT_STR(key);

    ++i;
  }

  for (const std::string& key : keys) {
    EXPECT_TRUE(index.contains(key));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, SingleThreadTest)
{
  for (const usize num_keys : {1e5, 1e6, 1e7}) {
    std::vector<std::string> keys;
    {
      std::default_random_engine rng{/*seed=*/1};
      RandomStringGenerator generate_key;

      for (usize i = 0; i < num_keys; ++i) {
        keys.emplace_back(generate_key(rng));
      }
    }

    LatencyMetric insert_latency;
    for (usize trial = 0; trial < 3; ++trial) {
      ART index;
      {
        LatencyTimer timer{insert_latency, num_keys};
        for (std::string_view s : keys) {
          index.put(s);
        }
      }
    }
    std::cerr << BATT_INSPECT(insert_latency) << std::endl;
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

usize slow_index_of(u8 key_byte, const std::array<u8, 4>& keys)
{
  for (usize i = 0; i < keys.size(); ++i) {
    if (keys[i] == key_byte) {
      return i;
    }
  }
  return 7;
}

usize slow_index_of(u8 key_byte, const std::array<u8, 16>& keys)
{
  for (usize i = 0; i < keys.size(); ++i) {
    if (keys[i] == key_byte) {
      return i;
    }
  }
  return 31;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, SimdSearch)
{
  using turtle_kv::index_of;

  std::uniform_int_distribution<u8> pick_byte{0, 255};

  // array<u8, 4> case
  {
    // hand-written test cases
    {
      std::array<u8, 4> keys = {2, 0, 3, 1};

      EXPECT_EQ(index_of(0, keys), 1);
      EXPECT_EQ(index_of(1, keys), 3);
      EXPECT_EQ(index_of(2, keys), 0);
      EXPECT_EQ(index_of(3, keys), 2);
      EXPECT_EQ(index_of(4, keys) & 4, 4);
      EXPECT_EQ(index_of(50, keys) & 4, 4);
      EXPECT_EQ(index_of(99, keys) & 4, 4);
    }

    // randomly generated cases
    //
    std::default_random_engine rng{/*seed=*/1};

    for (usize i = 0; i < 1000; ++i) {
      std::array<u8, 4> keys;
      keys.fill(0);
      for (u8& k : keys) {
        k = pick_byte(rng);
      }
      for (u16 p = 0; p < 256; ++p) {
        EXPECT_EQ(index_of((u8)p, keys) & 4, slow_index_of((u8)p, keys) & 4);
      }
    }
  }

  // array<u8, 16> case
  {
    std::default_random_engine rng{/*seed=*/1};

    for (usize i = 0; i < 100000; ++i) {
      std::array<u8, 16> keys;
      keys.fill(0);
      for (u8& k : keys) {
        k = pick_byte(rng);
      }
      for (u16 p = 0; p < 256; ++p) {
        EXPECT_EQ(index_of((u8)p, keys), slow_index_of((u8)p, keys));
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, MultiThreadTest)
{
  const int n_rounds = 3;

  for (usize n_threads = 1; n_threads < std::thread::hardware_concurrency(); ++n_threads) {
    std::atomic<int> round{-1};
    std::atomic<int> pending{0};
    std::atomic<ART*> p_index{nullptr};
    std::atomic<const std::string*> p_keys{nullptr};
    std::atomic<usize> n_keys{0};
    std::vector<std::thread> threads;
    for (usize i = 0; i < n_threads; ++i) {
      threads.emplace_back([&, i] {
        for (int r = 0; r < n_rounds * 3; ++r) {
          VLOG(1) << "thread " << i << " waiting for round " << r;
          while (round.load() < r) {
            continue;
          }
          BATT_CHECK_EQ(r, round.load());

          VLOG(1) << "thread " << i << " starting round " << r;
          ART& index = *p_index.load();
          const std::string* keys = p_keys.load();

          for (usize j = i; j < n_keys; j += n_threads) {
            index.put(keys[j]);
          }

          pending.fetch_sub(1);
          VLOG(1) << "thread " << i << " finished round " << r;
        }
      });
    }
    auto on_scope_exit = batt::finally([&] {
      for (std::thread& t : threads) {
        t.join();
      }
      std::cerr << std::endl;
    });

    usize size_i = 0;
    for (const usize num_keys : {3e5, 2e5, 1e5}) {
      std::vector<std::string> keys;
      {
        std::default_random_engine rng{/*seed=*/1};
        RandomStringGenerator generate_key;

        for (usize i = 0; i < num_keys; ++i) {
          keys.emplace_back(generate_key(rng));
        }
      }

      n_keys.store(num_keys);
      p_keys.store(keys.data());
      auto on_scope_exit2 = batt::finally([&] {
        n_keys.store(0);
        p_keys.store(nullptr);
      });

      LatencyMetric insert_latency;
      for (int r = 0; r < n_rounds; ++r) {
        ART index;

        pending.store(n_threads);
        p_index.store(&index);
        auto on_scope_exit3 = batt::finally([&] {
          p_index.store(nullptr);
        });

        {
          LatencyTimer timer{insert_latency, num_keys};

          const int next_round = r + size_i * n_rounds;
          VLOG(1) << "starting round: " << next_round;
          round.store(next_round);
          while (pending.load() > 0) {
            std::this_thread::yield();
          }
        }
      }
      std::cerr << BATT_INSPECT(n_threads) << BATT_INSPECT(insert_latency) << std::endl;
      ++size_i;
    }
  }
}

}  // namespace
