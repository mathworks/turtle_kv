#include <turtle_kv/util/art.hpp>
//
#include <turtle_kv/util/art.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/import/metrics.hpp>

#include <batteries/stream_util.hpp>

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
  const usize num_keys = 1e5;
  const usize num_scans = 10000;
  const usize max_scan_length = 100;

  std::default_random_engine rng{/*seed=*/1};
  RandomStringGenerator generate_key;
  std::uniform_int_distribution<usize> pick_scan_length{1, max_scan_length};

  std::vector<std::string> keys;
  std::unordered_set<std::string_view> inserted;

  for (usize i = 0; i < num_keys; ++i) {
    keys.emplace_back(generate_key(rng));
  }

  ART index;

  usize i = 0;
  for (const std::string& key : keys) {
    if (inserted.count(key)) {
      continue;
    }

    inserted.emplace(key);

    EXPECT_FALSE(index.contains(key));

    index.insert(key);

    EXPECT_TRUE(index.contains(key)) << BATT_INSPECT(i) << BATT_INSPECT_STR(key);

    ++i;
  }

  for (const std::string& key : keys) {
    EXPECT_TRUE(index.contains(key));
  }

  LatencyMetric sort_latency;

  std::vector<std::string_view> sorted_keys;
  for (const std::string& key : keys) {
    sorted_keys.emplace_back(key);
  }
  {
    LatencyTimer timer{sort_latency, sorted_keys.size()};
    std::sort(sorted_keys.begin(), sorted_keys.end());
  }

  LatencyMetric scan_latency;
  LatencyMetric scanner_latency;
  LatencyMetric scanner_nosync_latency;

  for (usize i = 0; i < num_scans; ++i) {
    const std::string lower_bound_key = generate_key(rng);
    const usize scan_length = pick_scan_length(rng);

    std::vector<std::string> expected_result;
    for (auto iter = std::lower_bound(sorted_keys.begin(), sorted_keys.end(), lower_bound_key);
         iter != sorted_keys.end() && expected_result.size() < scan_length;
         ++iter) {
      expected_result.emplace_back(*iter);
    }

    for (usize j = 0; j < 3; ++j) {
      {
        std::vector<std::string> actual_result;
        {
          LatencyTimer timer{scan_latency};
          index.scan(lower_bound_key, [&actual_result, scan_length](const std::string_view& key) {
            actual_result.emplace_back(key);
            return actual_result.size() < scan_length;
          });
        }

        EXPECT_EQ(expected_result.size(), actual_result.size());

        ASSERT_EQ(expected_result, actual_result)
            << BATT_INSPECT_STR(lower_bound_key) << BATT_INSPECT(i);
      }

      {
        std::vector<std::string> actual_result;
        {
          LatencyTimer timer{scanner_latency};

          ART::Scanner<ART::Synchronized::kTrue> scanner{index, lower_bound_key};

          while (!scanner.is_done() && actual_result.size() < scan_length) {
            actual_result.emplace_back(scanner.get_key());
            scanner.advance();
          }
        }

        EXPECT_EQ(expected_result.size(), actual_result.size());

        ASSERT_EQ(expected_result, actual_result)
            << BATT_INSPECT_STR(lower_bound_key) << BATT_INSPECT(i);
      }

      {
        std::vector<std::string> actual_result;
        {
          LatencyTimer timer{scanner_nosync_latency};

          ART::Scanner<ART::Synchronized::kFalse> scanner{index, lower_bound_key};

          while (!scanner.is_done() && actual_result.size() < scan_length) {
            actual_result.emplace_back(scanner.get_key());
            scanner.advance();
          }
        }

        EXPECT_EQ(expected_result.size(), actual_result.size());

        ASSERT_EQ(expected_result, actual_result)
            << BATT_INSPECT_STR(lower_bound_key) << BATT_INSPECT(i);
      }
    }
  }

  LatencyMetric item_latency;
  {
    usize count = 0;
    {
      LatencyTimer timer{item_latency, num_keys};
      ART::Scanner<ART::Synchronized::kTrue> scanner{index, std::string_view{}};
      while (!scanner.is_done()) {
        ++count;
        scanner.advance();
      }
    }
    EXPECT_EQ(count, num_keys);
  }

  LatencyMetric item_nosync_latency;
  {
    usize count = 0;
    {
      LatencyTimer timer{item_nosync_latency, num_keys};
      ART::Scanner<ART::Synchronized::kFalse> scanner{index, std::string_view{}};
      while (!scanner.is_done()) {
        ++count;
        scanner.advance();
      }
    }
    EXPECT_EQ(count, num_keys);
  }

  std::cerr << BATT_INSPECT(scan_latency) << std::endl
            << BATT_INSPECT(scanner_latency) << std::endl
            << BATT_INSPECT(scanner_nosync_latency) << std::endl
            << BATT_INSPECT(item_latency) << std::endl
            << BATT_INSPECT(item_nosync_latency) << std::endl
            << BATT_INSPECT(sort_latency) << std::endl;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, WideKeySet)
{
  std::vector<std::string> keys;
  for (u16 first = 0; first < 256; ++first) {
    for (u16 second = 0; second < 256; ++second) {
      char data[2] = {(char)first, (char)second};
      keys.emplace_back(data, 2);
    }
  }

  ART index;

  for (const std::string& key : keys) {
    EXPECT_FALSE(index.contains(key));
    index.insert(key);
    EXPECT_TRUE(index.contains(key));
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
          index.insert(s);
        }
      }
      for (const std::string& key : keys) {
        ASSERT_TRUE(index.contains(key));
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

  for (usize n_threads = 1; n_threads <= std::thread::hardware_concurrency(); ++n_threads) {
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
            index.insert(keys[j]);
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

        usize found_count = 0;
        for (const std::string& key : keys) {
          ASSERT_TRUE(index.contains(key)) << BATT_INSPECT_STR(key) << BATT_INSPECT(found_count);
          ++found_count;
        }
      }
      std::cerr << BATT_INSPECT(n_threads) << BATT_INSPECT(insert_latency) << std::endl;
      ++size_i;
    }

    if (n_threads >= 4) {
      n_threads += 1;
      if (n_threads >= 8) {
        n_threads += 2;
        if (n_threads >= 16) {
          n_threads += 4;
        }
      }
    }
  }
}

}  // namespace
