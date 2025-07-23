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

using turtle_kv::LatencyMetric;
using turtle_kv::LatencyTimer;
using turtle_kv::None;
using turtle_kv::OkStatus;
using turtle_kv::Optional;
using turtle_kv::Status;
using turtle_kv::testing::RandomStringGenerator;

using ART = turtle_kv::ART<void>;

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

  {
    ART::Scanner<ART::Synchronized::kFalse> scanner{index, ""};
    EXPECT_TRUE(scanner.is_done());
  }

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
struct TestStringViewInserter {
  const std::string_view& src;

  Status insert_new(void* dst) const
  {
    new (dst) std::string_view{this->src};
    return OkStatus();
  }

  Status update_existing(std::string_view* dst) const
  {
    *dst = this->src;
    return OkStatus();
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct TestIntInserter {
  usize src;

  Status insert_new(void* dst) const
  {
    *((usize*)dst) = this->src;
    return OkStatus();
  }

  Status update_existing(usize* dst) const
  {
    *dst = this->src;
    return OkStatus();
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void insert_key(turtle_kv::ART<void>& art, const std::string& key)
{
  art.insert(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void insert_key(turtle_kv::ART<std::string_view>& art, const std::string& key)
{
  BATT_CHECK_OK(art.insert(key, TestStringViewInserter{.src = key}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void insert_key(turtle_kv::ART<usize>& art, const std::string& key)
{
  BATT_CHECK_OK(art.insert(key, TestIntInserter{.src = *((const usize*)key.data() + 4)}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
void run_benchmark_test()
{
  const int n_rounds = 3;
  const int n_stages_per_round = 2;  // insert and query

  const std::array<usize, 3> data_set_sizes = {
      300 * 1000,
      200 * 1000,
      100 * 1000,
  };

  for (usize n_threads = 1; n_threads <= std::thread::hardware_concurrency(); ++n_threads) {
    std::atomic<int> round{-1};
    std::atomic<int> pending{0};
    std::atomic<turtle_kv::ART<ValueT>*> p_index{nullptr};
    std::atomic<const std::string*> p_keys{nullptr};
    std::atomic<usize> n_keys{0};
    std::vector<std::thread> threads;

    for (usize i = 0; i < n_threads; ++i) {
      threads.emplace_back([&, i] {
        std::default_random_engine rng{std::random_device{}()};

        const int n_loops = n_rounds * (int)data_set_sizes.size() * n_stages_per_round;

        for (int r = 0; r < n_loops; ++r) {
          VLOG(1) << "thread " << i << " waiting for round " << r;
          while (round.load() < r) {
            continue;
          }
          BATT_CHECK_EQ(r, round.load());

          VLOG(1) << "thread " << i << " starting round " << r;
          turtle_kv::ART<ValueT>& index = *p_index.load();
          const std::string* keys = p_keys.load();
          const usize n = n_keys.load();

          if ((r % 2) == 0) {
            for (usize j = i; j < n; j += n_threads) {
              insert_key(index, keys[j]);
            }
          } else {
            std::uniform_int_distribution<usize> pick_i{0, n - 1};
            for (usize j = i; j < n; j += n_threads) {
              const usize k = pick_i(rng);
              BATT_CHECK(index.contains(keys[k]));
            }
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
    for (const usize num_keys : data_set_sizes) {
      //----- --- -- -  -  -   -
      // Generate random key set.
      //
      std::vector<std::string> keys;
      {
        std::default_random_engine rng{/*seed=*/1};
        RandomStringGenerator generate_key;

        for (usize i = 0; i < num_keys; ++i) {
          keys.emplace_back(generate_key(rng));
        }
      }

      //----- --- -- -  -  -   -
      // Initialize the thread-shared state.
      //
      n_keys.store(num_keys);
      p_keys.store(keys.data());
      auto on_scope_exit2 = batt::finally([&] {
        n_keys.store(0);
        p_keys.store(nullptr);
      });

      //----- --- -- -  -  -   -
      // Create a metric for each latency we want to measure.
      //
      LatencyMetric insert_latency;
      LatencyMetric st_query_latency;
      LatencyMetric mt_query_latency;

      //----- --- -- -  -  -   -
      // Run the benchmark repeatedly `n_rounds` times.
      //
      for (int r = 0; r < n_rounds; ++r) {
        turtle_kv::ART<ValueT> index;

        // Set `pending` and `p_index`; the threads will not start working until `round` is updated.
        //
        pending.store(n_threads);
        p_index.store(&index);
        auto on_scope_exit3 = batt::finally([&] {
          p_index.store(nullptr);
        });

        // Stage 0: inserts
        {
          LatencyTimer timer{insert_latency, num_keys};

          const int next_round = (r + size_i * n_rounds) * 2;

          //----- --- -- -  -  -   -
          // Ready, set, go!
          //
          round.store(next_round);
          //----- --- -- -  -  -   -

          while (pending.load() > 0) {
            std::this_thread::yield();
          }
        }

        pending.store(n_threads);

        // Stage 1: queries
        {
          LatencyTimer timer{mt_query_latency, num_keys};

          const int next_round = (r + size_i * n_rounds) * 2 + 1;

          //----- --- -- -  -  -   -
          // Ready, set, go!
          //
          round.store(next_round);
          //----- --- -- -  -  -   -

          while (pending.load() > 0) {
            std::this_thread::yield();
          }
        }

        //----- --- -- -  -  -   -
        // After multi-threaded part of this round is done.
        //
        usize found_count = 0;
        auto start_time = std::chrono::steady_clock::now();
        for (const std::string& key : keys) {
          ASSERT_TRUE(index.contains(key)) << BATT_INSPECT_STR(key) << BATT_INSPECT(found_count);
          ++found_count;
        }
        st_query_latency.update(start_time, found_count);
        ASSERT_EQ(found_count, keys.size());
      }
      std::cerr << "threads: " << n_threads << " N=" << num_keys << std::endl
                << "      put: " << insert_latency << std::endl
                << "   st_get: " << st_query_latency << std::endl
                << "   mt_get: " << mt_query_latency << std::endl;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, MultiThreadTest_Void)
{
  run_benchmark_test<void>();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, MultiThreadTest_StringView)
{
  run_benchmark_test<std::string_view>();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, MultiThreadTest_Int)
{
  run_benchmark_test<usize>();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(ArtTest, ValuePutGetScan)
{
  const usize n_keys = 100 * 1000;
  std::default_random_engine rng{std::random_device{}()};
  RandomStringGenerator generate_key;

  std::vector<std::string> keys;
  keys.resize(n_keys);

  std::unordered_set<std::string_view> unique_keys;

  for (std::string& k : keys) {
    for (;;) {
      k = generate_key(rng);
      if (!unique_keys.count(k)) {
        unique_keys.emplace(k);
        break;
      }
    }
  }

  turtle_kv::ART<std::string_view> art;

  const auto check_key_by_index = [&art, &keys](usize query_i, usize expected_i) {
    ASSERT_TRUE(art.contains(keys[query_i]));

    const std::string_view* p_value = art.unsynchronized_find(keys[query_i]);
    ASSERT_NE(p_value, nullptr);
    ASSERT_THAT(*p_value, ::testing::StrEq(keys[expected_i]));

    Optional<std::string_view> value_copy = art.find(keys[query_i]);
    ASSERT_TRUE(value_copy);
    ASSERT_THAT(*value_copy, ::testing::StrEq(keys[expected_i]));
  };

  for (usize i = 0; i < n_keys; ++i) {
    Status status = art.insert(keys[i],
                               TestStringViewInserter{
                                   .src = keys[i / 2],
                               });

    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);

    for (usize j = i - std::min<usize>(i, 25); j <= i; ++j) {
      ASSERT_NO_FATAL_FAILURE(check_key_by_index(j, j / 2));
    }
  }

  // Check all values once more to make sure nothing that was inserted earlier was messed up by a
  // later insertion or update.
  //
  for (usize i = 0; i < n_keys; ++i) {
    ASSERT_NO_FATAL_FAILURE(check_key_by_index(i, i / 2));
  }

  // Now update all keys and verify them.
  //
  for (usize i = 0; i < n_keys; ++i) {
    Status status = art.insert(keys[i],
                               TestStringViewInserter{
                                   .src = keys[i],
                               });

    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);

    for (usize j = i - std::min<usize>(i, 25); j <= i; ++j) {
      ASSERT_NO_FATAL_FAILURE(check_key_by_index(j, j));
    }
  }

  for (usize i = 0; i < n_keys; ++i) {
    ASSERT_NO_FATAL_FAILURE(check_key_by_index(i, i));
  }
}

}  // namespace
