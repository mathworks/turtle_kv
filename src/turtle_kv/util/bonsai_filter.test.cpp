#include <turtle_kv/util/bonsai_filter.hpp>
//
#include <turtle_kv/util/bonsai_filter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/util/bonsai_filter2.hpp>

#include <turtle_kv/core/testing/generate.hpp>

#include <llfs/stable_string_store.hpp>

#include <cmath>
#include <map>
#include <random>
#include <set>

namespace {

using namespace batt::int_types;

using llfs::StableStringStore;
using turtle_kv::BonsaiFilter;
using turtle_kv::BonsaiFilter2;
using turtle_kv::testing::RandomStringGenerator;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct TrieNode {
  std::map<char, std::unique_ptr<TrieNode>> branch;

  void insert(std::string_view s, usize& node_count)
  {
    if (s.empty()) {
      return;
    }

    std::unique_ptr<TrieNode>& branch_ptr = this->branch[s[0]];
    if (branch_ptr == nullptr) {
      node_count += 1;
      branch_ptr = std::make_unique<TrieNode>();
    }

    branch_ptr->insert(s.substr(1), node_count);
  }
};

template <typename BonsaiT>
void run_bonsai_filter_test(const usize num_buckets, const usize num_keys)
{
  BonsaiT index{num_buckets};

  std::default_random_engine rng{/*seed=*/1};

  std::uniform_int_distribution<int> percent{0, 10000};
  RandomStringGenerator generate_key;

  std::cout << index.dump_config() << std::endl;

  std::set<std::string> true_keys;

  // Maintain a Trie to estimate the minimum theoretical size for the collection.
  //
  usize trie_node_count = 0;
  TrieNode trie;

  for (usize i = 0; i < num_keys; ++i) {
    if (i == 14863) {
      // BonsaiFilter2::verbose() = true;
    }

    std::string key = generate_key(rng);

    true_keys.emplace(key);
    index.put(key);
    trie.insert(key, trie_node_count);

    if (percent(rng) < 1000 || i == num_keys - 1) {
      std::set<std::string> found_keys;

      double false_positives = 0;
      typename BonsaiT::ScanStats scan_stats;

      index.scan_all(
          [&](std::string_view emit_view) {
            std::string emit_str{emit_view};
            if (true_keys.count(emit_str) == 1) {
              found_keys.emplace(emit_str);
              if (BonsaiFilter2::verbose()) {
                std::cerr << "  (added)" << std::endl;
              }
            } else {
              false_positives += 1;
              if (BonsaiFilter2::verbose()) {
                std::cerr << "  " << BATT_INSPECT(false_positives) << std::endl;
              }
            }
          },
          scan_stats);

      EXPECT_EQ(found_keys, true_keys);

      BATT_CHECK_EQ(found_keys, true_keys);

      const usize emitted = found_keys.size() + false_positives;
      const usize n_log_n = (usize)((double)(i + 1) * std::log2((double)(i + 1)));
      double ratio = (double)emitted / (double)n_log_n;

      std::cout << BATT_INSPECT(i) << BATT_INSPECT(false_positives)
                << " (rate=" << (false_positives / (double)emitted) << ")" << BATT_INSPECT(emitted)
                << BATT_INSPECT(n_log_n) << BATT_INSPECT(ratio) << " " << scan_stats << std::endl;
    }
  }

  double opt_bytes_per_key = (double)trie_node_count / (double)true_keys.size();
  double used_bytes_per_key = (double)index.byte_size() / (double)true_keys.size();

  std::cout << BATT_INSPECT(trie_node_count) << BATT_INSPECT(opt_bytes_per_key)
            << BATT_INSPECT(used_bytes_per_key) << std::endl;
}

TEST(BonsaiFilterTest, Test)
{
  // BonsaiFilter2::verbose() = true;
  //  run_bonsai_filter_test<BonsaiFilter2>(65536 * 24 / 2, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 16384, 1>>(32, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 8192, 1>>(64, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 4096, 1>>(128, 65536);  // BEST?

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 2048, 1>>(255, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 1024, 1>>(510, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 512, 1>>(1016, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 256, 1>>(2016, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 128, 1>>(3972, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 64, 1>>(7710, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 32, 1>>(14564, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 16, 1>>(26214, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 8, 1>>(43691, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 4096, 1024>>(64, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 2048, 512>>(128, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 1024, 256>>(256, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 512, 128>>(512, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 256, 64>>(1024, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 128, 32>>(2048, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 64, 16>>(4096, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 32, 8>>(8192, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 16, 4>>(16384, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 8, 2>>(32768, 65536);
  run_bonsai_filter_test<BonsaiFilter<256, 4, 1>>(65536, 65536);

  std::cout << std::endl;

  run_bonsai_filter_test<BonsaiFilter<256, 32, 16>>(5461, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 16, 8>>(10922, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 8, 4>>(21845, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 4, 2>>(43690, 65536);
  // run_bonsai_filter_test<BonsaiFilter<256, 2, 1>>(87381, 65536);

  std::cout << std::endl;
}

}  // namespace
