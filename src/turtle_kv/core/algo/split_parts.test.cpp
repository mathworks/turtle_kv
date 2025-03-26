#include <turtle_kv/core/algo/split_parts.hpp>
//
#include <turtle_kv/core/algo/split_parts.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/import/env.hpp>

#include <batteries/algo/parallel_running_total.hpp>

#include <random>
#include <vector>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::getenv_as;
using turtle_kv::MaxItemSize;
using turtle_kv::MaxPartSize;
using turtle_kv::MinPartSize;
using turtle_kv::SmallVec;
using turtle_kv::SmallVecBase;
using turtle_kv::split_parts;
using turtle_kv::SplitParts;

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;

constexpr usize kMinItemSize = 1;
constexpr usize kMaxBaseSize = 19;

std::vector<usize> make_totals(usize n_items, usize kMaxItemSize, std::default_random_engine& rng)
{
  std::uniform_int_distribution<usize> pick_base_size{0, kMaxBaseSize};
  std::vector<usize> totals{pick_base_size(rng)};
  usize total_sum = totals.back();
  std::uniform_int_distribution<usize> pick_item_size(kMinItemSize, kMaxItemSize);
  for (usize i = 0; i < n_items; ++i) {
    total_sum += pick_item_size(rng);
    totals.emplace_back(total_sum);
  }
  return totals;
}

TEST(SplitParts, Randomized)
{
  bool extra_testing = getenv_as<int>("TURTLE_KV_EXTRA_TESTING").value_or(0);

  for (MaxPartSize kMaxPartSize{24}; kMaxPartSize <= 36;
       kMaxPartSize = MaxPartSize{kMaxPartSize + 1u}) {
    const MinPartSize kMinPartSize{kMaxPartSize / 3};
    const MaxItemSize kMaxItemSize{kMinPartSize / 2};

    std::default_random_engine rng{1};

    // Split out edge case: 0 items.
    //
    EXPECT_THAT(split_parts(make_totals(/*n=*/0, kMaxItemSize, rng),
                            kMinPartSize,
                            kMaxPartSize,
                            kMaxItemSize)
                    .offsets,
                ElementsAre(0));

    // Split out edge case: all items will definitely fit into one part.
    //
    {
      usize max_tested_total = 0;
      for (usize n = 1; n <= kMaxPartSize / kMaxItemSize; ++n) {
        for (usize i = 0; i < (extra_testing ? 1000 * 1000 : 1000); ++i) {
          std::vector<usize> totals;
          if (i == 0) {
            totals.emplace_back(0);
            for (usize j = 0; j < n; ++j) {
              totals.emplace_back((j + 1) * kMaxItemSize);
            }
          } else {
            totals = make_totals(n, kMaxItemSize, rng);
          }
          max_tested_total = std::max(totals.back(), max_tested_total);
          EXPECT_THAT(split_parts(totals, kMinPartSize, kMaxPartSize, kMaxItemSize).offsets,
                      ElementsAre(0, n))
              << std::endl
              << "totals=" << batt::dump_range(totals);
        }
      }
      if (extra_testing) {
        EXPECT_EQ(max_tested_total, kMaxBaseSize + kMaxPartSize - kMaxPartSize % kMaxItemSize)
            << BATT_INSPECT(kMaxPartSize) << BATT_INSPECT(kMaxItemSize)
            << BATT_INSPECT(kMaxPartSize / kMaxItemSize);
      }
    }

    std::uniform_int_distribution<usize> pick_n_items(
        (kMinPartSize + kMinItemSize - 1) / kMinItemSize,
        179);

    for (usize loop = 0; loop < (extra_testing ? 10 * 1000 * 1000 : 100 * 1000); ++loop) {
      VLOG_EVERY_N(1, (extra_testing ? 100 * 1000 : 10 * 1000))
          << BATT_INSPECT(loop) << BATT_INSPECT(kMaxPartSize) << BATT_INSPECT(kMinPartSize)
          << BATT_INSPECT(kMaxItemSize);
      usize n = pick_n_items(rng);

      std::vector<usize> totals = make_totals(n, kMaxItemSize, rng);
      std::vector<usize> item_sizes;
      {
        usize prev_total = 0;
        bool first = true;
        for (usize t : totals) {
          if (first) {
            first = false;
          } else {
            item_sizes.emplace_back(t - prev_total);
          }
          prev_total = t;
        }
      }

      SplitParts result = split_parts(totals, kMinPartSize, kMaxPartSize, kMaxItemSize);
      auto& parts = result.offsets;

      // Calculate the minimum number of parts that can be generated without respecting the minimum
      // part size. This can be obtained by the greedy approach of going in order taking as much as
      // possible each time.
      //
      usize expected_n_parts = 1;
      usize part_total = 0;
      VLOG(2);
      {
        bool first = true;
        usize prev_t = 0;
        for (usize t : totals) {
          if (first) {
            first = false;
          } else {
            if ((t - prev_t) + part_total > kMaxPartSize) {
              ++expected_n_parts;
              VLOG(2) << BATT_INSPECT(part_total);
              part_total = 0;
            }
            part_total += (t - prev_t);
          }
          prev_t = t;
        }
      }

      // Calculate the sizes of the generated parts.
      //
      std::vector<usize> part_sizes;
      usize actual_max_part_size = 0;
      usize actual_min_part_size = kMaxPartSize;
      for (usize j = 0; j < parts.size() - 1; ++j) {
        const usize part_begin_index = parts[j];
        const usize part_end_index = parts[j + 1];
        const usize part_size = totals[part_end_index] - totals[part_begin_index];
        actual_max_part_size = std::max(actual_max_part_size, part_size);
        actual_min_part_size = std::min(actual_min_part_size, part_size);
        part_sizes.emplace_back(part_size);
      }

      const auto debug_info = [&](std::ostream& out) {
        const usize average = totals.back() / (parts.size() - 1);
        out << BATT_INSPECT(kMinPartSize) << BATT_INSPECT(kMaxPartSize)
            << BATT_INSPECT(kMaxItemSize) << std::endl
            << BATT_INSPECT(actual_min_part_size) << BATT_INSPECT(actual_max_part_size) << std::endl
            << BATT_INSPECT(actual_max_part_size - actual_min_part_size) << BATT_INSPECT(average)
            << std::endl
            << "totals=" << batt::dump_range(totals) << std::endl
            << "parts=" << batt::dump_range(parts) << std::endl
            << "part_sizes=" << batt::dump_range(part_sizes) << std::endl
            << "item_sizes=" << batt::dump_range(item_sizes) << std::endl;
      };

      for (usize part_size : part_sizes) {
        if (totals.back() >= kMinPartSize) {
          EXPECT_GE(part_size, kMinPartSize) << debug_info;
        }
        EXPECT_LE(part_size, kMaxPartSize) << debug_info;
      }

      EXPECT_EQ(parts.size(), expected_n_parts + 1u)
          << batt::dump_range(parts) << BATT_INSPECT(n) << debug_info;
      EXPECT_LE(actual_max_part_size - actual_min_part_size, kMaxItemSize * 2) << debug_info;

      ASSERT_EQ(result.size(), part_sizes.size());
      for (usize i = 0; i < result.size(); ++i) {
        usize z = totals[result[i].upper_bound] - totals[result[i].lower_bound];
        EXPECT_EQ(z, part_sizes[i]);

        auto part_totals = batt::slice_range(totals, push_back(result[i], 1));
        EXPECT_EQ(part_totals.back() - part_totals.front(), part_sizes[i])
            << BATT_INSPECT(result[i]) << BATT_INSPECT(totals[result[i].lower_bound])
            << BATT_INSPECT(totals[result[i].upper_bound]);
      }
    }
  }
}

}  // namespace
