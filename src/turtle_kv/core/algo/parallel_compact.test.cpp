#include <turtle_kv/core/algo/parallel_compact.hpp>
//
#include <turtle_kv/core/algo/parallel_compact.hpp>

#include <turtle_kv/core/algo/tuning_defaults.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/int_types.hpp>
#include <batteries/stream_util.hpp>

#include <random>
#include <vector>

namespace {

using namespace batt::int_types;

using batt::WorkContext;
using batt::WorkerPool;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct Entry {
  int key;
  int value;

  bool needs_combine() const
  {
    return true;
  }
};

inline bool operator==(const Entry& l, const Entry& r)
{
  return l.key == r.key && l.value == r.value;
}

inline std::ostream& operator<<(std::ostream& out, const Entry& t)
{
  return out << "(" << t.key << ":" << t.value << ")";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(AlgoTest_ParallelCompact, Test)
{
  using turtle_kv::Chunk;

  constexpr usize kInputSize = 1000000;

  const turtle_kv::ParallelAlgoDefaults& algo_defaults = turtle_kv::parallel_algo_defaults();

  for (usize seed = 0; seed < 100; ++seed) {
    std::default_random_engine rng{seed};
    std::uniform_int_distribution<usize> pick_group_size{1, 7};
    std::uniform_int_distribution<int> pick_value{1, 10};

    std::vector<Entry> input;
    std::vector<Entry> expected_output;
    int next_key = 1;
    while (input.size() < kInputSize) {
      usize group_size = pick_group_size(rng);
      int group_total = 0;
      for (usize j = 0; j < group_size; ++j) {
        input.emplace_back(Entry{
            .key = next_key,
            .value = pick_value(rng),
        });
        group_total = group_total * 3 + input.back().value;
      }
      expected_output.emplace_back(Entry{
          .key = next_key,
          .value = group_total,
      });
      ++next_key;
    }

    std::vector<Entry> actual_output(input.size());

    const auto group_eq = [](const Entry& l, const Entry& r) {
      return l.key == r.key;
    };

    const auto compact_fn = [](const Entry& l, const Entry& r) {
      return Entry{
          .key = l.key,
          .value = l.value * 3 + r.value,
      };
    };

    actual_output.erase(turtle_kv::parallel_compact(
                            WorkerPool::default_pool(),                                   //
                            input.cbegin(), input.cend(),                                 //
                            actual_output.begin(),                                        //
                            group_eq,                                                     //
                            compact_fn,                                                   //
                            turtle_kv::DecayToItem<false>{},                              //
                            /*min_task_size=*/algo_defaults.compact_edits.min_task_size,  //
                            /*max_tasks=*/batt::TaskCount{std::thread::hardware_concurrency()}),
                        actual_output.end());

    EXPECT_THAT(actual_output, ::testing::ContainerEq(expected_output)) << "seed=" << seed;

    std::vector<Entry> actual_output2(input.size());
    std::vector<Chunk<std::vector<Entry>::iterator>> output_chunks(input.size());

    output_chunks.erase(
        turtle_kv::parallel_compact_into_chunks(WorkerPool::default_pool(),                 //
                                                input.cbegin(), input.cend(),               //
                                                actual_output2.begin(),                     //
                                                output_chunks.begin(),                      //
                                                group_eq,                                   //
                                                compact_fn,                                 //
                                                turtle_kv::DecayToItem<false>{},            //
                                                algo_defaults.compact_edits.min_task_size,  //
                                                algo_defaults.compact_edits.max_tasks       //
                                                ),
        output_chunks.end());

    auto flattened = turtle_kv::flatten(output_chunks.begin(), std::prev(output_chunks.end()));

    std::vector<Entry> flat_output2(flattened.begin(), flattened.end());

    EXPECT_THAT(flat_output2, ::testing::ContainerEq(expected_output));
  }
}

}  // namespace
