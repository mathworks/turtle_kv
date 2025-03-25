#include <turtle_kv/core/merge_compactor.hpp>
//
#include <turtle_kv/core/merge_compactor.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/import/env.hpp>

#include <batteries/stream_util.hpp>
#include <batteries/strong_typedef.hpp>

#include <iomanip>
#include <map>
#include <random>
#include <string>
#include <vector>

namespace {

using namespace batt::int_types;

using batt::as_seq;
using batt::WorkerPool;

using turtle_kv::CInterval;
using turtle_kv::EditSlice;
using turtle_kv::EditView;
using turtle_kv::getenv_as;
using turtle_kv::global_max_key;
using turtle_kv::Interval;
using turtle_kv::KeyOrder;
using turtle_kv::KeyView;
using turtle_kv::MergeCompactor;
using turtle_kv::MergeFrame;
using turtle_kv::Optional;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::ValueView;

namespace seq = turtle_kv::seq;

constexpr usize kNumKeys = 16;
constexpr usize kNumIterations = 1 * 1000;

ValueView apply_all(const std::vector<ValueView>& vs)
{
  Optional<ValueView> ans;
  std::for_each(vs.rbegin(), vs.rend(), [&ans](const ValueView& update) {
    if (!ans) {
      ans = update;
    } else {
      ans = combine(update, *ans);
    }
  });
  BATT_CHECK(ans);
  return *ans;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(MergeCompactor, Test)
{
  bool extra_testing = getenv_as<int>("TURTLE_KV_EXTRA_TESTING").value_or(0);

  LOG(INFO) << BATT_INSPECT(extra_testing);
  VLOG(1) << "verbose logging on";

  std::vector<std::string> keys;
  for (usize i = 0; i < kNumKeys; ++i) {
    keys.emplace_back(batt::to_string(std::hex, std::setw(1), i));
  }

  std::uniform_int_distribution<usize> pick_keys_in_level(0, 0xffff);
  std::uniform_int_distribution<usize> pick_level_slice_breaks(0, 0x1ffff);
  std::uniform_int_distribution<int> pick_edit_op(0, 2);

  std::default_random_engine seed_rng{0};

  for (usize i = 0; i < kNumIterations * (extra_testing ? 50 : 1); ++i) {
    for (usize num_levels : {1, 4, 8, 32, 57, 2, 11, 64, 0}) {
      std::default_random_engine rng{seed_rng()};

      std::map<std::string_view, std::vector<ValueView>> expected;
      std::vector<EditView> edits;
      std::vector<std::unique_ptr<std::string>> values;
      std::vector<std::vector<Slice<EditView>>> levels;
      {
        // IMPORTANT: make sure that pointers into `edits` will never be invalidated.
        //
        edits.reserve(64 * kNumKeys);
        EditView* const edits_data = edits.data();
        EditView* level_begin = edits_data;
        EditView* level_end = level_begin;

        for (usize level_i = 0; level_i < num_levels; ++level_i) {
          VLOG(2) << BATT_INSPECT(level_i);
          levels.emplace_back();
          auto& current_level = levels.back();

          auto finalize_slice = [&] {
            BATT_CHECK_GE(level_begin, edits.data())
                << BATT_INSPECT(edits.size()) << BATT_INSPECT(edits_data);
            BATT_CHECK_EQ(level_end, edits.data() + edits.size());

            VLOG(2) << "--- slice (size=" << std::distance(level_begin, level_end) << ") ---";
            current_level.emplace_back(as_slice(level_begin, level_end));
            level_begin = level_end;
          };

          std::bitset<kNumKeys> keys_in_level{pick_keys_in_level(rng)};
          std::bitset<kNumKeys + 1> level_slice_breaks{pick_level_slice_breaks(rng)};
          VLOG(2) << BATT_INSPECT(keys_in_level) << BATT_INSPECT(level_slice_breaks);

          for (usize j = 0; j < keys_in_level.size(); ++j) {
            if (level_slice_breaks[j]) {
              finalize_slice();
            }
            if (keys_in_level[j]) {
              const int op = pick_edit_op(rng);
              VLOG(2) << BATT_INSPECT(op);
              EditView edit;
              switch (op) {
                case 0: {  // Write
                  values.emplace_back(
                      std::make_unique<std::string>(batt::to_string(values.size())));
                  edit = EditView{keys[j], ValueView::from_str(*values.back())};
                  break;
                }
                case 1: {  // FetchAdd
                  edit = EditView{keys[j], ValueView::add_i32(1)};
                  break;
                }
                case 2: {  // Delete
                  edit = EditView{keys[j], ValueView::deleted()};
                  break;
                }
                default:
                  FAIL() << "bad op value: " << op;
                  break;
              }
              edits.emplace_back(edit);
              expected[edit.key].emplace_back(edit.value);
              VLOG(2) << BATT_INSPECT(edits.back());
              BATT_CHECK_EQ(edits.data(), edits_data);
              ++level_end;
            }
          }

          finalize_slice();
          if (level_slice_breaks[keys_in_level.size()]) {
            finalize_slice();
          }
        }
      }

      //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
      VLOG(1) << [&](std::ostream& out) {
        usize n = 0;
        for (const auto& level : levels) {
          out << std::endl << "line[" << n << "]: ";
          n += 1;
          for (const Slice<EditView>& slice : level) {
            out << " {";
            for (const EditView& e : slice) {
              out << e.key << ":" << e.value.as_str() << " ";
            }
            out << "}";
          }
        }
        out << std::endl;
      };
      VLOG(1) << "expected: " << batt::dump_range(expected, batt::Pretty::True);
      //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

      VLOG(1) << "starting";
      EditView::terse_printing_enabled() = true;
      MergeCompactor compactor{WorkerPool::default_pool()};

      compactor.set_generator([&](MergeCompactor::GeneratorContext& context) -> Status {
        MergeFrame frame;
        for (std::vector<Slice<EditView>>& line : levels) {
          frame.push_line(as_seq(line) | seq::map([](const Slice<EditView>& slice) {
                            return EditSlice{slice};
                          }) |
                          seq::boxed());
        }
        context.push_frame(&frame);
        return context.await_frame_consumed(&frame);
      });

      if (i % 2 == 1) {
        auto iter = expected.begin();
        while (iter != expected.end()) {
          MergeCompactor::EditBuffer merged_edits;
          VLOG(1) << "reading";
          Status status = compactor.read_some(merged_edits, global_max_key());
          ASSERT_TRUE(status.ok());

          VLOG(1) << "checking";
          for (const EditView& actual : merged_edits.get()) {
            ASSERT_NE(iter, expected.end());
            ASSERT_EQ(actual.key, iter->first);
            ASSERT_EQ(actual.value, apply_all(iter->second)) << " (key=" << iter->first << ")";
            ++iter;
          }
        }
        ASSERT_EQ(iter, expected.end());

      } else {
        // This should produce an empty result (because all keys > ".") and leave the generated
        // lines/frames intact.
        //
        MergeCompactor::EditBuffer buffer;
        {
          StatusOr<MergeCompactor::ResultSet</*kDecayToItems=*/false>> maybe_result =
              compactor.read(buffer, /*max_key=*/".");

          EXPECT_TRUE(maybe_result.ok());
          EXPECT_EQ(maybe_result->get().size(), 0u);
        }

        VLOG(1) << "reading";
        StatusOr<MergeCompactor::ResultSet</*kDecayToItems=*/false>> maybe_result =
            compactor.read(buffer, global_max_key());

        ASSERT_TRUE(maybe_result.ok());

        MergeCompactor::ResultSet</*kDecayToItems=*/false>& result = *maybe_result;

        auto iter = expected.begin();
        VLOG(1) << "checking";
        for (const EditView& actual : result.get()) {
          ASSERT_NE(iter, expected.end());
          ASSERT_EQ(actual.key, iter->first);
          ASSERT_EQ(actual.value, apply_all(iter->second)) << " (key=" << iter->first << ")";
          ++iter;
        }
        ASSERT_EQ(iter, expected.end());
      }
    }
  }
}

namespace {
std::string_view str_from_int(const llfs::big_u16& n)
{
  return std::string_view{(const char*)&n, 2};
}
}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(MergeCompactor, ResultSetDropKeyRange)
{
  for (unsigned seed = 0; seed < 100 * 1000; ++seed) {
    bool drop_exact_matches = (seed & 0b01) == 0;
    bool drop_half_open = (seed & 0b10) == 0;

    // Seed and prime the RNG.
    //
    std::default_random_engine rng{seed};
    for (usize i = 0; i < 32; ++i) {
      (void)rng();
    }

    // Generate a bunch of random, sorted, unique 2-char strings.
    //
    std::array<llfs::big_u16, 256> word_buffer;
    {
      u16 max_word = 0;
      for (usize i = 0; i < word_buffer.size(); ++i) {
        std::uniform_int_distribution<u16> pick_word{max_word, u16(65535 - word_buffer.size() + i)};
        max_word = pick_word(rng);
        word_buffer[i] = max_word;
        ++max_word;
      }
    }
    const auto key_by_index = [&word_buffer](usize i) {
      return str_from_int(word_buffer[i]);
    };

    // Create a test result set by randomly appending sorted runs of 2-byte keys.
    //
    MergeCompactor::ResultSet</*kDecayToItems=*/false> result_set;
    std::vector<std::string_view> expected;
    {
      std::uniform_int_distribution<usize> pick_n_appends{1, 7};
      std::uniform_int_distribution<usize> pick_append_size{1, 7};

      usize max_word_i = 0;
      const usize n_appends = pick_n_appends(rng);

      for (usize i = 0; i < n_appends; ++i) {
        const usize append_size = pick_append_size(rng);
        std::vector<EditView> edits;

        for (usize j = 0; j < append_size && max_word_i < 256; ++j) {
          std::uniform_int_distribution<usize> pick_word{max_word_i, 255};
          const usize word_i = pick_word(rng);

          max_word_i = word_i + 1;
          std::string_view key = key_by_index(word_i);
          edits.emplace_back(EditView{key, ValueView::from_str(key)});
          expected.emplace_back(key);
        }

        if (!edits.empty()) {
          result_set.append(std::move(edits));
        }
      }
    }

    const auto get_actual = [&] {
      std::vector<std::string_view> actual;
      for (const EditView& edit : result_set.get()) {
        actual.emplace_back(get_key(edit));
      }
      return actual;
    };

    EXPECT_EQ(get_actual(), expected);

    // Drop single-key ranges for all keys that we know aren't in the result set; expect no change
    // to the actual result set items.
    //
    {
      usize word_i = 0;
      for (const KeyView& key : expected) {
        while (key_by_index(word_i) < key) {
          auto prev_actual = get_actual();

          BATT_DEBUG_INFO(BATT_INSPECT_RANGE(prev_actual)
                          << BATT_INSPECT_STR(key_by_index(word_i)));

          {
            CInterval<KeyView> expect_no_match{key_by_index(word_i), key_by_index(word_i)};
            result_set.drop_key_range(expect_no_match);

            EXPECT_EQ(get_actual(), expected)
                << std::endl
                << BATT_INSPECT_STR(expect_no_match.lower_bound) << BATT_INSPECT_STR(key)
                << BATT_INSPECT(word_i) << std::endl
                << BATT_INSPECT_RANGE(prev_actual);
          }
          {
            Interval<KeyView> expect_no_match{key_by_index(word_i), key_by_index(word_i + 1)};
            result_set.drop_key_range_half_open(expect_no_match);

            EXPECT_EQ(get_actual(), expected)
                << std::endl
                << BATT_INSPECT_STR(expect_no_match.lower_bound) << BATT_INSPECT_STR(key)
                << BATT_INSPECT(word_i) << std::endl
                << BATT_INSPECT_RANGE(prev_actual);
          }

          ++word_i;
        }
        EXPECT_EQ(key_by_index(word_i), key);
        ++word_i;
      }
    }

    // Randomly drop ranges until all keys are dropped.
    //
    const int drop_count_limit = expected.size() + 3;

    for (int drop_count = 0; drop_count < drop_count_limit; ++drop_count) {
      std::ostringstream oss;
      oss << result_set.debug_dump();

      auto prev_actual = get_actual();
      result_set.check_invariants();

      if (drop_exact_matches && !expected.empty()) {
        std::uniform_int_distribution<usize> pick_first{0, expected.size() - 1};
        const usize first_i = pick_first(rng);
        std::uniform_int_distribution<usize> pick_last{first_i, expected.size() - 1};
        const usize last_i = pick_last(rng);

        if (drop_half_open) {
          Interval<KeyView> dropped_key_range{expected[first_i], expected[last_i]};

          BATT_DEBUG_INFO(BATT_INSPECT(expected.size())
                          << BATT_INSPECT_RANGE(prev_actual) << BATT_INSPECT(first_i)
                          << BATT_INSPECT(last_i) << std::endl
                          << oss.str() << std::endl
                          << result_set.debug_dump());

          result_set.drop_key_range_half_open(dropped_key_range);
          result_set.check_invariants();

          expected.erase(expected.begin() + first_i, expected.begin() + last_i);

          EXPECT_EQ(get_actual(), expected)
              << BATT_INSPECT_RANGE(prev_actual) << std::endl
              << BATT_INSPECT_STR(dropped_key_range.lower_bound) << std::endl
              << BATT_INSPECT_STR(dropped_key_range.upper_bound);

        } else {
          CInterval<KeyView> dropped_key_crange{expected[first_i], expected[last_i]};

          BATT_DEBUG_INFO(BATT_INSPECT(expected.size())
                          << BATT_INSPECT_RANGE(prev_actual) << BATT_INSPECT(first_i)
                          << BATT_INSPECT(last_i) << std::endl
                          << oss.str() << std::endl
                          << result_set.debug_dump());

          result_set.drop_key_range(dropped_key_crange);
          result_set.check_invariants();

          expected.erase(expected.begin() + first_i, expected.begin() + (last_i + 1));

          EXPECT_EQ(get_actual(), expected)
              << BATT_INSPECT_RANGE(prev_actual) << BATT_INSPECT_STR(dropped_key_crange.lower_bound)
              << BATT_INSPECT_STR(dropped_key_crange.upper_bound);
        }

      } else {
        std::uniform_int_distribution<u16> pick_u16{0, 65535};

        llfs::big_u16 a, b;
        a = pick_u16(rng);
        b = pick_u16(rng);

        if (a > b) {
          std::swap(a, b);
        }

        CInterval<KeyView> dropped_key_range{str_from_int(a), str_from_int(b)};

        result_set.drop_key_range(dropped_key_range);
        result_set.check_invariants();

        const auto first = std::lower_bound(expected.begin(), expected.end(),
                                            dropped_key_range.lower_bound, KeyOrder{});

        const auto last = std::upper_bound(expected.begin(), expected.end(),
                                           dropped_key_range.upper_bound, KeyOrder{});

        expected.erase(first, last);

        EXPECT_EQ(get_actual(), expected)
            << BATT_INSPECT_RANGE(prev_actual) << BATT_INSPECT_STR(dropped_key_range.lower_bound)
            << BATT_INSPECT_STR(dropped_key_range.upper_bound);
      }

      // Do some iterator math forward and backwards to make sure the all the chunk offsets are
      // still correct.
      //
      for (usize i = 0; i < expected.size(); ++i) {
        auto actual_edits = result_set.get();
        auto iter0 = actual_edits.begin();
        auto iter1 = iter0 + i;

        EXPECT_EQ(iter1 - iter0, i);
        EXPECT_EQ(get_key(*iter1), expected[i]);

        for (usize j = 0; j <= i; ++j) {
          auto iter2 = iter1 - j;
          auto iter3 = actual_edits.begin() + (i - j);

          EXPECT_GE(iter1, iter2) << BATT_INSPECT(i) << BATT_INSPECT(j) << "\n\n"
                                  << BATT_INSPECT(iter0) << "\n\n"
                                  << BATT_INSPECT(iter1) << "\n\n"
                                  << BATT_INSPECT(iter2) << "\n\n";
          EXPECT_EQ(iter1 - iter2, j)
              << BATT_INSPECT(iter1) << "\n\n"
              << BATT_INSPECT(iter2) << "\n\n"
              << BATT_INSPECT(i) << BATT_INSPECT(j) << BATT_INSPECT(drop_count)
              << BATT_INSPECT(seed) << BATT_INSPECT(i - j);
          EXPECT_EQ(iter2, iter3);
          EXPECT_EQ(get_key(*iter2), expected[i - j]);
          EXPECT_EQ(get_key(*iter3), expected[i - j]);
        }
      }
    }

    // end - for all seeds
  }
}

}  // namespace
