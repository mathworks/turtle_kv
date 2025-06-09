#if 0
#include <turtle_kv/new_mem_table.hpp>
//
#include <turtle_kv/new_mem_table.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/testing/workload.test.hpp>

#include <turtle_kv/core/table.hpp>
#include <turtle_kv/mem_table_entry.hpp>

#include <llfs/stable_string_store.hpp>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>
#include <batteries/stream_util.hpp>

#include <atomic>

namespace {

using namespace batt::int_types;

using batt::fixed_point::LinearProjection;
using llfs::BasicStableStringStore;
using turtle_kv::DefaultStrEq;
using turtle_kv::DefaultStrHash;
using turtle_kv::KeyView;
using turtle_kv::MemTableEntry;
using turtle_kv::OkStatus;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::StdMapTable;
using turtle_kv::Table;
using turtle_kv::ValueView;
using turtle_kv::testing::get_project_file;
using turtle_kv::testing::run_workload;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
class HashMapTable : public Table
{
 public:
  BasicStableStringStore<4096, 4096> string_store_;
  absl::flat_hash_map<std::string_view, std::string_view> hash_map_;

  Status put(const KeyView& key, const ValueView& value) override
  {
    std::string_view stored_key = this->string_store_.store(key);
    std::string_view stored_value = this->string_store_.store(value.as_str());

    auto [iter, inserted] = this->hash_map_.emplace(stored_key, stored_value);
    if (!inserted) {
      iter->second = stored_value;
    }

    return OkStatus();
  }

  StatusOr<ValueView> get(const KeyView& key) override
  {
    auto iter = this->hash_map_.find(key);
    if (iter == this->hash_map_.end()) {
      return {batt::StatusCode::kNotFound};
    }

    return {ValueView::from_str(iter->second)};
  }

  StatusOr<usize> scan(const KeyView& min_key [[maybe_unused]],
                       const Slice<std::pair<KeyView, ValueView>>& items_out
                       [[maybe_unused]]) override
  {
    BATT_PANIC() << "TODO [tastolfi 2025-05-24] implement me!";
    return {batt::StatusCode::kUnimplemented};
  }

  Status remove(const KeyView& key [[maybe_unused]]) override
  {
    BATT_PANIC() << "TODO [tastolfi 2025-05-24] implement me!";
    return {batt::StatusCode::kUnimplemented};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
class CustomHashTable : public Table
{
 public:
  struct Bucket {
    std::atomic<u64> hash_val;
    const char* key_data;
    const char* value_data;
    std::atomic<u32> state;
    u16 key_size;
    u16 value_size;

    struct View {
      std::string_view key;
      std::string_view value;
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    void lock()
    {
      for (;;) {
        if ((this->state.fetch_or(1) & 1) == 1) {
          continue;
        }
        break;
      }
      this->state.fetch_add(2);
    }

    void unlock()
    {
      // Adding 1 when we have the lock will always unset the lsb (the lock bit) and increment the
      // sequence counter in a single instruction.
      //
      this->state.fetch_add(1);
    }

    View read() const
    {
      for (;;) {
        const u32 before_state = this->state.load();
        if ((before_state & 3) != 0) {
          continue;
        }
        View view{
            .key =
                std::string_view{
                    this->key_data,
                    this->key_size,
                },
            .value =
                std::string_view{
                    this->value_data,
                    this->value_size,
                },
        };
        const u32 after_state = this->state.load();
        if (before_state == after_state) {
          return view;
        }
      }
      BATT_UNREACHABLE();
    }
  };

  const usize leaf_size_;
  const usize bucket_count_ = this->leaf_size_ / 24;
  const usize overflow_bucket_count_ = std::max<usize>(4096, this->bucket_count_ / 16);

  const LinearProjection<u64, usize> bucket_from_hash_val_{this->bucket_count_};
  const LinearProjection<u64, usize> overflow_bucket_from_hash_val_{this->overflow_bucket_count_};

  BasicStableStringStore<4096, 4096> string_store_;
  DefaultStrEq str_eq_;
  DefaultStrHash str_hash_;
  std::vector<std::unique_ptr<Bucket[]>> bucket_storage_;
  std::vector<Slice<Bucket>> buckets_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit CustomHashTable(usize leaf_size = 4096 * 1024) noexcept : leaf_size_{leaf_size}
  {
    const usize n_buckets = this->leaf_size_ / 24;
    this->bucket_storage_.emplace_back(new Bucket[n_buckets]);
    this->buckets_.emplace_back(batt::as_slice(this->bucket_storage_.back().get(), n_buckets));

    std::memset((void*)this->bucket_storage_.back().get(), 0, sizeof(Bucket) * n_buckets);
  }

  Status put(const KeyView& key, const ValueView& value) override
  {
    const u64 key_hash_val = this->str_hash_(key) | 1;
    std::string_view stored_key = this->string_store_.store(key);
    std::string_view stored_value = this->string_store_.store(value.as_str());

    usize bucket_i = this->bucket_from_hash_val_(key_hash_val);
    for (;;) {
      Bucket& bucket = this->buckets_.back()[bucket_i];

      const u64 observed_hash_val_0 = bucket.hash_val.load();

      if (observed_hash_val_0 == 0 || observed_hash_val_0 == key_hash_val) {
        bucket.lock();
        auto on_scope_exit = batt::finally([&] {
          bucket.unlock();
        });

        const u64 observed_hash_val_1 = bucket.hash_val.load();

        if (observed_hash_val_1 == 0) {
          bucket.hash_val.store(key_hash_val);
          bucket.key_data = stored_key.data();
          bucket.key_size = stored_key.size();
          bucket.value_data = stored_value.data();
          bucket.value_size = stored_value.size();
          break;
        }

        if (observed_hash_val_1 == key_hash_val &&
            this->str_eq_(key, std::string_view{bucket.key_data, bucket.key_size})) {
          bucket.value_data = stored_value.data();
          bucket.value_size = stored_value.size();
          break;
        }
      }

      bucket_i += 1;
      if (bucket_i == this->buckets_.back().size()) {
        bucket_i = 0;
      }
    }

    return OkStatus();
  }

  StatusOr<ValueView> get(const KeyView& key) override
  {
    const u64 key_hash_val = this->str_hash_(key) | 1;
    usize bucket_i = this->bucket_from_hash_val_(key_hash_val);
    for (;;) {
      Bucket& bucket = this->buckets_.back()[bucket_i];

      if (bucket.hash_val == key_hash_val) {
        Bucket::View bucket_view = bucket.read();
        if (this->str_eq_(key, bucket_view.key)) {
          return {ValueView::from_str(bucket_view.value)};
        }
      }

      bucket_i += 1;
      if (bucket_i == this->buckets_.back().size()) {
        bucket_i = 0;
      }
    }

    BATT_UNREACHABLE();
  }

  StatusOr<usize> scan(const KeyView& min_key [[maybe_unused]],
                       const Slice<std::pair<KeyView, ValueView>>& items_out
                       [[maybe_unused]]) override
  {
    BATT_PANIC() << "TODO [tastolfi 2025-05-24] implement me!";
    return {batt::StatusCode::kUnimplemented};
  }

  Status remove(const KeyView& key [[maybe_unused]]) override
  {
    BATT_PANIC() << "TODO [tastolfi 2025-05-24] implement me!";
    return {batt::StatusCode::kUnimplemented};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
void test_index(const std::string& index_name, Table& index_impl)
{
  for (const char* workload_file : {
           "data/workloads/workload-abcdf.test.txt",
           //"data/workloads/workload-abcdf.txt",
       }) {
    auto [op_count, time_points] =
        run_workload(get_project_file(std::filesystem::path{workload_file}), index_impl);

    for (usize i = 1; i < time_points.size(); ++i) {
      double elapsed = (time_points[i].seconds - time_points[i - 1].seconds);
      double rate =
          (time_points[i].op_count - time_points[i - 1].op_count) / std::max(1e-10, elapsed);

      LOG(INFO) << "(THREADS=1)" << BATT_INSPECT(index_name) << " | " << time_points[i].label
                << ": " << rate << " ops/sec";
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
TEST(NewMemTableTest, Test)
{
  const std::vector<std::string> random_words{
      "gas",      "prosper",     "enter",     "volume",     "fill",        "distant",  "tower",
      "diplomat", "situation",   "offspring", "circle",     "dive",        "elite",    "customer",
      "mushroom", "meal",        "lay",       "quotation",  "vegetarian",  "bulletin", "button",
      "define",   "participate", "temporary", "attraction", "discipline",  "pocket",   "safety",
      "mail",     "slime",       "due",       "loop",       "deteriorate", "stumble",  "stab",
      "traffic",  "late",        "original",  "pat",        "costume",     "output",   "rotation",
      "picture",  "proclaim",    "tube",      "medicine",   "cap",         "liver",    "admiration",
      "carriage",
  };

  std::vector<std::string> sorted_words = random_words;
  std::sort(sorted_words.begin(), sorted_words.end());

  usize words_size = 0;
  usize prefixes_size = 0;
  for (const std::string& word : random_words) {
    words_size += 1 + word.size();
    for (usize prefix = 1; prefix <= word.size(); ++prefix) {
      prefixes_size += 16 + prefix;
    }
  }

  std::cout << std::endl
            << BATT_INSPECT_RANGE_PRETTY(sorted_words) << BATT_INSPECT(sorted_words.size())
            << BATT_INSPECT(words_size) << BATT_INSPECT(prefixes_size) << std::endl;

  LossyHashTrieTable range_index{words_size / 2};
  for (const std::string& word : random_words) {
    range_index.put(word);
  }

  std::vector<std::string> indexed_words = range_index.scan_all();

  for (const std::string& word : random_words) {
    const auto [first, last] = std::equal_range(indexed_words.begin(), indexed_words.end(), word);
    EXPECT_NE(first, last) << BATT_INSPECT_STR(word);
  }

  std::cout << std::endl
            << BATT_INSPECT_RANGE_PRETTY(indexed_words) << BATT_INSPECT(indexed_words.size())
            << std::endl
            << BATT_INSPECT_RANGE(range_index.max_prefix_len_) << std::endl;

  std::thread test_thread{[&] {
    BATT_CHECK_OK(batt::pin_thread_to_cpu(0));
    {
      StdMapTable std_map_table;
      test_index("std_map_table", std_map_table);
    }
    {
      HashMapTable hash_map_table;
      test_index("hash_map_table", hash_map_table);
    }
    {
      CustomHashTable custom_hash_table;
      test_index("custom_hash_table", custom_hash_table);
    }
  }};

  test_thread.join();
}

}  // namespace
#endif
