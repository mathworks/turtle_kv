//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/kv_store.hpp>
//

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/constants.hpp>
#include <batteries/env.hpp>
#include <batteries/int_types.hpp>
#include <batteries/seq/loop_control.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>

namespace {

using namespace batt::int_types;
using namespace batt::constants;

using batt::getenv_as;
using batt::Status;
using batt::StatusOr;

using turtle_kv::EditOffset;
using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::ValueView;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class BenchmarksTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->load_data_file();
    this->read_params();
    this->create_kv_store();
    this->open_kv_store();
  }

  void load_data_file()
  {
    LOG(INFO) << "loading data file...";
    data_str = [&] {
      std::ifstream ifs{data_file};
      std::ostringstream oss;
      oss << ifs.rdbuf();
      return std::move(oss).str();
    }();
    EXPECT_EQ(data_str.size(), 1 * kGiB);
    LOG(INFO) << "data file loaded;" << BATT_INSPECT(data_str.size());
  }

  void read_params()
  {
    n_keys = getenv_as<usize>("N").value_or(1 * 1000 * 1000);
    key_len = getenv_as<usize>("KL").value_or(24);
    value_len = getenv_as<usize>("VL").value_or(100);
    step_size = std::max(key_len, value_len);

    config.tree_options.set_key_size_hint(key_len);
    config.tree_options.set_value_size_hint(value_len);

    config.change_log_size_bytes =
        getenv_as<usize>("WAL_MB").value_or(default_config.change_log_size_bytes / kMiB) * kMiB;

    config.tree_options.set_leaf_size(
        getenv_as<usize>("LEAF_KB").value_or(config.tree_options.leaf_size() / kKiB) * kKiB);

    options.cache_size_bytes =
        getenv_as<usize>("CACHE_MB").value_or(default_options.cache_size_bytes / kMiB) * kMiB;

    options.initial_checkpoint_distance =
        getenv_as<usize>("CHI").value_or(default_options.initial_checkpoint_distance);

    LOG(INFO) << "wal=" << batt::dump_size(config.change_log_size_bytes)
              << " cache=" << batt::dump_size(options.cache_size_bytes)
              << " chi=" << options.initial_checkpoint_distance
              << BATT_INSPECT(config.tree_options);
  }

  void create_kv_store()
  {
    Status status = KVStore::create(kv_store_dir, config, turtle_kv::RemoveExisting{true});
    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  void open_kv_store()
  {
    this->kv_store = BATT_OK_RESULT_OR_PANIC(
        KVStore::open(this->kv_store_dir, this->config.tree_options, this->options));

    this->kv_store->set_checkpoint_distance(64);
  }

  void insert_data(bool sorted)
  {
    LOG(INFO) << "Inserting" << BATT_INSPECT(n_keys) << BATT_INSPECT(sorted);

    if (sorted) {
      this->update_sorted_data();
    }

    this->max_inserted_pos = 0;
    auto load_start_time = std::chrono::steady_clock::now();

    LOG(INFO) << BATT_INSPECT(key_len) << BATT_INSPECT(value_len);

    const auto insert_fn = [this](const std::string_view& key, const std::string_view& value) {
      Status status = this->kv_store->put(key, ValueView::from_str(value));
      BATT_CHECK_OK(status);
    };

    if (sorted) {
      this->visit_sorted_data(insert_fn);
    } else {
      this->visit_random_data(insert_fn);
    }

    auto load_finish_time = std::chrono::steady_clock::now();
    double load_time_nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(load_finish_time - load_start_time)
            .count();

    const u64 bytes_inserted = (this->key_len + this->value_len) * this->n_keys;

    LOG(INFO) << "Insert finished in " << (load_time_nanos / 1e6) << " ms -- "
              << ((double)n_keys) / load_time_nanos * 1e6 << " kops/sec"
              << BATT_INSPECT(batt::dump_size(bytes_inserted));
  }

  template <std::invocable<const std::string_view& /*key*/, const std::string_view& /*value*/> Fn>
  void visit_random_data(Fn&& fn)
  {
    usize data_pos = 0;

    for (usize i = 0; i < this->n_keys; ++i) {
      if (data_pos + this->step_size > this->data_str.size()) {
        BATT_CHECK_NE(data_pos, 0);
        data_pos = data_pos + this->step_size - this->data_str.size();
        BATT_CHECK_LE(data_pos + this->step_size, this->data_str.size());
      }
      const char* p_data = &this->data_str[data_pos];

      const std::string_view key{p_data, this->key_len};
      const std::string_view value{p_data, this->value_len};

      BATT_INVOKE_LOOP_FN((fn, key, value));

      ++data_pos;
    }
  }

  template <std::invocable<const std::string_view& /*key*/, const std::string_view& /*value*/> Fn>
  void visit_sorted_data(Fn&& fn)
  {
    this->update_sorted_data();

    const char* p_data = this->sorted_data.get();
    for (usize i = 0; i < this->n_keys; ++i) {
      const std::string_view key{p_data, this->key_len};
      const std::string_view value{p_data, this->value_len};

      BATT_INVOKE_LOOP_FN((fn, key, value));

      p_data += this->key_len;
    }
  }

  void update_sorted_data()
  {
    if (this->sorted_data == nullptr || this->key_len != this->sorted_key_len ||
        this->value_len != this->sorted_value_len) {
      //----- --- -- -  -  -   -
      // Lazily initialize the sorted data.
      //
      BATT_CHECK_EQ(std::max(this->key_len, this->value_len), this->step_size);

      this->sorted_data_size = this->n_keys * this->step_size;
      this->sorted_data.reset(new char[this->sorted_data_size]);

      std::vector<std::string_view> to_sort;

      this->visit_random_data(
          [&](const std::string_view& key, const std::string_view& value [[maybe_unused]]) {
            to_sort.emplace_back(key);
          });

      std::sort(to_sort.begin(), to_sort.end());

      char* dst = sorted_data.get();
      for (usize i = 0; i < this->n_keys; ++i) {
        BATT_CHECK_EQ(to_sort[i].size(), this->key_len);
        std::memcpy(dst, to_sort[i].data(), to_sort[i].size());
        dst += to_sort[i].size();
      }

      this->sorted_key_len = this->key_len;
      this->sorted_value_len = this->value_len;
      //----- --- -- -  -  -   -
    }
  }

  void checkpoint()
  {
    LOG(INFO) << "Forcing checkpoint...";
    EditOffset checkpoint_offset = BATT_OK_RESULT_OR_PANIC(kv_store->force_checkpoint());
    BATT_CHECK_OK(kv_store->wait_for_checkpoint(checkpoint_offset));
    LOG(INFO) << "Checkpoint committed at " << checkpoint_offset;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::filesystem::path data_file = "/mnt/kv-bakeoff/random_bytes.bin";
  std::filesystem::path kv_store_dir = "/mnt/kv-bakeoff/turtle_kv_benchmark";
  std::string data_str;
  std::unique_ptr<char[]> sorted_data{nullptr};
  usize sorted_data_size = 0;
  usize sorted_key_len = 0;
  usize sorted_value_len = 0;
  const KVStore::Config default_config = KVStore::Config::with_default_values();
  const KVStore::RuntimeOptions default_options = KVStore::RuntimeOptions::with_default_values();
  KVStore::Config config = default_config;
  KVStore::RuntimeOptions options = default_options;
  usize n_keys = 0;
  usize key_len = 0;
  usize value_len = 0;
  usize step_size = 0;
  usize max_inserted_pos = 0;
  std::unique_ptr<KVStore> kv_store;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, RandomInsertOrder)
{
  this->insert_data(/*sorted=*/false);
  this->checkpoint();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, SortedInsertOrder)
{
  this->insert_data(/*sorted=*/true);
  this->checkpoint();
}

}  // namespace
