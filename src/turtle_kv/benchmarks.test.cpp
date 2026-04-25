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

#include <turtle_kv/util/print_csv.hpp>

#include <batteries/constants.hpp>
#include <batteries/do_nothing.hpp>
#include <batteries/env.hpp>
#include <batteries/int_types.hpp>
#include <batteries/seq/loop_control.hpp>
#include <batteries/stream_util.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <atomic>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace {

using namespace batt::int_types;
using namespace batt::constants;

using batt::getenv_as;
using batt::Status;
using batt::StatusOr;
using batt::to_string;

using turtle_kv::EditOffset;
using turtle_kv::KeyView;
using turtle_kv::KVStore;
using turtle_kv::Movable;
using turtle_kv::ValueView;

using Clock = std::chrono::steady_clock;
using Duration = Clock::duration;
using TimePoint = Clock::time_point;
using Barrier = std::barrier<batt::DoNothing>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class BenchmarkMetric
{
 public:
  BenchmarkMetric(const BenchmarkMetric&) = delete;
  BenchmarkMetric& operator=(const BenchmarkMetric&) = delete;

  virtual ~BenchmarkMetric() = default;

  virtual std::string name() = 0;

  virtual std::string value() = 0;

  virtual void start_collecting() = 0;

  virtual void stop_collecting() = 0;

 protected:
  BenchmarkMetric() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename MetricT>
class CountMetricDelta : public BenchmarkMetric
{
 public:
  using ValueT = std::decay_t<decltype(std::declval<MetricT&>().get())>;

  explicit CountMetricDelta(std::string name, MetricT& metric) noexcept
      : name_{std::move(name)}
      , metric_{metric}
  {
  }

  std::string name() override
  {
    return this->name_;
  }

  std::string value() override
  {
    return batt::to_string(this->delta_value_);
  }

  void start_collecting() override
  {
    this->start_value_ = this->metric_.get();
  }

  void stop_collecting() override
  {
    this->delta_value_ = this->metric_.get() - this->start_value_;
  }

 private:
  std::string name_;
  MetricT& metric_;
  ValueT start_value_;
  ValueT delta_value_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class BenchmarkContext
{
 public:
  using Self = BenchmarkContext;

  struct TrialResult {
    TimePoint begin_time;
    TimePoint end_time;
    std::vector<std::pair<std::string, std::string>> metrics;

    Duration duration() const
    {
      return this->end_time - this->begin_time;
    }
  };

  struct ScopedTrial {
    Movable<BenchmarkContext*> context_{nullptr};
    Movable<isize> trial_i_{0};

    ScopedTrial(const ScopedTrial&) = delete;
    ScopedTrial& operator=(const ScopedTrial&) = delete;

    ScopedTrial(ScopedTrial&&) = default;
    ScopedTrial& operator=(ScopedTrial&&) = default;

    ScopedTrial(BenchmarkContext* context, isize trial_i) noexcept
        : context_{context}
        , trial_i_{trial_i}
    {
      //----- --- -- -  -  -   -
      this->context_.ref()->trial_sync.arrive_and_wait();
      this->context_.ref()->before_trial();

      //----- --- -- -  -  -   -
      this->context_.ref()->trial_sync.arrive_and_wait();
      for (const auto& p_metric : this->context_.ref()->custom_metrics) {
        p_metric->start_collecting();
      }

      //----- --- -- -  -  -   -
      this->context_.ref()->trial_sync.arrive_and_wait();

      VLOG(1) << "trial started: " << this->trial_i_;
      this->result().begin_time = Clock::now();
    }

    ~ScopedTrial() noexcept
    {
      if (this->context_.ref() && this->trial_i_ < this->context_.ref()->n_trials) {
        this->result().end_time = Clock::now();
        VLOG(1) << "trial finished: " << this->trial_i_;

        //----- --- -- -  -  -   -
        this->context_.ref()->trial_sync.arrive_and_wait();
        for (const auto& p_metric : this->context_.ref()->custom_metrics) {
          p_metric->stop_collecting();
          this->result().metrics.push_back(std::make_pair(p_metric->name(), p_metric->value()));
        }

        //----- --- -- -  -  -   -
        this->context_.ref()->trial_sync.arrive_and_wait();
        this->context_.ref()->after_trial();

        //----- --- -- -  -  -   -
        this->context_.ref()->trial_sync.arrive_and_wait();
      }
    }

    TrialResult& result() const
    {
      return this->context_.cref()->trial_results[this->trial_i_];
    }
  };

  class iterator
      : public boost::iterator_facade<        //
            iterator,                         // <- Derived
            ScopedTrial,                      // <- Value
            std::random_access_iterator_tag,  // <- CategoryOrTraversal
            ScopedTrial,                      // <- Reference
            isize                             // <- Difference
            >
  {
   public:
    using Self = iterator;
    using iterator_category = std::random_access_iterator_tag;
    using value_type = ScopedTrial;
    using reference = ScopedTrial;

    explicit iterator(BenchmarkContext* context, isize trial_i) noexcept
        : context{context}
        , trial_i{trial_i}
    {
    }

    ScopedTrial dereference() const
    {
      return ScopedTrial{this->context, this->trial_i};
    }

    bool equal(const Self& other) const
    {
      return this->context == other.context && this->trial_i == other.trial_i;
    }

    void increment()
    {
      BATT_CHECK_LT(this->trial_i, this->context->n_trials);
      ++this->trial_i;
    }

    void decrement()
    {
      BATT_CHECK_GT(this->trial_i, 0);
      --this->trial_i;
    }

    void advance(isize delta)
    {
      this->trial_i += delta;
      BATT_CHECK_IN_RANGE(0, this->trial_i, this->context->n_trials + 1);
    }

    isize distance_to(const Self& other) const
    {
      return other.trial_i - this->trial_i;
    }

    //----- --- -- -  -  -   -
    BenchmarkContext* context = nullptr;
    isize trial_i = 0;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BenchmarkContext(const BenchmarkContext&) = delete;
  BenchmarkContext& operator=(const BenchmarkContext&) = delete;

  explicit BenchmarkContext(const char* test_name,
                            usize thread_i,
                            usize n_threads,
                            Barrier& trial_sync) noexcept
      : test_name{test_name}
      , thread_i{thread_i}
      , n_threads{n_threads}
      , n_trials{3}
      , trial_results(this->n_trials)
      , trial_sync{trial_sync}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  iterator begin()
  {
    return iterator{this, 0};
  }

  iterator end()
  {
    return iterator{this, this->n_trials};
  }

  Duration avg_duration() const
  {
    Duration d = std::chrono::seconds(0);
    for (auto& r : this->trial_results) {
      d += r.duration();
    }
    return d / this->trial_results.size();
  }

  template <typename MetricT>
  void add_count_metric_delta(std::string name, MetricT& count_metric)
  {
    this->custom_metrics.emplace_back(
        std::make_unique<CountMetricDelta<MetricT>>(std::move(name), count_metric));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const char* test_name = "(not set)";
  usize thread_i = 0;
  usize n_threads = 0;
  isize n_trials = 0;
  std::vector<TrialResult> trial_results;
  Barrier& trial_sync;

  std::function<void()> before_trial = batt::DoNothing{};
  std::function<void()> after_trial = batt::DoNothing{};

  std::vector<std::unique_ptr<BenchmarkMetric>> custom_metrics;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class BenchmarksTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->load_data_file();
    this->read_params();
  }

  const char* test_name() const
  {
    const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();

    return test_info->name();
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

  void insert_data(bool sorted, usize shard_i = 0, usize n_shards = 1)
  {
    if (sorted) {
      this->require_sorted_data();
    }

    this->max_inserted_pos = 0;

    const auto insert_fn = [this](const std::string_view& key, const std::string_view& value) {
      Status status = this->kv_store->put(key, ValueView::from_str(value));
      BATT_CHECK_OK(status);
    };

    if (sorted) {
      this->visit_sorted_data(insert_fn, shard_i, n_shards);
    } else {
      this->visit_random_data(insert_fn, shard_i, n_shards);
    }
  }

  void insert_data(BenchmarkContext& context, bool sorted)
  {
    this->insert_data(sorted, context.thread_i, context.n_threads);
  }

  usize shard_begin(usize shard_i, usize n_shards)
  {
    usize shard_size = this->n_keys / n_shards;
    usize remainder = this->n_keys % n_shards;
    return shard_i * shard_size + std::min(shard_i, remainder);
  }

  template <std::invocable<const std::string_view& /*key*/, const std::string_view& /*value*/> Fn>
  void visit_random_data(Fn&& fn, usize shard_i = 0, usize n_shards = 1)
  {
    usize start_i = this->shard_begin(shard_i, n_shards);
    usize end_i = this->shard_begin(shard_i + 1, n_shards);
    usize data_pos = start_i % this->data_str.size();

    for (usize i = start_i; i < end_i; ++i) {
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

  std::string_view get_inserted_key(usize i) const
  {
    return std::string_view{this->data_str.data() + i, this->key_len};
  }

  template <std::invocable<const std::string_view& /*key*/, const std::string_view& /*value*/> Fn>
  void visit_sorted_data(Fn&& fn, usize shard_i = 0, usize n_shards = 1)
  {
    this->require_sorted_data();

    usize start_i = this->shard_begin(shard_i, n_shards);
    usize end_i = this->shard_begin(shard_i + 1, n_shards);
    const char* p_data = this->sorted_data.get() + start_i * this->key_len;

    for (usize i = start_i; i < end_i; ++i) {
      const std::string_view key{p_data, this->key_len};
      const std::string_view value{p_data, this->value_len};

      BATT_INVOKE_LOOP_FN((fn, key, value));

      p_data += this->key_len;
    }
  }

  void require_sorted_data()
  {
    if (this->sorted_data == nullptr || this->key_len != this->sorted_key_len ||
        this->value_len != this->sorted_value_len) {
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
    }
  }

  void checkpoint()
  {
    EditOffset checkpoint_offset = BATT_OK_RESULT_OR_PANIC(kv_store->force_checkpoint());
    BATT_CHECK_OK(kv_store->wait_for_checkpoint(checkpoint_offset));
  }

  template <std::invocable<BenchmarkContext&> Fn>
  void thread_scaling(Fn&& fn)
  {
    const usize n_proc = getenv_as<usize>("T").value_or(std::thread::hardware_concurrency());

    std::vector<std::vector<std::pair<std::string, std::string>>> rows;

    usize n_threads = 1;
    for (;;) {
      Barrier start_point{(i32)n_threads};
      std::vector<std::thread> threads;
      std::vector<Duration> thread_duration(n_threads);
      std::vector<std::vector<std::vector<std::pair<std::string, std::string>>>> thread_metrics(
          n_threads);

      const auto run_thread =
          [this, &start_point, &thread_duration, &thread_metrics, &fn, n_threads](usize thread_i) {
            BenchmarkContext context{this->test_name(), thread_i, n_threads, start_point};
            fn(context);
            thread_duration[thread_i] = context.avg_duration();

            for (auto& r : context.trial_results) {
              thread_metrics[thread_i].push_back(r.metrics);
            }
          };

      for (usize thread_i = 1; thread_i < n_threads; ++thread_i) {
        threads.emplace_back(
            [this, &start_point, &thread_duration, &fn, thread_i, n_threads, &run_thread] {
              run_thread(thread_i);
            });
      }

      LOG(INFO) << BATT_INSPECT(n_threads);

      run_thread(0);

      for (std::thread& t : threads) {
        t.join();
      }

      Duration duration = *std::max_element(thread_duration.begin(), thread_duration.end());

      double rate = (double)(this->n_keys) * 1e6 /
                    (double)std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

      auto& row = rows.emplace_back();

      row.emplace_back("test_name", this->test_name());
      row.emplace_back("N", to_string(this->n_keys));
      row.emplace_back("n_threads", to_string(n_threads));
      row.emplace_back("rate(kops/sec)", to_string(rate));

      for (usize thread_i = 0; thread_i < n_threads; ++thread_i) {
        for (usize trial_i = 0; trial_i < thread_metrics[thread_i].size(); ++trial_i) {
          for (const auto& [name, value] : thread_metrics[thread_i][trial_i]) {
            row.emplace_back(to_string(name, ".", thread_i, ".", trial_i), value);
          }
        }
      }

      if (n_threads == n_proc) {
        break;
      }

      n_threads =
          std::min<usize>(n_proc, n_threads + std::max<usize>(batt::log2_floor(n_threads), 1));
    }

    turtle_kv::print_csv(std::cout, rows);

    // std::cout << batt::dump_range(duration_per_threads, batt::Pretty::True) << std::endl;
  }

  void random_gets(usize thread_i = 0, usize n_threads = 1)
  {
    std::default_random_engine rng{std::random_device{}()};
    std::uniform_int_distribution<usize> pick_i{0, this->n_keys - 1};

    usize start_i = this->shard_begin(thread_i, n_threads);
    usize end_i = this->shard_begin(thread_i + 1, n_threads);

    for (usize i = start_i; i < end_i; ++i) {
      KeyView key = this->get_inserted_key(pick_i(rng));
      BATT_CHECK_OK(this->kv_store->get(key));
    }
  }

  void random_gets(BenchmarkContext& context)
  {
    this->random_gets(context.thread_i, context.n_threads);
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
TEST_F(BenchmarksTest, RandomInsertOrderThreads)
{
  this->thread_scaling([this](auto& context) {
    // Setup
    //
    if (context.thread_i == 0) {
      context.before_trial = [this] {
        this->create_kv_store();
        this->open_kv_store();
      };
      context.after_trial = [this] {
        auto& mem_table = this->kv_store->metrics().mem_table;
        LOG(INFO) << BATT_INSPECT(mem_table.wait_for_trim_count)
                  << BATT_INSPECT(mem_table.short_finalize_count)
                  << BATT_INSPECT(mem_table.finalize_size_stats)
                  << BATT_INSPECT(mem_table.short_finalize_size_stats);
      };
    }

    // Workload
    //
    for (auto _ : context) {
      this->insert_data(context, /*sorted=*/false);
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, SortedInsertOrderThreads)
{
  this->require_sorted_data();

  this->thread_scaling([this](BenchmarkContext& context) {
    // Setup
    //
    if (context.thread_i == 0) {
      context.before_trial = [this] {
        this->create_kv_store();
        this->open_kv_store();
      };
    }

    // Workload
    //
    for (auto _ : context) {
      this->insert_data(context, /*sorted=*/true);
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, MemTableGetThreads)
{
  this->thread_scaling([this](BenchmarkContext& context) {
    // Setup
    //
    if (context.thread_i == 0) {
      context.before_trial = [this, &context] {
        this->create_kv_store();
        this->open_kv_store();
        this->insert_data(/*sorted=*/false);

        auto& kv_store_metrics = this->kv_store->metrics();

        context.add_count_metric_delta("found_in_mem_table",
                                       kv_store_metrics.all_mem_tables_get_count);

        context.add_count_metric_delta("found_in_checkpoint",
                                       kv_store_metrics.checkpoint_get_count);
      };
      context.after_trial = [&context] {
        context.custom_metrics.clear();
      };
    }

    // Workload
    //
    for (auto _ : context) {
      this->random_gets(context);
      this->kv_store->reset_thread_context();
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, CheckpointGetThreads)
{
  this->thread_scaling([this](BenchmarkContext& context) {
    // Setup
    //
    if (context.thread_i == 0) {
      context.before_trial = [this, &context] {
        this->create_kv_store();
        this->open_kv_store();
        this->insert_data(/*sorted=*/false);
        this->checkpoint();

        auto& kv_store_metrics = this->kv_store->metrics();

        context.add_count_metric_delta("found_in_mem_table",
                                       kv_store_metrics.all_mem_tables_get_count);

        context.add_count_metric_delta("found_in_checkpoint",
                                       kv_store_metrics.checkpoint_get_count);

        auto& cache_slot_pool_metrics = llfs::PageCacheSlot::Pool::Metrics::instance();

        context.add_count_metric_delta("pin_count", cache_slot_pool_metrics.pin_count);
        context.add_count_metric_delta("unpin_count", cache_slot_pool_metrics.unpin_count);
      };
      context.after_trial = [&context] {
        context.custom_metrics.clear();
      };
    }

    // Workload
    //
    for (auto _ : context) {
      this->random_gets(context);
      this->kv_store->reset_thread_context();
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, CheckpointGetThreadsEvictionPause)
{
  this->thread_scaling([this](BenchmarkContext& context) {
    // Setup
    //
    if (context.thread_i == 0) {
      context.before_trial = [this, &context] {
        this->create_kv_store();
        this->open_kv_store();
        this->insert_data(/*sorted=*/false);
        this->checkpoint();

        auto& kv_store_metrics = this->kv_store->metrics();

        context.add_count_metric_delta("found_in_mem_table",
                                       kv_store_metrics.all_mem_tables_get_count);

        context.add_count_metric_delta("found_in_checkpoint",
                                       kv_store_metrics.checkpoint_get_count);

        auto& cache_slot_pool_metrics = llfs::PageCacheSlot::Pool::Metrics::instance();

        context.add_count_metric_delta("pin_count", cache_slot_pool_metrics.pin_count);
        context.add_count_metric_delta("unpin_count", cache_slot_pool_metrics.unpin_count);

        this->kv_store->reset_thread_context();
        llfs::PageCacheSlot::eviction_pause() = true;
      };
      context.after_trial = [&context] {
        llfs::PageCacheSlot::eviction_pause() = false;
        context.custom_metrics.clear();
      };
    }

    // Workload
    //
    for (auto _ : context) {
      this->random_gets(context);
      this->kv_store->reset_thread_context();
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(BenchmarksTest, TestContext)
{
  Barrier b{1};
  BenchmarkContext context{this->test_name(), 0, 1, b};

  LOG(INFO) << "before";
  for (auto _ : context) {
    LOG(INFO) << " a trial";
  }
  LOG(INFO) << "after";

  for (const auto& trial_result : context.trial_results) {
    LOG(INFO) << BATT_INSPECT(trial_result.duration());
  }
  LOG(INFO) << BATT_INSPECT(context.avg_duration());
}

}  // namespace
