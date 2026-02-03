#include <turtle_kv/kv_store.hpp>
//

#include <turtle_kv/config.hpp>
#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/checkpoint_log.hpp>
#include <turtle_kv/file_utils.hpp>
#include <turtle_kv/kv_store_scanner.hpp>
#include <turtle_kv/page_file.hpp>

#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/sharded_level_scanner.hpp>
#include <turtle_kv/tree/storage_config.hpp>
#include <turtle_kv/tree/subtree_metrics.hpp>

#include <turtle_kv/util/memory_stats.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/env.hpp>

#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/page_device_metrics.hpp>

#if TURTLE_KV_ENABLE_TCMALLOC
#if TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/malloc_hook.h>
#endif  // TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING
#endif  // TURTLE_KV_ENABLE_TCMALLOC

#include <set>

namespace turtle_kv {

namespace {

std::string_view change_log_file_name() noexcept
{
  return "change_log.turtle_kv";
}

std::string_view checkpoint_log_file_name() noexcept
{
  return "checkpoint_log.llfs";
}

std::string_view leaf_page_file_name() noexcept
{
  return "leaf_pages.llfs";
}

std::string_view node_page_file_name() noexcept
{
  return "node_pages.llfs";
}

std::string_view filter_page_file_name() noexcept
{
  return "filter_pages.llfs";
}

u64 query_page_loader_reset_every_n()
{
  static const u64 cached_value = [] {
    const u64 turtlekv_query_page_loader_reset_every_log2 =
        getenv_as<u64>("turtlekv_query_page_loader_reset_every_log2").value_or(7);

    LOG(INFO) << BATT_INSPECT(turtlekv_query_page_loader_reset_every_log2);

    const u64 n = (u64{1} << turtlekv_query_page_loader_reset_every_log2) - 1;
    return n;
  }();

  return cached_value;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status KVStore::configure_storage_context(llfs::StorageContext& storage_context,
                                                     const TreeOptions& tree_options,
                                                     const RuntimeOptions& runtime_options) noexcept
{
  llfs::PageCacheOptions page_cache_options =
      page_cache_options_from(tree_options, runtime_options.cache_size_bytes);

  storage_context.set_page_cache_options(page_cache_options);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status KVStore::create(const std::filesystem::path& dir_path,  //
                                  const Config& config,                   //
                                  RemoveExisting remove) noexcept
{
  StatusOr<llfs::ScopedIoRing> scoped_io_ring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096},  //
                                   llfs::ThreadPoolSize{1});
  BATT_REQUIRE_OK(scoped_io_ring);

  boost::intrusive_ptr<llfs::StorageContext> p_storage_context =
      llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),  //
                                        scoped_io_ring->get_io_ring());

  return KVStore::create(*p_storage_context, dir_path, config, remove);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status KVStore::create(llfs::StorageContext& storage_context,   //
                                  const std::filesystem::path& dir_path,   //
                                  const KVStore::Config& kv_store_config,  //
                                  RemoveExisting remove_existing           //
                                  ) noexcept
{
  const TreeOptions& tree_options = kv_store_config.tree_options;

  // Handle `remove_existing` once, before creating anything.  This means we pass
  // RemoveExisting{false} to all the other create functions.
  //
  if (remove_existing) {
    BATT_REQUIRE_OK(remove_existing_path(dir_path));
  }

  // Create the directory path up to `dir_path`.
  {
    std::error_code ec;
    std::filesystem::create_directories(dir_path, ec);
    BATT_REQUIRE_OK(ec);
  }

  // Create the change log file.
  {
    ChangeLogFile::Config change_log_config{
        .block_size = BlockSize{ChangeLogFile::kDefaultBlockSize},
        .block_count = BlockCount{BATT_CHECKED_CAST(
            BlockCount::value_type,
            (kv_store_config.change_log_size_bytes + ChangeLogFile::kDefaultBlockSize - 1) /
                ChangeLogFile::kDefaultBlockSize)},
        .block0_offset = FileOffset{ChangeLogFile::kDefaultBlock0Offset},
    };

    BATT_REQUIRE_OK(ChangeLogFile::create(dir_path / change_log_file_name(),  //
                                          change_log_config,                  //
                                          RemoveExisting{false}));
  }

  // Create the checkpoint log volume file.
  //
  BATT_REQUIRE_OK(create_checkpoint_log(storage_context,  //
                                        tree_options,     //
                                        dir_path / checkpoint_log_file_name()));

  // Create page files for leaves, nodes, and filters.
  {
    const llfs::PageCount leaf_count{kv_store_config.initial_capacity_bytes /
                                     tree_options.leaf_size()};

    const llfs::PageCount max_leaf_count{kv_store_config.max_capacity_bytes /
                                         tree_options.leaf_size()};

    const llfs::PageCount node_count{leaf_count / 2};

    const llfs::PageCount max_node_count{max_leaf_count / 2};

    const llfs::PageCount filter_count{leaf_count};

    const llfs::PageCount max_filter_count{max_leaf_count};

    BATT_REQUIRE_OK(create_page_file(storage_context,
                                     PageFileSpec{
                                         .filename = dir_path / leaf_page_file_name(),
                                         .initial_page_count = leaf_count,
                                         .max_page_count = max_leaf_count,
                                         .page_size = tree_options.leaf_size(),
                                     },
                                     RemoveExisting{false},
                                     /*device_id=*/0));

    BATT_REQUIRE_OK(create_page_file(storage_context,
                                     PageFileSpec{
                                         .filename = dir_path / node_page_file_name(),
                                         .initial_page_count = node_count,
                                         .max_page_count = max_node_count,
                                         .page_size = tree_options.node_size(),
                                     },
                                     RemoveExisting{false},
                                     /*device_id=*/1));

    BATT_REQUIRE_OK(create_page_file(storage_context,
                                     PageFileSpec{
                                         .filename = dir_path / filter_page_file_name(),
                                         .initial_page_count = filter_count,
                                         .max_page_count = max_filter_count,
                                         .page_size = tree_options.filter_page_size(),
                                     },
                                     RemoveExisting{false},
                                     /*device_id=*/2));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<KVStore>> KVStore::open(
    const std::filesystem::path& dir_path,
    const TreeOptions& tree_options,
    Optional<RuntimeOptions> runtime_options) noexcept
{
  BATT_ASSIGN_OK_RESULT(
      llfs::ScopedIoRing scoped_io_ring,
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096}, llfs::ThreadPoolSize{1}));

  boost::intrusive_ptr<llfs::StorageContext> storage_context =
      llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(),
                                        scoped_io_ring.get_io_ring());

  if (!runtime_options) {
    runtime_options = RuntimeOptions::with_default_values();
  }

  BATT_REQUIRE_OK(
      KVStore::configure_storage_context(*storage_context, tree_options, *runtime_options));

  return KVStore::open(batt::Runtime::instance().default_scheduler(),
                       batt::WorkerPool::default_pool(),
                       *storage_context,
                       dir_path,
                       tree_options,
                       runtime_options,
                       std::move(scoped_io_ring));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<KVStore>> KVStore::open(
    batt::TaskScheduler& task_scheduler,
    batt::WorkerPool& worker_pool,
    llfs::StorageContext& storage_context,
    const std::filesystem::path& dir_path,
    const TreeOptions& tree_options,
    Optional<RuntimeOptions> runtime_options,
    llfs::ScopedIoRing&& scoped_io_ring) noexcept
{
  BATT_REQUIRE_OK(storage_context.add_existing_named_file(dir_path / leaf_page_file_name()));
  BATT_REQUIRE_OK(storage_context.add_existing_named_file(dir_path / node_page_file_name()));
  BATT_REQUIRE_OK(storage_context.add_existing_named_file(dir_path / filter_page_file_name()));
  BATT_REQUIRE_OK(storage_context.add_existing_named_file(dir_path / checkpoint_log_file_name()));

  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogWriter> change_log_writer,
                        ChangeLogWriter::open(dir_path / change_log_file_name()));

  change_log_writer->start(task_scheduler.schedule_task());

  BATT_ASSIGN_OK_RESULT(std::unique_ptr<llfs::Volume> checkpoint_log_volume,
                        open_checkpoint_log(storage_context,  //
                                            dir_path / checkpoint_log_file_name()));

  if (!runtime_options) {
    runtime_options = RuntimeOptions::with_default_values();
  }

  return {
      std::unique_ptr<KVStore>{new KVStore{task_scheduler,
                                           worker_pool,
                                           std::move(scoped_io_ring),
                                           storage_context.shared_from_this(),
                                           tree_options,
                                           *runtime_options,
                                           std::move(change_log_writer),
                                           std::move(checkpoint_log_volume)}},
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status KVStore::global_init()
{
  const static Status status = []() -> Status {
#if TURTLE_KV_ENABLE_TCMALLOC
    const bool tune_tcmalloc = getenv_as<bool>("turtlekv_tune_tcmalloc").value_or(false);
    if (tune_tcmalloc) {
      MallocExtension* m_ext = MallocExtension::instance();
      BATT_CHECK_NOT_NULLPTR(m_ext);

      const double release_rate = getenv_as<double>("turtlekv_memory_release_rate").value_or(0);
      const usize thread_cache_mb = getenv_as<usize>("turtlekv_memory_cache_mb").value_or(65536);

      m_ext->SetMemoryReleaseRate(release_rate);
      m_ext->SetNumericProperty("tcmalloc.max_total_thread_cache_bytes", thread_cache_mb * kMiB);

      // Verify/report the properties we just configured.
      //
      usize value = 0;
      BATT_CHECK(m_ext->GetNumericProperty("tcmalloc.max_total_thread_cache_bytes", &value));
      LOG(INFO) << "tcmalloc.max_total_thread_cache_bytes " << value;
      LOG(INFO) << "tcmalloc.memory_release_rate " << m_ext->GetMemoryReleaseRate();
    } else {
      LOG(INFO) << "turtlekv_tune_tcmalloc is NOT enabled";
    }

#if TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING
    const char* turtlekv_heap_profile = std::getenv("turtlekv_heap_profile");
    if (turtlekv_heap_profile) {
      HeapProfilerStart(turtlekv_heap_profile);
      BATT_CHECK_NE(IsHeapProfilerRunning(), 0);
      LOG(INFO) << BATT_INSPECT_STR(turtlekv_heap_profile) << "; heap profiling enabled";
    } else {
      LOG(INFO) << "turtlekv_heap_profile NOT enabled";
    }
#endif  // TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING
#endif  // TURTLE_KV_ENABLE_TCMALLOC

#if 0
    if (getenv_param<turtlekv_use_bloom_filters>() &&
        getenv_param<turtle_kv_use_vector_quotient_filters>()) {
      BATT_PANIC() << "Select either bloom filters or quotient filters, not both!";
    }
    if (!getenv_param<turtlekv_use_bloom_filters>() &&
        !getenv_param<turtle_kv_use_vector_quotient_filters>()) {
      BATT_PANIC() << "Select either bloom filters or quotient filters!";
    }
#endif

    return OkStatus();
  }();

  return status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStore::KVStore(batt::TaskScheduler& task_scheduler,
                              batt::WorkerPool& worker_pool,
                              llfs::ScopedIoRing&& scoped_io_ring,
                              boost::intrusive_ptr<llfs::StorageContext>&& storage_context,
                              const TreeOptions& tree_options,
                              const RuntimeOptions& runtime_options,
                              std::unique_ptr<ChangeLogWriter>&& change_log_writer,
                              std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept
    : metrics_{}
    , task_scheduler_{task_scheduler}
    , worker_pool_{worker_pool}
    , scoped_io_ring_{std::move(scoped_io_ring)}
    , storage_context_{std::move(storage_context)}
    , tree_options_{tree_options}
    , runtime_options_{runtime_options}
    , log_writer_{std::move(change_log_writer)}
    , checkpoint_distance_{this->runtime_options_.initial_checkpoint_distance}
    , checkpoint_log_{std::move(checkpoint_log)}
    , current_epoch_{0}
    , state_{[&] {
      State* state = new State{};
      state->mem_table_.reset(new MemTable{
          this->page_cache(),
          this->metrics_,
          /*max_byte_size=*/this->tree_options_.flush_size(),
          DeltaBatchId{1}.to_mem_table_id(),
      });
      state->base_checkpoint_ = Checkpoint::empty_at_batch(DeltaBatchId::from_u64(0));
      state->base_checkpoint_.tree()->lock();

      BATT_CHECK_EQ(state->use_count(), 0);
      intrusive_ptr_add_ref(state);
      BATT_CHECK_EQ(state->use_count(), 1);

      return state;
    }()}

    , deltas_size_{0}

    , checkpoint_token_pool_{std::make_shared<batt::Grant::Issuer>(
          /*max_concurrent_checkpoint_jobs=*/1)}

    , info_task_{this->task_scheduler_.schedule_task(),
                 [this] {
                   this->info_task_main();
                 },
                 "KVStore::info_task"}

    , memtable_compact_channels_storage_{new PipelineChannel<
          boost::intrusive_ptr<MemTable>>[this->runtime_options_.memtable_compact_threads]}

    , memtable_compact_channels_{as_slice(this->memtable_compact_channels_storage_.get(),
                                          this->runtime_options_.memtable_compact_threads)}

    , checkpoint_generator_{this->worker_pool_,
                            this->tree_options_,
                            this->page_cache(),
                            this->state_.load()->base_checkpoint_.clone(),
                            *this->checkpoint_log_}

    , checkpoint_batch_count_{0}
{
  BATT_CHECK_EQ(this->state_.load()->use_count(), 1);

  this->tree_options_.set_trie_index_reserve_size(this->tree_options_.trie_index_reserve_size());

  BATT_CHECK_OK(KVStore::global_init());

  BATT_CHECK_OK(NodePageView::register_layout(this->page_cache()));
  BATT_CHECK_OK(LeafPageView::register_layout(this->page_cache()));
  BATT_CHECK_OK(llfs::BloomFilterPageView::register_layout(this->page_cache()));

  if (this->tree_options_.filter_bits_per_key() != 0) {
    BATT_CHECK_OK(llfs::BloomFilterPageView::register_layout(this->page_cache()));
    BATT_CHECK_OK(VqfFilterPageView::register_layout(this->page_cache()));

    Status status = this->page_cache().assign_paired_device(this->tree_options_.leaf_size(),
                                                            kPairedFilterForLeaf,
                                                            this->tree_options_.filter_page_size());

    if (!status.ok()) {
      LOG(WARNING) << "Failed to assign filter device: " << BATT_INSPECT(status);
    }
  }

  if (this->runtime_options_.use_threaded_checkpoint_pipeline) {
    for (usize i = 0; i < this->runtime_options_.memtable_compact_threads; ++i) {
      this->memtable_compact_threads_.emplace_back([this, i] {
        this->memtable_compact_thread_main(i);
      });
    }
    this->checkpoint_update_thread_.emplace([this] {
      this->checkpoint_update_thread_main();
    });
    this->checkpoint_flush_thread_.emplace([this] {
      this->checkpoint_flush_thread_main();
    });
  }
  this->epoch_thread_.emplace([this] {
    this->epoch_thread_main();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStore::~KVStore() noexcept
{
  {
    auto& merge_compactor = MergeCompactor::metrics();
    LOG(INFO) << BATT_INSPECT(merge_compactor.output_buffer_waste);
    LOG(INFO) << BATT_INSPECT(merge_compactor.result_set_waste);
    LOG(INFO) << BATT_INSPECT(merge_compactor.result_set_compact_count);
    LOG(INFO) << BATT_INSPECT(merge_compactor.result_set_compact_byte_count);
    LOG(INFO) << BATT_INSPECT(merge_compactor.average_bytes_per_compaction());
  }
  {
    auto& kv_store = this->metrics();
    LOG(INFO) << BATT_INSPECT(kv_store.obsolete_state_count_stats);
  }
  {
    auto& art = ARTBase::metrics();
    LOG(INFO) << BATT_INSPECT(art.byte_alloc_count) << BATT_INSPECT(art.construct_count)
              << BATT_INSPECT(art.destruct_count) << BATT_INSPECT(art.bytes_per_instance())
              << BATT_INSPECT(art.insert_count) << BATT_INSPECT(art.average_item_count())
              << BATT_INSPECT(art.bytes_per_insert());
  }
  {
    auto& leaf = PackedLeafPage::metrics();
    LOG(INFO) << BATT_INSPECT(leaf.page_utilization_pct_stats)
              << BATT_INSPECT(leaf.packed_size_stats)
              << BATT_INSPECT(leaf.packed_trie_wasted_stats);
  }

  this->halt();
  this->join();

  this->reset_thread_context();

  const State* final_state = this->state_.exchange(nullptr);
  if (final_state) {
    BATT_CHECK_GT(final_state->use_count(), 0);
    intrusive_ptr_release(final_state);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::halt()
{
  this->halt_.set_value(true);
  this->log_writer_->halt();
  for (PipelineChannel<boost::intrusive_ptr<MemTable>>& channel :
       this->memtable_compact_channels_) {
    channel.close();
  }
  this->checkpoint_update_channel_.close();
  this->checkpoint_flush_channel_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::join()
{
  this->log_writer_->join();
  for (std::thread& t : this->memtable_compact_threads_) {
    t.join();
  }
  this->memtable_compact_threads_.clear();
  if (this->checkpoint_update_thread_) {
    this->checkpoint_update_thread_->join();
    this->checkpoint_update_thread_ = None;
  }
  if (this->checkpoint_flush_thread_) {
    this->checkpoint_flush_thread_->join();
    this->checkpoint_flush_thread_ = None;
  }
  if (this->epoch_thread_) {
    this->epoch_thread_->join();
    this->epoch_thread_ = None;
  }
  this->info_task_.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::put(const KeyView& key, const ValueView& value) noexcept /*override*/
{
#if TURTLE_KV_PROFILE_UPDATES
  this->metrics_.put_count.add(1);
  LatencyTimer put_timer{Every2ToTheConst<8>{}, this->metrics_.put_latency};
#endif

  // Pin the current state so we can access the active MemTable.
  //
  const State* const observed_state = this->state_.load();
  BATT_CHECK_GT(observed_state->use_count(), 0);

  boost::intrusive_ptr<const State> pinned_state{observed_state};
  BATT_CHECK_GT(observed_state->use_count(), 1);

  MemTable* const observed_mem_table = observed_state->mem_table_.get();
  const u64 observed_mem_table_id = observed_mem_table->id();

  ChangeLogWriter::Context& log_writer_context =
      this->per_thread_.get(this).log_writer_context(observed_mem_table_id);

#if TURTLE_KV_PROFILE_UPDATES
  LatencyTimer put_memtable_timer{Every2ToTheConst<8>{}, this->metrics_.put_memtable_latency};
#endif

  // Insert the key/value pair into the active MemTable; this will also append a change log buffer.
  //
  Status status = observed_mem_table->put(log_writer_context, key, value);

#if TURTLE_KV_PROFILE_UPDATES
  put_memtable_timer.stop();
#endif

  // If the MemTable is too full to accept this update, then finalize the current MemTable and try
  // again.
  //
  if (status == batt::StatusCode::kResourceExhausted) {
#if TURTLE_KV_PROFILE_UPDATES
    this->metrics_.put_memtable_full_count.add(1);
#endif

    BATT_REQUIRE_OK(this->update_checkpoint(observed_state));

#if TURTLE_KV_PROFILE_UPDATES
    LatencyTimer put_wait_trim_timer{this->metrics_.put_wait_trim_latency};
#endif

    // Limit the number of deltas that can build up.
    //
    BATT_REQUIRE_OK(this->deltas_size_->await_true([this](usize n) {
      return n <= this->checkpoint_distance_.load() * 2;
    }));

#if TURTLE_KV_PROFILE_UPDATES
    put_wait_trim_timer.stop();
    this->metrics_.put_retry_count.add(1);
#endif

    // Now that we have a new MemTable with plenty of space, try again.
    //
    return this->put(key, value);
  }

  return status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::force_checkpoint()
{
  const usize saved_checkpoint_distance = this->checkpoint_distance_.exchange(1);
  auto on_scope_exit = batt::finally([&] {
    usize expected = 1;
    this->checkpoint_distance_.compare_exchange_strong(expected, saved_checkpoint_distance);
  });

  const State* const observed_state = this->state_.load();
  boost::intrusive_ptr<const State> pinned_state{observed_state};
  boost::intrusive_ptr<MemTable> pinned_mem_table = observed_state->mem_table_;
  BATT_CHECK_GT(pinned_state->use_count(), 1);

  BATT_REQUIRE_OK(this->update_checkpoint(observed_state));

  BATT_REQUIRE_OK(this->deltas_size_->await_true([this](usize n) {
    return n < 2;
  }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::set_checkpoint_distance(usize chi) noexcept
{
  BATT_CHECK_GT(chi, 0);

  this->checkpoint_distance_.store(chi);

  if (this->runtime_options_.use_threaded_checkpoint_pipeline) {
    this->checkpoint_update_channel_.poke();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::reset_thread_context() noexcept
{
  ThreadContext& thread_context = this->per_thread_.get(this);

  thread_context.query_page_loader.emplace(this->page_cache());
  thread_context.query_result_storage.emplace();
  thread_context.scan_result_storage.emplace();
  thread_context.query_count = 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageLoader& KVStore::ThreadContext::get_page_loader()
{
  const u64 n = query_page_loader_reset_every_n();
  if (!n) {
    return this->page_cache;
  }

  if ((this->query_count & n) == 0) {
    this->query_page_loader.emplace(this->page_cache);
  }

  return *this->query_page_loader;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> KVStore::get(const KeyView& key) noexcept /*override*/
{
  const State* const observed_state = this->state_.load();

  // First check the current active MemTable.
  //
  Optional<ValueView> value =
      TURTLE_KV_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<14>{},
                                       this->metrics_.mem_table_get_latency,
                                       observed_state->mem_table_->get(key));

  const auto return_memtable_value =
      [](Optional<ValueView> mem_table_value,
         FastCountMetric<u64>& get_count_metric) -> StatusOr<ValueView> {
    get_count_metric.add(1);
    if (mem_table_value->is_delete()) {
      return {batt::StatusCode::kNotFound};
    }
    return *mem_table_value;
  };

  if (value) {
    if (!value->needs_combine()) {
      return return_memtable_value(value, this->metrics_.mem_table_get_count);
    }
  }

  // Second, check recently finalized MemTables (the deltas_ stack).
  //
  const usize observed_deltas_size = observed_state->deltas_.size();
  for (usize i = observed_deltas_size; i != 0;) {
    --i;
    const boost::intrusive_ptr<MemTable>& delta = observed_state->deltas_[i];

    Optional<ValueView> delta_value =
        TURTLE_KV_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<18>{},
                                         this->metrics_.delta_get_latency,
                                         delta->finalized_get(key));

    if (delta_value) {
      // VLOG(1) << "found key " << batt::c_str_literal(key) << " in delta MemTable " << i << "/"
      //         << observed_state->deltas_.size();
      if (value) {
        *value = combine(*value, *delta_value);
        if (!value->needs_combine()) {
          return return_memtable_value(
              value,
              this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)]);
        }
      } else {
        if (!delta_value->needs_combine()) {
          return return_memtable_value(
              delta_value,
              this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)]);
        }
        value = delta_value;
      }
    }
  }

  // If we haven't resolved the query by this point, we must search the current checkpoint tree.
  //
  boost::intrusive_ptr<const State> pinned_state{observed_state};
  ThreadContext& thread_context = this->per_thread_.get(this);

  ++thread_context.query_count;
  thread_context.query_result_storage.emplace();

  llfs::PageLoader& page_loader = thread_context.get_page_loader();

  KeyQuery query{
      page_loader,
      *thread_context.query_result_storage,
      this->tree_options_,
      key,
  };

  StatusOr<ValueView> value_from_checkpoint =
      TURTLE_KV_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<12>{},
                                       this->metrics_.checkpoint_get_latency,
                                       pinned_state->base_checkpoint_.find_key(query));

  if (value_from_checkpoint.ok()) {
    // VLOG(1) << "found key " << batt::c_str_literal(key) << " in checkpoint tree";
    this->metrics_.checkpoint_get_count.add(1);
    if (value) {
      return combine(*value, *value_from_checkpoint);
    }
    return *value_from_checkpoint;
  }

  if (value && value_from_checkpoint.status() == batt::StatusCode::kNotFound) {
    return *value;
  }

  // Failed to find a value for the key.
  //
  return value_from_checkpoint.status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> KVStore::scan(
    const KeyView& min_key,
    const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept /*override*/
{
  ThreadContext& thread_context = this->per_thread_.get(this);
  thread_context.scan_result_storage.emplace();

  this->metrics_.scan_count.add(1);

  KVStoreScanner scanner{*this, min_key};
  BATT_REQUIRE_OK(scanner.start());

  return scanner.read(items_out);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> KVStore::scan_keys(const KeyView& min_key,
                                   const Slice<KeyView>& items_out) noexcept /*override*/
{
  ThreadContext& thread_context = this->per_thread_.get(this);
  thread_context.scan_result_storage.emplace();

  this->metrics_.scan_count.add(1);

  KVStoreScanner scanner{*this, min_key};
  BATT_REQUIRE_OK(scanner.start());

  return scanner.read_keys(items_out);
}
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::remove(const KeyView& key) noexcept /*override*/
{
  return this->put(key, ValueView::deleted());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::update_checkpoint(const State* observed_state)
{
#if TURTLE_KV_PROFILE_UPDATES
  LatencyTimer memtable_create_timer{this->metrics_.put_memtable_create_latency};
#endif

  // Gather some information from the current MemTable before we send it off.
  //
  boost::intrusive_ptr<MemTable> old_mem_table = observed_state->mem_table_;
  const DeltaBatchId batch_id = DeltaBatchId::from_mem_table_id(old_mem_table->id());
  const DeltaBatchId next_batch_id = batch_id.next();

  // Create a new state to replace the old.
  //
  State* new_state = new State{};

  BATT_CHECK_EQ(new_state->use_count(), 0);
  intrusive_ptr_add_ref(new_state);
  BATT_CHECK_EQ(new_state->use_count(), 1);

  new_state->mem_table_.reset(new MemTable{
      this->page_cache(),
      this->metrics_,
      /*max_byte_size=*/this->tree_options_.flush_size(),
      next_batch_id.to_mem_table_id(),
  });

  for (;;) {
    // Multiple threads are potentially racing to install a new active MemTable;
    // the important thing is that the old mem table has been replaced.
    //
    if (observed_state->mem_table_ != old_mem_table) {
      BATT_CHECK_NE(observed_state, new_state);
      BATT_CHECK_EQ(new_state->use_count(), 1);
      intrusive_ptr_release(new_state);
      return OkStatus();
    }

    BATT_CHECK(observed_state->base_checkpoint_.tree()->is_serialized());

    new_state->deltas_ = observed_state->deltas_;
    new_state->deltas_.emplace_back(observed_state->mem_table_);
    new_state->base_checkpoint_ = observed_state->base_checkpoint_;

    // Try to swap our new state object for the current one.
    //
    if (this->state_.compare_exchange_weak(observed_state, new_state)) {
      break;
    }
  }
  this->deltas_size_->fetch_add(1);

#if TURTLE_KV_PROFILE_UPDATES
  memtable_create_timer.stop();
#endif

  // Since we successfully exchanged the successor to `observed_state`, when this scope exits we
  // must release the ref count once after adding the old state to the obsolete states list.
  // Eventually it will be cleaned up by the epoch thread.
  //
  auto on_scope_exit = batt::finally([&] {
    this->add_obsolete_state(observed_state);
  });

  // Finalize the old mem table and hand it off to be compacted, applied to checkpoint, etc.
  //
  const bool finalize_ok = old_mem_table->finalize();
  BATT_CHECK(finalize_ok);

  // Wait for any previous MemTables to be consumed by the compactor task.
  //
  const u64 this_mem_table_id = old_mem_table->id();
  const u64 next_mem_table_id = MemTable::next_id_for(this_mem_table_id);
  while (this->next_mem_table_id_to_push_.load() != this_mem_table_id) {
    batt::spin_yield();
  }

  //----- --- -- -  -  -   -
  if (this->runtime_options_.use_threaded_checkpoint_pipeline) {
#if TURTLE_KV_PROFILE_UPDATES
    LatencyTimer queue_push_timer{this->metrics_.put_memtable_queue_push_latency};
#endif

    const usize i = this_mem_table_id % this->memtable_compact_channels_.size();
    BATT_REQUIRE_OK(this->memtable_compact_channels_[i].write(std::move(old_mem_table)));

  } else {
    std::unique_ptr<DeltaBatch> delta_batch = this->compact_memtable(std::move(old_mem_table));

    BATT_ASSIGN_OK_RESULT(std::unique_ptr<CheckpointJob> checkpoint_job,
                          this->apply_batch_to_checkpoint(std::move(delta_batch)));

    if (checkpoint_job) {
      BATT_REQUIRE_OK(this->commit_checkpoint(std::move(checkpoint_job)));
    }
  }
  //----- --- -- -  -  -   -

  // Signal to the next mem table, it is ok to push.
  //
  this->next_mem_table_id_to_push_.store(next_mem_table_id);

  return OkStatus();
}

using CheckpointEvent = llfs::PackedVariant<turtle_kv::PackedCheckpoint>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::StatusOr<turtle_kv::Checkpoint> KVStore::recover_latest_checkpoint(
    llfs::Volume& checkpoint_log_volume)
{
  llfs::StatusOr<llfs::TypedVolumeReader<CheckpointEvent>> reader =
      checkpoint_log_volume.typed_reader<CheckpointEvent>(
          llfs::SlotRangeSpec{
              .lower_bound = llfs::None,
              .upper_bound = llfs::None,
          },
          llfs::LogReadMode::kDurable);

  BATT_REQUIRE_OK(reader);

  std::pair<llfs::SlotParse, turtle_kv::PackedCheckpoint> prev_checkpoint;
  prev_checkpoint.second.batch_upper_bound = 0;

  for (;;) {
    llfs::StatusOr<usize> n_slots_visited = reader->visit_typed_next(
        batt::WaitForResource::kFalse,
        [&prev_checkpoint](const llfs::SlotParse& slot,
                           const turtle_kv::PackedCheckpoint& packed_checkpoint) {
          // Validate that the checkpoints are in ascending order based on batch_upper_bound
          //
          BATT_CHECK_GE(packed_checkpoint.batch_upper_bound,
                        prev_checkpoint.second.batch_upper_bound);
          prev_checkpoint =
              std::pair<llfs::SlotParse, turtle_kv::PackedCheckpoint>{slot, packed_checkpoint};
          return llfs::OkStatus();
        });

    BATT_REQUIRE_OK(n_slots_visited);
    VLOG(2) << "Visited n_slots_visited= " << *n_slots_visited << " checkpoints";
    if (*n_slots_visited == 0) {
      break;
    }
  }

  // Return empty checkpoint if no checkpoints are found
  //
  if (prev_checkpoint.second.batch_upper_bound == 0) {
    return Checkpoint::empty_at_batch(DeltaBatchId::from_u64(0));
  }

  return turtle_kv::Checkpoint::recover(checkpoint_log_volume,
                                        prev_checkpoint.first,
                                        prev_checkpoint.second);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::info_task_main() noexcept
{
  BATT_DEBUG_INFO(this->debug_info());

  this->halt_.await_equal(true).IgnoreError();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::memtable_compact_thread_main(usize thread_i)
{
  Status status = [this, thread_i]() -> Status {
    for (;;) {
      StatusOr<boost::intrusive_ptr<MemTable>> mem_table =
          this->memtable_compact_channels_[thread_i].read();

      BATT_REQUIRE_OK(mem_table);
      BATT_CHECK_NOT_NULLPTR(*mem_table);

      const u64 this_delta_batch_id = (**mem_table).id();
      const u64 next_delta_batch_id = MemTable::next_id_for(this_delta_batch_id);

      std::unique_ptr<DeltaBatch> delta_batch = this->compact_memtable(std::move(*mem_table));

      while (this->next_delta_batch_to_push_.load() != this_delta_batch_id) {
        batt::spin_yield();
      }

      BATT_REQUIRE_OK(this->checkpoint_update_channel_.write(std::move(delta_batch)));

      this->next_delta_batch_to_push_.store(next_delta_batch_id);
    }
  }();

  LOG(INFO) << "memtable_compact_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<DeltaBatch> KVStore::compact_memtable(boost::intrusive_ptr<MemTable>&& mem_table)
{
  // Convert the MemTable to a DeltaBatch; this compacts all updates per key.
  //
  std::unique_ptr<DeltaBatch> delta_batch = std::make_unique<DeltaBatch>(std::move(mem_table));

  TURTLE_KV_COLLECT_LATENCY(this->metrics_.compact_batch_latency,
                            delta_batch->merge_compact_edits());

  this->metrics_.batch_count.add(1);
  this->metrics_.batch_edits_count.add(delta_batch->result_set_size());

  return delta_batch;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::checkpoint_update_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      StatusOr<std::unique_ptr<DeltaBatch>> delta_batch = this->checkpoint_update_channel_.read();
      if (delta_batch.status() == batt::StatusCode::kPoke) {
        delta_batch = std::unique_ptr<DeltaBatch>{nullptr};
      }
      BATT_REQUIRE_OK(delta_batch);

      BATT_ASSIGN_OK_RESULT(std::unique_ptr<CheckpointJob> checkpoint_job,
                            this->apply_batch_to_checkpoint(std::move(*delta_batch)));

      if (checkpoint_job) {
        const auto& page_cache_job = checkpoint_job->job();
        VLOG(1) << BATT_INSPECT(page_cache_job.new_page_count())
                << BATT_INSPECT(page_cache_job.pinned_page_count());

        BATT_REQUIRE_OK(this->checkpoint_flush_channel_.write(std::move(checkpoint_job)));
      }
    }
  }();

  LOG(INFO) << "checkpoint_update_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<CheckpointJob>> KVStore::apply_batch_to_checkpoint(
    std::unique_ptr<DeltaBatch>&& delta_batch)
{
  // Allow the page cache to be temporarily overcommitted so we don't deadlock; but if overcommit is
  // triggered, we immediately truncate the checkpoint to try to unpin pages ASAP.
  //
  llfs::PageCacheOvercommit overcommit;
  overcommit.allow(true);
  BATT_CHECK(!overcommit.is_triggered());

  // A MemTable has filled up.
  //
  if (delta_batch) {
    // Apply the finalized MemTable to the current checkpoint (in-memory).
    //
    StatusOr<usize> push_status = TURTLE_KV_COLLECT_LATENCY(
        this->metrics_.apply_batch_latency,
        this->checkpoint_generator_.apply_batch(std::move(delta_batch), overcommit));

    BATT_REQUIRE_OK(push_status);
    BATT_CHECK_EQ(*push_status, 1);

    this->checkpoint_batch_count_ += 1;
  }

  // If the batch count is below the checkpoint distance, we are done.
  //
  if (!overcommit.is_triggered() && !this->should_create_checkpoint()) {
    return nullptr;
  }

  if (overcommit.is_triggered()) {
    on_page_cache_overcommit(
        [this](std::ostream& out) {
          out << "Finalizing checkpoint early due to cache overcommit;"
              << BATT_INSPECT(this->checkpoint_batch_count_);
        },
        this->page_cache(),
        this->metrics_.overcommit);
  }

  // Else if we have reached the target checkpoint distance, then flush the checkpoint and start a
  // new one.
  //
  this->metrics_.checkpoint_count.add(1);
  this->metrics_.checkpoint_pinned_pages_stats.update(
      this->checkpoint_generator_.page_cache_job().pinned_page_count());

  // Allocate a token for the checkpoint job.
  //
  BATT_ASSIGN_OK_RESULT(batt::Grant checkpoint_token,
                        this->checkpoint_token_pool_->issue_grant(1, batt::WaitForResource::kTrue));

  // Serialize all pages and create the job.
  //
  StatusOr<std::unique_ptr<CheckpointJob>> checkpoint_job = TURTLE_KV_COLLECT_LATENCY(
      this->metrics_.finalize_checkpoint_latency,
      this->checkpoint_generator_.finalize_checkpoint(std::move(checkpoint_token),
                                                      batt::make_copy(this->checkpoint_token_pool_),
                                                      overcommit));

  BATT_REQUIRE_OK(checkpoint_job);

  BATT_CHECK_NOT_NULLPTR(*checkpoint_job);
  BATT_CHECK((*checkpoint_job)->append_job_grant);
  BATT_CHECK((*checkpoint_job)->appendable_job);
  BATT_CHECK((*checkpoint_job)->prepare_slot_sequencer);
  BATT_CHECK_NE((*checkpoint_job)->batch_count, 0);
  BATT_CHECK_EQ((*checkpoint_job)->batch_count, this->checkpoint_batch_count_);

  // Number of batches in the current checkpoint resets to zero.
  //
  this->checkpoint_batch_count_ = 0;

  return checkpoint_job;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::checkpoint_flush_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      BATT_ASSIGN_OK_RESULT(std::unique_ptr<CheckpointJob> checkpoint_job,
                            this->checkpoint_flush_channel_.read());

      BATT_REQUIRE_OK(this->commit_checkpoint(std::move(checkpoint_job)));
    }
  }();

  LOG(INFO) << "checkpoint_flush_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::commit_checkpoint(std::unique_ptr<CheckpointJob>&& checkpoint_job)
{
  // Durably commit the checkpoint.
  //
  StatusOr<llfs::SlotRange> checkpoint_slot_range =
      TURTLE_KV_COLLECT_LATENCY(this->metrics_.append_job_latency,                     //
                                this->checkpoint_log_->append(                         //
                                    std::move(*checkpoint_job->appendable_job),        //
                                    *checkpoint_job->append_job_grant,                 //
                                    std::move(checkpoint_job->prepare_slot_sequencer)  //
                                    ));

  BATT_REQUIRE_OK(checkpoint_slot_range);

  // Lock the new slot range.
  //
  StatusOr<llfs::SlotReadLock> slot_read_lock = this->checkpoint_log_->lock_slots(
      llfs::SlotRangeSpec::from(*checkpoint_slot_range),
      llfs::LogReadMode::kSpeculative,
      /*lock_holder=*/"TabletCheckpointTask::handle_checkpoint_commit");

  BATT_REQUIRE_OK(slot_read_lock);

  // Update the base checkpoint and clear deltas.
  //
  Optional<llfs::slot_offset_type> prev_checkpoint_slot;
  {
    State* const new_state = new State{};

    BATT_CHECK_EQ(new_state->use_count(), 0);
    intrusive_ptr_add_ref(new_state);
    BATT_CHECK_EQ(new_state->use_count(), 1);

    new_state->base_checkpoint_ = std::move(*checkpoint_job->checkpoint);
    new_state->base_checkpoint_.notify_durable(std::move(*slot_read_lock));
    new_state->base_checkpoint_.tree()->lock();

    BATT_CHECK(new_state->base_checkpoint_.tree()->is_serialized());

    const State* old_state = this->state_.load();

    BATT_CHECK(old_state->base_checkpoint_.tree()->is_serialized());

    // Save the old checkpoint's slot lower bound so we can trim to it later.
    //
    Optional<llfs::SlotRange> prev_slot_range = old_state->base_checkpoint_.slot_range();
    if (prev_slot_range) {
      prev_checkpoint_slot = prev_slot_range->upper_bound;
    }

    // CAS-loop to update the state object.
    //
    for (;;) {
      if (old_state == new_state) {
        break;
      }

      new_state->mem_table_ = old_state->mem_table_;

      // Remove the deltas covered by the new base checkpoint.
      //
      BATT_CHECK_LE(checkpoint_job->batch_count, old_state->deltas_.size());
      new_state->deltas_.assign(old_state->deltas_.begin() + checkpoint_job->batch_count,
                                old_state->deltas_.end());

      if (this->state_.compare_exchange_weak(old_state, new_state)) {
        break;
      }
    }
    if (old_state != new_state) {
      this->add_obsolete_state(old_state);
    }
    this->deltas_size_->fetch_sub(checkpoint_job->batch_count);
  }

  // Trim the checkpoint volume to free old pages.
  //
  if (prev_checkpoint_slot) {
    BATT_REQUIRE_OK(this->checkpoint_log_->trim(*prev_checkpoint_slot));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::add_obsolete_state(const State* old_state)
{
  const i64 expires_at_epoch = this->current_epoch_.load() + 10;  // 3;

  BATT_CHECK(!old_state->last_epoch_);
  //----- --- -- -  -  -   -
  old_state->last_epoch_ = expires_at_epoch;
  //----- --- -- -  -  -   -
  BATT_CHECK(old_state->last_epoch_);
  BATT_CHECK_EQ(*old_state->last_epoch_, expires_at_epoch);
  {
    absl::MutexLock lock{&this->obsolete_states_mutex_};
    this->obsolete_states_.emplace_back(old_state);
    BATT_CHECK_GT(old_state->use_count(), 1);
  }
  intrusive_ptr_release(old_state);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::epoch_thread_main()
{
  constexpr i64 kMinEpochUsec = 125000;
  constexpr i64 kMaxEpochUsec = 250000;

  std::default_random_engine rng{std::random_device{}()};
  std::uniform_int_distribution<i64> pick_delay_usec{kMinEpochUsec, kMaxEpochUsec};

  while (!this->halt_.get_value()) {
    std::this_thread::sleep_for(std::chrono::microseconds{pick_delay_usec(rng)});
    this->current_epoch_.fetch_add(1);

    std::vector<boost::intrusive_ptr<const State>> to_delete;
    {
      absl::MutexLock lock{&this->obsolete_states_mutex_};

      this->metrics().obsolete_state_count_stats.update(this->obsolete_states_.size());

      for (usize i = 0; i < this->obsolete_states_.size();) {
        BATT_CHECK(this->obsolete_states_[i]->last_epoch_);
        if (*this->obsolete_states_[i]->last_epoch_ - this->current_epoch_ < 0) {
          to_delete.emplace_back(std::move(this->obsolete_states_[i]));
          this->obsolete_states_[i] = std::move(this->obsolete_states_.back());
          this->obsolete_states_.pop_back();
        } else {
          ++i;
        }
      }
    }

    // (done explicitly for emphasis)
    //
    to_delete.clear();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::collect_stats(
    std::function<void(std::string_view /*name*/, double /*value*/)> fn) const noexcept
{
  //----- --- -- -  -  -   -
  const auto emit_latency = [&fn](std::string_view name, const LatencyMetric& metric) {
    fn(batt::to_string(name, ".count"), metric.count.get());
    fn(batt::to_string(name, ".seconds"), metric.total_seconds());
  };
  //----- --- -- -  -  -   -
  const auto emit_stats = [&fn](std::string_view name, const auto& metric) {
    fn(batt::to_string(name, ".count"), metric.count());
    fn(batt::to_string(name, ".total"), metric.total());
    fn(batt::to_string(name, ".min"), metric.min());
    fn(batt::to_string(name, ".max"), metric.max());
  };
  //----- --- -- -  -  -   -

  auto& kv_store = this->metrics_;
  auto& checkpoint_log = *this->checkpoint_log_;
  auto& cache = checkpoint_log.cache();
  auto& change_log_file = this->log_writer_->change_log_file();
  auto& change_log_writer = this->log_writer_->metrics();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageCacheSlot::Pool metrics.
  {
    auto& cache_slot_pool = llfs::PageCacheSlot::Pool::Metrics::instance();

    fn("page_cache.query_count", cache_slot_pool.query_count.get());
    fn("page_cache.hit_count", cache_slot_pool.hit_count.get());
    fn("page_cache.miss_count", cache_slot_pool.miss_count.get());
    fn("page_cache.insert_count", cache_slot_pool.insert_count.get());
    fn("page_cache.erase_count", cache_slot_pool.erase_count.get());
    fn("page_cache.admit_bytes", cache_slot_pool.admit_byte_count.get());
    fn("page_cache.erase_bytes", cache_slot_pool.erase_byte_count.get());
    fn("page_cache.evict_bytes", cache_slot_pool.evict_byte_count.get());
    fn("page_cache.pinned_bytes", cache_slot_pool.pinned_byte_count.get());
    fn("page_cache.unpinned_bytes", cache_slot_pool.unpinned_byte_count.get());
    fn("page_cache.slot_alloc.count", cache_slot_pool.allocate_count.get());
    fn("page_cache.slot_alloc.free_queue.count", cache_slot_pool.allocate_free_queue_count.get());
    fn("page_cache.slot_alloc.construct.count", cache_slot_pool.allocate_construct_count.get());
    fn("page_cache.slot_alloc.evict.count", cache_slot_pool.allocate_evict_count.get());
  }

  auto& page_cache = cache.metrics();

  fn("page_cache.get_count", page_cache.get_count.get());

  emit_latency("page_cache.allocate_page_latency", page_cache.allocate_page_alloc_latency);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Page cache overcommit metrics.
  //
  fn("page_cache.overcommit.trigger_count", this->metrics_.overcommit.trigger_count.get());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Tree Node related stats.
  //
  {
    InMemoryNode::Metrics& node = InMemoryNode::metrics();

    fn("kv_store.tree.node_count", node.serialized_node_count.get());
    fn("kv_store.tree.pivot_count", node.serialized_pivot_count.get());
    fn("kv_store.tree.buffer_segment_count", node.serialized_buffer_segment_count.get());
    fn("kv_store.tree.nonempty_level_count", node.serialized_nonempty_level_count.get());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageDevice metrics.
  //
  {
    static constexpr usize kMinPageSizeBits = 12;
    static constexpr usize kMaxPageSizeBits = 26;
    static const std::array<std::string_view, kMaxPageSizeBits - kMinPageSizeBits + 1>
        page_size_name = {
            "004kb",
            "008kb",
            "016kb",
            "032kb",
            "064kb",
            "128kb",
            "256kb",
            "512kb",
            "001mb",
            "002mb",
            "004mb",
            "008mb",
            "016mb",
            "032mb",
            "064mb",
        };

    auto& page_device = llfs::PageDeviceMetrics::instance();

    for (usize page_size_bits = kMinPageSizeBits; page_size_bits <= kMaxPageSizeBits;
         ++page_size_bits) {
      //----- --- -- -  -  -   -
      // Report read/write count for all page sizes.
      //
      fn(batt::to_string("page_device.",
                         page_size_name[page_size_bits - kMinPageSizeBits],
                         ".read_count"),
         page_device.read_count_per_page_size_log2[page_size_bits].get());

      fn(batt::to_string("page_device.",
                         page_size_name[page_size_bits - kMinPageSizeBits],
                         ".write_count"),
         page_device.write_count_per_page_size_log2[page_size_bits].get());
    }
  }

  [[maybe_unused]] const auto print_page_alloc_info = [&](std::ostream& out) {
    for (const llfs::PageDeviceEntry* entry : cache.all_devices()) {
      if (!entry->can_alloc || !entry->arena.has_allocator()) {
        continue;
      }
      out << entry->arena.allocator().debug_info() << "\n";
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Footprint metrics.
  //
  u64 page_bytes_in_use = 0;
  {
    const auto leaf_size = this->tree_options_.leaf_size();
    const auto filter_size = this->tree_options_.filter_page_size();

    for (const llfs::PageDeviceEntry* entry : cache.all_devices()) {
      if (!entry->can_alloc || !entry->arena.has_allocator()) {
        continue;
      }
      page_bytes_in_use += entry->arena.allocator().in_use_bytes();
      if (entry->arena.allocator().page_size() == leaf_size) {
        page_bytes_in_use += entry->arena.allocator().in_use_count() * filter_size;
      }
    }
  }
  const u64 on_disk_footprint = page_bytes_in_use + change_log_file.size();

  [[maybe_unused]] const double space_amp =
      (double)on_disk_footprint / (double)change_log_writer.received_user_byte_count.get();

  fn("kv_store.footprint.page_bytes", page_bytes_in_use);
  fn("kv_store.footprint.log_bytes", change_log_file.size());
  fn("kv_store.footprint.total_bytes", on_disk_footprint);
  fn("kv_store.log.user_bytes", change_log_writer.received_user_byte_count.get());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Query profiling metrics.
  //
  fn("kv_store.mem_table_get.count", kv_store.mem_table_get_count.get());
  fn("kv_store.delta_001_get.count", kv_store.delta_log2_get_count[0].get());
  fn("kv_store.delta_002_get.count", kv_store.delta_log2_get_count[1].get());
  fn("kv_store.delta_004_get.count", kv_store.delta_log2_get_count[2].get());
  fn("kv_store.delta_008_get.count", kv_store.delta_log2_get_count[3].get());
  fn("kv_store.delta_016_get.count", kv_store.delta_log2_get_count[4].get());
  fn("kv_store.delta_032_get.count", kv_store.delta_log2_get_count[5].get());
  fn("kv_store.delta_064_get.count", kv_store.delta_log2_get_count[6].get());
  fn("kv_store.delta_128_get.count", kv_store.delta_log2_get_count[7].get());
  fn("kv_store.delta_256_get.count", kv_store.delta_log2_get_count[8].get());
  fn("kv_store.delta_512_get.count", kv_store.delta_log2_get_count[9].get());
  fn("kv_store.checkpoint_get.count", kv_store.checkpoint_get_count.get());

  emit_latency("kv_store.delta_get_latency", kv_store.delta_get_latency);
  emit_latency("kv_store.checkpoint_get_latency", kv_store.checkpoint_get_latency);

  fn("leaf.find_key_success.count", PackedLeafPage::metrics().find_key_success_count.get());
  fn("leaf.find_key_failure.count", PackedLeafPage::metrics().find_key_failure_count.get());

  emit_latency("leaf.find_key_latency", PackedLeafPage::metrics().find_key_latency);

  {
    auto& query_page_loader = PinningPageLoader::metrics();

    emit_latency("kv_store.query_page_loader.prefetch_hint_latency",
                 query_page_loader.prefetch_hint_latency);
    emit_latency("kv_store.query_page_loader.hash_map_lookup_latency",
                 query_page_loader.hash_map_lookup_latency);
    emit_latency("kv_store.query_page_loader.get_page_from_cache_latency",
                 query_page_loader.get_page_from_cache_latency);

    fn("kv_store.query_page_loader.get_page_count", query_page_loader.get_page_count.get());
    fn("kv_store.query_page_loader.hash_map_miss_count",
       query_page_loader.hash_map_miss_count.get());
  }

  fn("kv_store.scan_count", kv_store.scan_count.get());

  //----- --- -- -  -  -   -
  // KVStoreScanner metrics.
  //
  {
    [[maybe_unused]] auto& scanner = KVStoreScanner::metrics();
    [[maybe_unused]] auto& sharded_level_scanner = ShardedLevelScannerMetrics::instance();
    /* TODO [tastolfi 2025-11-25]
  << BATT_INSPECT(scanner.ctor_latency) << "\n"                                  //
  << BATT_INSPECT(scanner.ctor_count) << "\n"                                    //
  << "\n"                                                                        //
  << BATT_INSPECT(scanner.start_count) << "\n"                                   //
  << BATT_INSPECT(scanner.start_latency) << "\n"                                 //
  << BATT_INSPECT(scanner.start_deltas_latency) << "\n"                          //
  << BATT_INSPECT(scanner.start_enter_subtree_latency) << "\n"                   //
  << BATT_INSPECT(scanner.start_resume_latency) << "\n"                          //
  << BATT_INSPECT(scanner.start_build_heap_latency) << "\n"                      //
  << "\n"                                                                        //
  << BATT_INSPECT(scanner.init_heap_size_stats) << "\n"                          //
  << "\n"                                                                        //
  << BATT_INSPECT(scanner.next_latency) << "\n"                                  //
  << BATT_INSPECT(scanner.next_count) << "\n"                                    //
  << BATT_INSPECT(scanner.heap_insert_latency) << "\n"                           //
  << BATT_INSPECT(scanner.heap_update_latency) << "\n"                           //
  << BATT_INSPECT(scanner.heap_remove_latency) << "\n"                           //
  << BATT_INSPECT(scanner.art_advance_latency) << "\n"                           //
  << BATT_INSPECT(scanner.art_advance_count) << "\n"                             //
  << BATT_INSPECT(scanner.scan_level_advance_latency) << "\n"                    //
  << BATT_INSPECT(scanner.scan_level_advance_count) << "\n"                      //
  << BATT_INSPECT(scanner.pull_next_sharded_latency) << "\n"                     //
  << BATT_INSPECT(scanner.pull_next_sharded_count) << "\n"                       //
  << BATT_INSPECT(scanner.full_leaf_attempts) << "\n"                            //
  << BATT_INSPECT(scanner.full_leaf_success) << "\n"                             //
  << "\n"                                                                        //
  << BATT_INSPECT(sharded_level_scanner.full_page_attempts) << "\n"              //
  << BATT_INSPECT(sharded_level_scanner.full_page_success) << "\n"               //
     */
  }

  {
    auto& key_query = KeyQuery::metrics();

    emit_latency("key_query.reject_page_latency", key_query.reject_page_latency);
    emit_latency("key_query.filter_lookup_latency", key_query.filter_lookup_latency);

    fn("key_query.total_filter_query_count", key_query.total_filter_query_count.get());
    fn("key_query.no_filter_page_count", key_query.no_filter_page_count.get());
    fn("key_query.filter_page_load_failed_count", key_query.filter_page_load_failed_count.get());
    fn("key_query.page_id_mismatch_count", key_query.page_id_mismatch_count.get());
    fn("key_query.filter_reject_count", key_query.filter_reject_count.get());
    fn("key_query.try_pin_leaf_count", key_query.try_pin_leaf_count.get());
    fn("key_query.try_pin_leaf_success_count", key_query.try_pin_leaf_success_count.get());
    fn("key_query.sharded_view_find_count", key_query.sharded_view_find_count.get());
    fn("key_query.sharded_view_find_success_count",
       key_query.sharded_view_find_success_count.get());
    fn("key_query.filter_positive_count", key_query.filter_positive_count.get());
    fn("key_query.filter_false_positive_count", key_query.filter_false_positive_count.get());
  }

#if TURTLE_KV_PROFILE_UPDATES
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Update profiling metrics.
  //
  fn("kv_store.put.count", kv_store.put_count.get());
  fn("kv_store.put_retry.count", kv_store.put_retry_count.get());
  fn("kv_store.put_memtable_full.count", kv_store.put_memtable_full_count.get());
  emit_latency("kv_store.put_latency", kv_store.put_latency);
  emit_latency("kv_store.put_memtable_latency", kv_store.put_memtable_latency);
  emit_latency("kv_store.put_wait_trim_latency", kv_store.put_wait_trim_latency);
  emit_latency("kv_store.put_memtable_create_latency", kv_store.put_memtable_create_latency);
  emit_latency("kv_store.put_memtable_queue_push_latency",
               kv_store.put_memtable_queue_push_latency);

#endif  // TURTLE_KV_PROFILE_UPDATES

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Checkpoint metrics.
  //
  {
    auto& checkpoint = this->checkpoint_generator_.metrics();

    fn("kv_store.checkpoint.count", kv_store.checkpoint_count.get());
    fn("kv_store.batch.count", kv_store.batch_edits_count.get());
    fn("kv_store.batch_edits.count", kv_store.batch_edits_count.get());
    emit_latency("kv_store.compact_batch_latency", kv_store.compact_batch_latency);
    emit_latency("kv_store.apply_batch_latency", kv_store.apply_batch_latency);
    emit_latency("kv_store.finalize_checkpoint_latency", kv_store.finalize_checkpoint_latency);
    emit_latency("kv_store.append_job_latency", kv_store.append_job_latency);

    for (usize i = 0; i < SubtreeMetrics::kMaxTreeHeight + 1; ++i) {
      const double value = Subtree::metrics().batch_count_per_height[i].get();
      fn(batt::to_string("subtree.batch_count_height_", std::setw(2), std::setfill('0'), i), value);
    }

    emit_latency("checkpoint.force_flush_all_latency", checkpoint.force_flush_all_latency);

#if TURTLE_KV_PROFILE_UPDATES

    emit_latency("checkpoint.serialize_latency", checkpoint.serialize_latency);

    // Checkpoint -> Batch Update Metrics
    //
    fn("checkpoint.batch_update.merge_compact.count",
       checkpoint.batch_update.merge_compact_count.get());

    fn("checkpoint.batch_update.merge_compact.failures",
       checkpoint.batch_update.merge_compact_failures.get());

    fn("checkpoint.batch_update.merge_compact.key_count",
       checkpoint.batch_update.merge_compact_key_count.get());

    fn("checkpoint.batch_update.running_total.count",
       checkpoint.batch_update.running_total_count.get());

    fn("checkpoint.batch_update.flush.count",  //
       checkpoint.batch_update.flush_count.get());

    fn("checkpoint.batch_update.split.count",  //
       checkpoint.batch_update.split_count.get());

    emit_latency("checkpoint.batch_update.merge_compact_latency",
                 checkpoint.batch_update.merge_compact_latency);

    emit_latency("checkpoint.batch_update.running_total_latency",
                 checkpoint.batch_update.running_total_latency);

#endif  // TURTLE_KV_PROFILE_UPDATES
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // MergeCompactor metrics.
  //
  auto& merge_compactor = MergeCompactor::metrics();

  fn("result_set.compact.count", merge_compactor.result_set_compact_count.get());
  fn("result_set.compact_bytes.count", merge_compactor.result_set_compact_byte_count.get());
#if TURTLE_KV_PROFILE_UPDATES
  emit_latency("result_set.compact_latency", merge_compactor.compact_latency);
#endif  // TURTLE_KV_PROFILE_UPDATES

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Bloom Filter metrics.
  //
  {
    auto& bloom_filter = BloomFilterMetrics::instance();

    emit_stats("bloom_filter.word_count_stats", bloom_filter.word_count_stats);
    emit_stats("bloom_filter.byte_size_stats", bloom_filter.byte_size_stats);
    emit_stats("bloom_filter.bit_size_stats", bloom_filter.bit_size_stats);
    emit_stats("bloom_filter.bit_count_stats", bloom_filter.bit_count_stats);
    emit_stats("bloom_filter.item_count_stats", bloom_filter.item_count_stats);
    emit_latency("bloom_filter.build_page_latency", bloom_filter.build_page_latency);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Quotient Filter metrics.
  //
  {
    auto& quotient_filter = QuotientFilterMetrics::instance();

    emit_stats("quotient_filter.byte_size_stats", quotient_filter.byte_size_stats);
    emit_stats("quotient_filter.bit_size_stats", quotient_filter.bit_size_stats);
    emit_stats("quotient_filter.item_count_stats", quotient_filter.item_count_stats);
    emit_stats("quotient_filter.bits_per_key_stats", quotient_filter.bits_per_key_stats);
    emit_latency("quotient_filter.build_page_latency", quotient_filter.build_page_latency);
  }

#if 0
  << "\n"                                                                        //
  << BATT_INSPECT(node.level_depth_stats) << "\n"                                //
  << "\n"                                                                        //
  << BATT_INSPECT(kv_store.scan_init_latency) << "\n"                            //
  << "\n"                                                                        //
  << BATT_INSPECT(checkpoint_log.root_log_space()) << "\n"                       //
  << BATT_INSPECT(checkpoint_log.root_log_size()) << "\n"                        //
  << BATT_INSPECT(checkpoint_log.root_log_capacity()) << "\n"                    //
  << "\n"                                                                        //
  << BATT_INSPECT(change_log_file.active_blocks()) << "\n"                       //
  << BATT_INSPECT(change_log_file.active_block_count()) << "\n"                  //
  << BATT_INSPECT(change_log_file.config().block_count) << "\n"                  //
  << BATT_INSPECT(change_log_file.capacity()) << "\n"                            //
  << BATT_INSPECT(change_log_file.size()) << "\n"                                //
  << BATT_INSPECT(change_log_file.space()) << "\n"                               //
  << BATT_INSPECT(change_log_file.available_block_tokens()) << "\n"              //
  << BATT_INSPECT(change_log_file.in_use_block_tokens()) << "\n"                 //
  << BATT_INSPECT(change_log_file.reserved_block_tokens()) << "\n"               //
  << BATT_INSPECT(change_log_file.metrics().freed_blocks_count) << "\n"          //
  << BATT_INSPECT(change_log_file.metrics().reserved_blocks_count) << "\n"       //
  << "\n"                                                                        //
  << BATT_INSPECT(change_log_writer.received_user_byte_count) << "\n"            //
  << BATT_INSPECT(change_log_writer.received_block_byte_count) << "\n"           //
  << BATT_INSPECT(change_log_writer.written_user_byte_count) << "\n"             //
  << BATT_INSPECT(change_log_writer.written_block_byte_count) << "\n"            //
  << BATT_INSPECT(change_log_writer.sleep_count) << "\n"                         //
  << BATT_INSPECT(change_log_writer.write_count) << "\n"                         //
  << BATT_INSPECT(change_log_writer.block_alloc_count) << "\n"                   //
  << BATT_INSPECT(change_log_writer.block_utilization_rate()) << "\n"            //
  << "\n"                                                                        //
  << BATT_INSPECT(cache_slot_pool.construct_count) << "\n"                       //
  << BATT_INSPECT(cache_slot_pool.free_queue_insert_count) << "\n"               //
  << BATT_INSPECT(cache_slot_pool.free_queue_remove_count) << "\n"               //
  << BATT_INSPECT(cache_slot_pool.evict_count) << "\n"                           //
  << BATT_INSPECT(cache_slot_pool.evict_prior_generation_count) << "\n"          //
  << "\n"                                                                        //
  << BATT_INSPECT_RANGE_PRETTY(page_cache.page_read_latency)                     //
  << "\n"                                                                        //
  << print_page_alloc_info                                                       //
  << "\n"                                                                        //
  << BATT_INSPECT(kv_store.mem_table_alloc) << "\n"                              //
  << BATT_INSPECT(kv_store.mem_table_free) << "\n"                               //
  << BATT_INSPECT(kv_store.mem_table_count_stats) << "\n"                        //
  << "\n"                                                                        //
  << "\n"                                                                        //
  << BATT_INSPECT(on_disk_footprint) << "\n"                                     //
  << BATT_INSPECT(space_amp) << "\n"                                             //
      ;
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> KVStore::debug_info() const noexcept
{
  return [this](std::ostream& out) {
    this->collect_stats([&out](std::string_view name, double value) {
      out << " " << name << " == " << value << "\n";
    });
  };
}

}  // namespace turtle_kv
