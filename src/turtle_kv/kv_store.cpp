//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/kv_store.hpp>
//

#include <turtle_kv/config.hpp>
#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/checkpoint_log.hpp>
#include <turtle_kv/file_utils.hpp>
#include <turtle_kv/kv_store_scanner.hpp>
#include <turtle_kv/page_file.hpp>

#include <turtle_kv/change_log/change_log_reader.hpp>

#include <turtle_kv/core/packed_key_value_slot.hpp>

#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/sharded_level_scanner.hpp>
#include <turtle_kv/tree/storage_config.hpp>

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

  // Recover the checkpoint
  //
  BATT_ASSIGN_OK_RESULT(Checkpoint latest_checkpoint,
                        KVStore::recover_latest_checkpoint(*checkpoint_log_volume));

  // TODO: [Gabe Bornstein 4/8/26] I don't like that we have to mess with checkpoint_upper_bound
  // in two places (KVStore::KVStore() is the other place). Consider how to refactor.
  // 1. Could move everything into KVStore::KVStore. Would requiring passing `dir_path` in.
  // 2. Could pass `checkpoint_upper_bound` as a parameter to KVStore::KVStore.
  // 3. Could update the value of the lastest_checkpoint.edit_offset_upper_bound before passing it
  // to KVStore::KVStore.
  // 4. Leave it as is.
  //
  // TODO [tastolfi 2026-04-08] I think that the checkpoint upper bound can be retrieved inside
  // KVStore::run_recovery, from the initial State; what I am more worried about is that we are
  // opening the ChangeLogWriter *before* doing recovery; to me this could spell trouble.  It makes
  // sense that run_recovery takes the path to the change log file; I think a good side-effect of
  // successful `run_recovery` could be to figure out what the correct values for the append and
  // trim points are, and create the ChangeLogWriter after we know that information.
  //
  Optional<turtle_kv::EditOffset> checkpoint_upper_bound =
      latest_checkpoint.edit_offset_upper_bound();

  std::unique_ptr<KVStore> kv_store{new KVStore{
      task_scheduler,
      worker_pool,
      std::move(scoped_io_ring),
      storage_context.shared_from_this(),
      tree_options,
      *runtime_options,
      std::move(change_log_writer),
      std::move(checkpoint_log_volume),
      std::move(latest_checkpoint),
  }};

  BATT_REQUIRE_OK(kv_store->run_recovery(dir_path / change_log_file_name(),
                                         checkpoint_upper_bound.value_or(EditOffset{0})));

  return {std::move(kv_store)};
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
                              std::unique_ptr<llfs::Volume>&& checkpoint_log,
                              Checkpoint&& latest_recovered_checkpoint) noexcept
    : metrics_{}
    , task_scheduler_{task_scheduler}
    , worker_pool_{worker_pool}
    , scoped_io_ring_{std::move(scoped_io_ring)}
    , storage_context_{std::move(storage_context)}
    , page_cache_{*BATT_OK_RESULT_OR_PANIC(this->storage_context_->get_page_cache())}
    , tree_options_{tree_options}
    , runtime_options_{runtime_options}
    , mem_table_allocation_tracker_{this->page_cache(), this->metrics_.overcommit}
    , change_log_writer_{std::move(change_log_writer)}
    , checkpoint_distance_{this->runtime_options_.initial_checkpoint_distance}
    , checkpoint_log_{std::move(checkpoint_log)}
    , filter_page_write_state_{FilterPageWriteState::make_new()}
    , per_thread_{}
    , state_{}
    , deltas_size_{0}
    , checkpoint_token_pool_{std::make_shared<batt::Grant::Issuer>(
          /*max_concurrent_checkpoint_jobs=*/1)}
    , halt_{false}
    , info_task_{}
    , next_mem_table_edit_offset_{0}
    , finalized_mem_table_channel_{}
    , checkpoint_update_channel_{}
    , checkpoint_generator_{}
    , checkpoint_batch_count_{0}
    , checkpoint_flush_channel_{}
    , mem_table_batch_scanner_thread_{}
    , checkpoint_update_thread_{}
    , checkpoint_flush_thread_{}
{
  this->initialize_state(std::move(latest_recovered_checkpoint));

  this->checkpoint_generator_.emplace(
      this->worker_pool_,
      this->tree_options_,
      this->page_cache(),
      batt::make_copy(this->filter_page_write_state_),
      batt::Toggle<State>::Reader{this->state_}->base_checkpoint_->clone(),
      *this->checkpoint_log_);

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

  this->info_task_.emplace(
      this->task_scheduler_.schedule_task(),
      [this] {
        this->info_task_main();
      },
      "KVStore::info_task");

  if (this->runtime_options_.use_threaded_checkpoint_pipeline) {
    this->mem_table_batch_scanner_thread_.emplace([this] {
      this->mem_table_batch_scanner_thread_main();
    });
    this->checkpoint_update_thread_.emplace([this] {
      this->checkpoint_update_thread_main();
    });
    this->checkpoint_flush_thread_.emplace([this] {
      this->checkpoint_flush_thread_main();
    });
  }
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
    auto& art = ARTBase::default_metrics();
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

  {
    auto& mem_table = this->metrics_.mem_table;
    LOG(INFO) << BATT_INSPECT(mem_table.alloc_count) << BATT_INSPECT(mem_table.free_count);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::initialize_state(Checkpoint&& latest_recovered_checkpoint)
{
  batt::Toggle<State>::Writer writer{this->state_};

  State& init_state = writer.new_value();

  init_state.base_checkpoint_.emplace(std::move(latest_recovered_checkpoint));

  init_state.mem_table_ = this->create_mem_table(
      init_state.base_checkpoint_->edit_offset_upper_bound().value_or(EditOffset{0}));

  this->next_mem_table_edit_offset_.set_value(
      init_state.mem_table_->edit_offset_lower_bound().value());

  this->deltas_size_->set_value(init_state.deltas_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::halt()
{
  // Let it be known: WE ARE HALTING!
  //
  this->halt_.set_value(true);

  // Stop the ChangeLogWriter.
  //
  this->change_log_writer_->halt();

  // Close all Watches.
  //
  this->deltas_size_->close();
  this->next_mem_table_edit_offset_.close();

  // Close all Channels.
  //
  this->finalized_mem_table_channel_.close();
  this->checkpoint_update_channel_.close();
  this->checkpoint_flush_channel_.close();

  // Prevent new asynchronous filter page writes from being initiated.
  //
  this->filter_page_write_state_->halt();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::join()
{
  this->filter_page_write_state_->join();
  this->change_log_writer_->join();
  if (this->mem_table_batch_scanner_thread_) {
    this->mem_table_batch_scanner_thread_->join();
    this->mem_table_batch_scanner_thread_ = None;
  }
  if (this->checkpoint_update_thread_) {
    this->checkpoint_update_thread_->join();
    this->checkpoint_update_thread_ = None;
  }
  if (this->checkpoint_flush_thread_) {
    this->checkpoint_flush_thread_->join();
    this->checkpoint_flush_thread_ = None;
  }
  if (this->info_task_) {
    this->info_task_->join();
    this->info_task_ = None;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::intrusive_ptr<MemTable> KVStore::create_mem_table(EditOffset edit_offset_lower_bound)
{
  const usize max_batches_per_mem_table = this->get_checkpoint_distance();

  boost::intrusive_ptr<MemTable> mem_table{new MemTable{
      this->mem_table_allocation_tracker_,
      *this->change_log_writer_,
      this->metrics_.mem_table,
      edit_offset_lower_bound,
      /*max_bytes_per_batch=*/this->tree_options_.flush_size(),
      max_batches_per_mem_table,
  }};

  BATT_CHECK_GT(mem_table->max_byte_size(), this->tree_options_.flush_size() / 2);

  return mem_table;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::put(const KeyView& key, const ValueView& value) noexcept /*override*/
{
  for (;;) {
#if TURTLE_KV_PROFILE_UPDATES
    this->metrics_.put_count.add(1);
    LatencyTimer put_timer{Every2ToTheConst<8>{}, this->metrics_.put_latency};
#endif

    // If the MemTable fills up, we will collect it here and try to finalize it after the State
    // Reader block exits.
    //
    boost::intrusive_ptr<MemTable> full_mem_table;

    {
      // Pin the current state so we can access the active MemTable.
      //
      batt::Toggle<State>::Reader state_reader{this->state_};

      MemTable* const observed_mem_table = state_reader->mem_table_.get();
      ThreadContext& thread_context = this->per_thread_.get(this);

#if TURTLE_KV_PROFILE_UPDATES
      LatencyTimer put_mem_table_timer{Every2ToTheConst<8>{}, this->metrics_.put_memtable_latency};
#endif

      // Insert the key/value pair into the active MemTable; this will also append a change log
      // buffer.
      //
      Status status =
          observed_mem_table->put(thread_context.change_log_writer_context_, key, value);

#if TURTLE_KV_PROFILE_UPDATES
      put_mem_table_timer.stop();
#endif

      // On success and unrecoverable errors, just return immediately.
      //
      if (status != batt::StatusCode::kResourceExhausted) {
        return status;
      }

      // Grab a (owning) reference to the MemTable.
      //
      full_mem_table = state_reader->mem_table_;
    }
    // ~Reader ends State read critical section

    // If the MemTable is too full to accept this update, then finalize the current MemTable and try
    // again.
    //
#if TURTLE_KV_PROFILE_UPDATES
    this->metrics_.put_memtable_full_count.add(1);
#endif

    BATT_REQUIRE_OK(this->finalize_mem_table(std::move(full_mem_table)));

#if TURTLE_KV_PROFILE_UPDATES
    LatencyTimer put_wait_trim_timer{this->metrics_.put_wait_trim_latency};
#endif

    // Limit the number of deltas that can build up.
    //
    const usize max_deltas_size = 2;

    BATT_REQUIRE_OK(this->deltas_size_->await_true([this, max_deltas_size](usize n) {
      return n <= max_deltas_size;
    }));

#if TURTLE_KV_PROFILE_UPDATES
    put_wait_trim_timer.stop();
    this->metrics_.put_retry_count.add(1);
#endif

    // Now that we have a new MemTable with plenty of space, try again...
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::recover_put(FirstVisitToBlock first_visit,
                            ChangeLogBlock* block,
                            EditOffset edit_offset,
                            ConstBuffer payload)
{
  for (;;) {
    boost::intrusive_ptr<MemTable> full_mem_table;

    {
      batt::Toggle<State>::Reader state_reader{this->state_};

      StatusOr<std::pair<KeyView, ValueView>> unpacked_slot = unpack_key_value_slot(payload);
      BATT_REQUIRE_OK(unpacked_slot);

      Status status = state_reader->mem_table_->put_recovered_slot(first_visit,
                                                                   block,
                                                                   edit_offset,
                                                                   unpacked_slot->first,
                                                                   unpacked_slot->second);

      if (status.ok() || status != batt::StatusCode::kResourceExhausted) {
        return status;
      }

      full_mem_table = state_reader->mem_table_;
    }

    // Handle case where MemTable has filled up and we need to:
    // 1. Finalize the old MemTable.
    // 2. Create a new MemTable.
    // 3. Retry the put.
    //
    BATT_CHECK_NOT_NULLPTR(full_mem_table);
    BATT_REQUIRE_OK(this->finalize_mem_table(std::move(full_mem_table)));

    // TODO [tastolfi 2026-04-07] maybe also the back-pressure stuff (i.e., waiting for the deltas
    // to shrink?)
  }
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

  boost::intrusive_ptr<MemTable> old_mem_table = [this]() -> boost::intrusive_ptr<MemTable> {
    batt::Toggle<State>::Reader state_reader{this->state_};
    if (state_reader->mem_table_->empty()) {
      return nullptr;
    }
    return state_reader->mem_table_;
  }();

  if (old_mem_table) {
    BATT_REQUIRE_OK(this->finalize_mem_table(std::move(old_mem_table)));
    BATT_REQUIRE_OK(this->deltas_size_->await_true([this](usize n) {
      return n < 2;
    }));
  }

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
  batt::Toggle<State>::Reader state_reader{this->state_};

  // First check the current active MemTable.
  //
  Optional<ValueView> value = TURTLE_KV_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<14>{},
                                                               this->metrics_.mem_table_get_latency,
                                                               state_reader->mem_table_->get(key));

  const auto return_mem_table_value =
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
      return return_mem_table_value(value, this->metrics_.mem_table_get_count);
    }
  }

  // Second, check recently finalized MemTables (the deltas_ stack).
  //
  const usize observed_deltas_size = state_reader->deltas_.size();
  for (usize i = observed_deltas_size; i != 0;) {
    --i;
    const boost::intrusive_ptr<MemTable>& delta = state_reader->deltas_[i];

    Optional<ValueView> delta_value =
        TURTLE_KV_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<18>{},
                                         this->metrics_.delta_get_latency,
                                         delta->finalized_get(key));

    if (delta_value) {
      if (value) {
        *value = combine(*value, *delta_value);
        if (!value->needs_combine()) {
          return return_mem_table_value(
              value,
              this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)]);
        }
      } else {
        if (!delta_value->needs_combine()) {
          return return_mem_table_value(
              delta_value,
              this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)]);
        }
        value = delta_value;
      }
    }
  }

  // If we haven't resolved the query by this point, we must search the current checkpoint tree.
  //
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
                                       state_reader->base_checkpoint_->find_key(query));

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
Status KVStore::finalize_mem_table(boost::intrusive_ptr<MemTable>&& old_mem_table)
{
  const bool newly_finalized = old_mem_table->finalize();
  const EditOffset current_edit_offset = old_mem_table->edit_offset_upper_bound();

  if (newly_finalized) {
    // If this thread wins the race to finalize the MemTable, then create a new one and install it
    // as the active MemTable.
    //
    BATT_REQUIRE_OK(this->reset_active_mem_table(current_edit_offset));

    // Lastly, we must send the old MemTable off to the checkpoint update pipeline.
    //
    BATT_REQUIRE_OK(this->hand_off_finalized_mem_table(std::move(old_mem_table)));

  } else {
    // A different thread won the race to finalize the MemTable; wait for the next MemTable to be
    // installed in `State` by that thread.
    //
    this->wait_for_new_mem_table(/*target_edit_offset=*/current_edit_offset);
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::reset_active_mem_table(EditOffset current_edit_offset)
{
#if TURTLE_KV_PROFILE_UPDATES
  LatencyTimer mem_table_create_timer{this->metrics_.put_memtable_create_latency};
#endif

  batt::Toggle<State>::Writer state_writer{this->state_};

  State& new_state = state_writer.new_value();
  const State& old_state = state_writer.old_value();

  new_state.mem_table_ = this->create_mem_table(current_edit_offset);
  new_state.base_checkpoint_ = old_state.base_checkpoint_;
  new_state.deltas_ = old_state.deltas_;
  new_state.deltas_.emplace_back(old_state.mem_table_);

  BATT_CHECK_EQ(new_state.mem_table_->edit_offset_lower_bound(), current_edit_offset);

  this->deltas_size_->fetch_add(1);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::hand_off_finalized_mem_table(boost::intrusive_ptr<MemTable>&& old_mem_table)
{
  if (this->runtime_options_.use_threaded_checkpoint_pipeline) {
    BATT_REQUIRE_OK(this->push_mem_table_to_channel(std::move(old_mem_table)));

  } else {
    BATT_REQUIRE_OK(this->scan_mem_table_to_build_batches(  //
        std::move(old_mem_table),
        [this](std::unique_ptr<DeltaBatch> delta_batch) -> Status {
          BATT_ASSIGN_OK_RESULT(std::unique_ptr<CheckpointJob> checkpoint_job,
                                this->apply_batch_to_checkpoint(std::move(delta_batch)));

          if (checkpoint_job) {
            BATT_REQUIRE_OK(this->commit_checkpoint(std::move(checkpoint_job)));
          }

          return OkStatus();
        }));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::wait_for_new_mem_table(EditOffset target_edit_offset)
{
  // Spin until we see a MemTable with a sufficiently advanced starting edit offset.
  //
  for (;;) {
    u64 observed;
    {
      batt::Toggle<State>::Reader state_reader{this->state_};
      if (state_reader->mem_table_->edit_offset_lower_bound() >= target_edit_offset) {
        break;
      }
      observed = state_reader.seq();
    }
    this->state_.wait(observed);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::push_mem_table_to_channel(boost::intrusive_ptr<MemTable>&& mem_table)
{
  BATT_CHECK(mem_table->is_finalized());

#if TURTLE_KV_PROFILE_UPDATES
  LatencyTimer queue_push_timer{this->metrics_.put_memtable_queue_push_latency};
#endif

  const EditOffset this_edit_offset = mem_table->edit_offset_lower_bound();
  const EditOffset next_edit_offset = mem_table->edit_offset_upper_bound();

  BATT_REQUIRE_OK(this->next_mem_table_edit_offset_.await_true([this_edit_offset](i64 observed) {
    return EditOffset{observed} == this_edit_offset;
  }));

  BATT_REQUIRE_OK(this->finalized_mem_table_channel_.write(std::move(mem_table)));

  const i64 replaced_value = this->next_mem_table_edit_offset_.set_value(next_edit_offset.value());
  BATT_CHECK_EQ(replaced_value, this_edit_offset.value());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status KVStore::run_recovery(const std::filesystem::path& path,
                                   EditOffset checkpoint_upper_bound)
{
  // Reover MemTable's from the ChangeLog
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogReader> log, ChangeLogReader::open(path));

  batt::Status status =
      log->visit_slots([this, checkpoint_upper_bound](FirstVisitToBlock first_visit,
                                                      ChangeLogBlock* block,
                                                      EditOffset edit_offset,
                                                      ConstBuffer payload) -> batt::Status {
        // Skip slots already recovered by checkpoint
        //
        if (edit_offset < checkpoint_upper_bound) {
          return batt::OkStatus();
        }

        batt::Status recovered_slot_status =
            this->recover_put(first_visit, block, edit_offset, payload);
        BATT_REQUIRE_OK(recovered_slot_status);

        return batt::OkStatus();
      });

  BATT_REQUIRE_OK(status);
  return batt::OkStatus();
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
  prev_checkpoint.second.edit_offset_upper_bound = 0;

  for (;;) {
    llfs::StatusOr<usize> n_slots_visited = reader->visit_typed_next(
        batt::WaitForResource::kFalse,
        [&prev_checkpoint](const llfs::SlotParse& slot,
                           const turtle_kv::PackedCheckpoint& packed_checkpoint) {
          // Validate that the checkpoints are in ascending order based on batch_upper_bound
          //
          BATT_CHECK_GE(EditOffset{packed_checkpoint.edit_offset_upper_bound},
                        EditOffset{prev_checkpoint.second.edit_offset_upper_bound});
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
  if (prev_checkpoint.second.edit_offset_upper_bound == 0) {
    return Checkpoint::make_empty();
  }

  return turtle_kv::Checkpoint::recover(checkpoint_log_volume,
                                        prev_checkpoint.first,
                                        prev_checkpoint.second);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// TODO: [Gabe Bornstein 3/25/26] Add a KVStore::recover function that recovers checkpoint,
// mem_table, and updates KVStore.state_ to reflect the newly recovered checkpoint and mem_table.
//
batt::StatusOr<boost::intrusive_ptr<turtle_kv::MemTable>> KVStore::recover_latest_mem_table(
    const std::filesystem::path& path)
{
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogReader> log, ChangeLogReader::open(path));

  boost::intrusive_ptr<MemTable> mem_table = nullptr;

  bool first_slot = true;

  batt::Status status =
      log->visit_slots([this, &mem_table, &first_slot](FirstVisitToBlock first_visit,
                                                       ChangeLogBlock* block,
                                                       EditOffset edit_offset,
                                                       ConstBuffer payload) -> batt::Status {
        if (first_slot) {
          mem_table = this->create_mem_table(edit_offset);
          first_slot = false;
        }

        StatusOr<std::pair<KeyView, ValueView>> unpacked_slot = unpack_key_value_slot(payload);
        BATT_REQUIRE_OK(unpacked_slot);

        batt::Status recovered_slot_status = mem_table->put_recovered_slot(first_visit,
                                                                           block,
                                                                           edit_offset,
                                                                           unpacked_slot->first,
                                                                           unpacked_slot->second);
        BATT_REQUIRE_OK(recovered_slot_status);

        // TODO: [Gabe Bornstein 3/31/26] Need to start reading from where the last checkpoint was
        // taken. Skip all slots that are already captured by the most recent checkpoint.
        //

        // TODO: [Gabe Bornstein 3/31/26]  Check if recovered_slot_status is ResourceExhausted,
        // stash the current mem_table, create a new mem_table, keep going.
        //

        return batt::OkStatus();
      });

  BATT_REQUIRE_OK(status);
  BATT_CHECK_NE(mem_table, nullptr);
  return mem_table;
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
void KVStore::mem_table_batch_scanner_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      StatusOr<boost::intrusive_ptr<MemTable>> mem_table =
          this->finalized_mem_table_channel_.read();

      BATT_REQUIRE_OK(mem_table);
      BATT_CHECK_NOT_NULLPTR(*mem_table);

      BATT_REQUIRE_OK(this->scan_mem_table_to_build_batches(
          std::move(*mem_table),
          [this](std::unique_ptr<DeltaBatch> delta_batch) -> Status {
            BATT_REQUIRE_OK(this->checkpoint_update_channel_.write(std::move(delta_batch)));

            return OkStatus();
          }));
    }
  }();

  LOG(INFO) << "mem_table_compact_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Fn>
  requires std::invocable<Fn, std::unique_ptr<DeltaBatch>>
Status KVStore::scan_mem_table_to_build_batches(boost::intrusive_ptr<MemTable>&& mem_table,
                                                Fn&& consume_fn)
{
  MemTable::BatchCompactor batch_compactor{*mem_table,
                                           /*byte_size_limit=*/this->tree_options_.flush_size()};

  EditOffset edit_offset_upper_bound = mem_table->edit_offset_upper_bound();
  usize batch_index = 0;
  bool has_next = batch_compactor.has_next();

  const auto make_batch_id = [&] {
    return DeltaBatchId{
        edit_offset_upper_bound,
        IndexInGroup{batch_index},
        IsLastInGroup{!has_next},
    };
  };

  // Handle empty MemTable case specially.
  //
  if (!has_next) {
    auto empty_batch = std::make_unique<DeltaBatch>(make_batch_id(),
                                                    batt::make_copy(mem_table),
                                                    DeltaBatch::ResultSet{});

    return consume_fn(std::move(empty_batch));
  }

  while (has_next) {
    MergeCompactor::ResultSet</*decay_to_items=*/false> next_result_set =
        batch_compactor.consume_next();

    has_next = batch_compactor.has_next();

    auto delta_batch =
        TURTLE_KV_COLLECT_LATENCY(this->metrics_.compact_batch_latency,
                                  std::make_unique<DeltaBatch>(make_batch_id(),
                                                               batt::make_copy(mem_table),
                                                               std::move(next_result_set)));
    ++batch_index;

    this->metrics_.batch_count.add(1);
    this->metrics_.batch_edits_count.add(delta_batch->result_set_size());

    BATT_REQUIRE_OK(consume_fn(std::move(delta_batch)));
  }

  return OkStatus();
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
  const auto batch_id = delta_batch                                            //
                            ? Optional<DeltaBatchId>{delta_batch->batch_id()}  //
                            : Optional<DeltaBatchId>{None};

  const BoolStatus checkpoint_after =
      delta_batch ? batt::bool_status_from(delta_batch->batch_id().is_last_in_group())
                  : BoolStatus::kUnknown;

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
        this->checkpoint_generator_->apply_batch(std::move(delta_batch), overcommit));

    BATT_REQUIRE_OK(push_status);
    BATT_CHECK_EQ(*push_status, 1);

    this->checkpoint_batch_count_ += 1;
  }

  // If the batch count is below the checkpoint distance, we are done.
  //
  if (!overcommit.is_triggered() &&
      ((this->checkpoint_batch_count_ < this->checkpoint_distance_.load() &&
        checkpoint_after == BoolStatus::kUnknown) ||
       (checkpoint_after == BoolStatus::kFalse))) {
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
      this->checkpoint_generator_->page_cache_job().pinned_page_count());

  // Allocate a token for the checkpoint job.
  //
  BATT_ASSIGN_OK_RESULT(batt::Grant checkpoint_token,
                        this->checkpoint_token_pool_->issue_grant(1, batt::WaitForResource::kTrue));

  // Serialize all pages and create the job.
  //
  StatusOr<std::unique_ptr<CheckpointJob>> checkpoint_job =
      TURTLE_KV_COLLECT_LATENCY(this->metrics_.finalize_checkpoint_latency,
                                this->checkpoint_generator_->finalize_checkpoint(
                                    std::move(checkpoint_token),
                                    batt::make_copy(this->checkpoint_token_pool_),
                                    overcommit));

  BATT_REQUIRE_OK(checkpoint_job);

  BATT_CHECK_NOT_NULLPTR(*checkpoint_job);
  BATT_CHECK((*checkpoint_job)->append_job_grant);
  BATT_CHECK((*checkpoint_job)->appendable_job);
  BATT_CHECK((*checkpoint_job)->prepare_slot_sequencer);

  // Number of batches in the current checkpoint resets to zero.
  //
  this->checkpoint_batch_count_ = 0;

  return checkpoint_job;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::checkpoint_flush_thread_main()
{
  auto on_scope_exit = batt::finally([&] {
    this->deltas_size_->close();
  });

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
  BATT_REQUIRE_OK(this->checkpoint_log_->sync(llfs::LogReadMode::kDurable,
                                              llfs::SlotUpperBoundAt{
                                                  .offset = checkpoint_slot_range->upper_bound,
                                              }));

  // Update the base checkpoint and clear deltas.
  //
  Optional<llfs::slot_offset_type> prev_checkpoint_slot;
  {
    batt::Toggle<State>::Writer state_writer{this->state_};

    State& new_state = state_writer.new_value();
    const State& old_state = state_writer.old_value();

    new_state.base_checkpoint_ = std::move(*checkpoint_job->checkpoint);
    new_state.base_checkpoint_->notify_durable(std::move(*slot_read_lock));
    new_state.base_checkpoint_->tree()->lock();

    BATT_CHECK(new_state.base_checkpoint_->tree()->is_serialized());
    BATT_CHECK(old_state.base_checkpoint_->tree()->is_serialized());

    // Save the old checkpoint's slot lower bound so we can trim to it later.
    //
    Optional<llfs::SlotRange> prev_slot_range = old_state.base_checkpoint_->slot_range();
    if (prev_slot_range) {
      prev_checkpoint_slot = prev_slot_range->upper_bound;
    }

    // Transfer the MemTable and deltas (finalized MemTables) from the old state.
    //
    new_state.mem_table_ = old_state.mem_table_;

    // Find the first delta MemTable on the stack that is *not* covered by the checkpoint.
    //  (deltas_ is in oldest-to-newest order)
    //
    auto iter = std::lower_bound(old_state.deltas_.begin(),
                                 old_state.deltas_.end(),
                                 checkpoint_job,
                                 OrderByEditOffsetUpperBound{});

    // Copy over references to delta MemTables _newer_ than the checkpoint.
    //
    new_state.deltas_.assign(iter, old_state.deltas_.end());

    BATT_CHECK_LE(new_state.deltas_.size(), old_state.deltas_.size())
        << "Deltas should never grow as a result of committing a checkpoint!";

    this->deltas_size_->set_value(new_state.deltas_.size());

  }  // ~Writer() swaps the new state into the active status.

  // Trim the checkpoint volume to free old pages.
  //
  if (prev_checkpoint_slot) {
    BATT_REQUIRE_OK(this->checkpoint_log_->trim(*prev_checkpoint_slot));
  }

  return OkStatus();
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
  auto& change_log_file = this->change_log_writer_->change_log_file();
  auto& change_log_writer = this->change_log_writer_->metrics();

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
    auto& checkpoint = this->checkpoint_generator_->metrics();

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
