#include <turtle_kv/kv_store.hpp>
//

#include <turtle_kv/checkpoint_log.hpp>
#include <turtle_kv/file_utils.hpp>
#include <turtle_kv/page_file.hpp>

#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/tree_scanner.hpp>

#include <turtle_kv/util/memory_stats.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/env.hpp>

#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/page_device_metrics.hpp>

#include <gperftools/malloc_extension.h>

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
/*static*/ KVStore::Config KVStore::Config::with_default_values() noexcept
{
  return Config{
      .tree_options = TreeOptions::with_default_values(),
      .initial_capacity_bytes = 512 * kMiB,
      .change_log_size_bytes = 32 * kMiB,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ KVStore::RuntimeOptions KVStore::RuntimeOptions::with_default_values() noexcept
{
  return RuntimeOptions{
      .initial_checkpoint_distance = 1,
      .use_threaded_checkpoint_pipeline = true,
      .cache_size_bytes = 4 * kGiB,
      .memtable_compact_threads = 4,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status KVStore::configure_storage_context(llfs::StorageContext& storage_context,
                                                     const TreeOptions& tree_options,
                                                     const RuntimeOptions& runtime_options) noexcept
{
  auto page_cache_options = llfs::PageCacheOptions::with_default_values();

  std::set<llfs::PageSize> sharded_leaf_views;

  sharded_leaf_views.insert(llfs::PageSize{4 * kKiB});
  sharded_leaf_views.insert(tree_options.trie_index_sharded_view_size());

  for (llfs::PageSize view_size : sharded_leaf_views) {
    if (tree_options.leaf_size() != view_size) {
      page_cache_options.add_sharded_view(tree_options.leaf_size(), view_size);
    }
  }

  page_cache_options.set_byte_size(runtime_options.cache_size_bytes,
                                   /*default_page_size=*/llfs::PageSize{4 * kKiB});

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

    const llfs::PageCount node_count{leaf_count / 2};

    const llfs::PageCount filter_count{leaf_count};

    BATT_REQUIRE_OK(create_page_file(storage_context,                   //
                                     dir_path / leaf_page_file_name(),  //
                                     leaf_count,                        //
                                     tree_options.leaf_size(),          //
                                     RemoveExisting{false},             //
                                     /*device_id=*/0));

    BATT_REQUIRE_OK(create_page_file(storage_context,                   //
                                     dir_path / node_page_file_name(),  //
                                     node_count,                        //
                                     tree_options.node_size(),          //
                                     RemoveExisting{false},             //
                                     /*device_id=*/1));

    BATT_REQUIRE_OK(create_page_file(storage_context,                     //
                                     dir_path / filter_page_file_name(),  //
                                     filter_count,                        //
                                     tree_options.filter_page_size(),     //
                                     RemoveExisting{false},               //
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

  return OkStatus();
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
    : task_scheduler_{task_scheduler}
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
          /*max_byte_size=*/this->tree_options_.leaf_data_size(),
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

    Status status = this->page_cache().assign_filter_device(this->tree_options_.leaf_size(),
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
  const State* const observed_state = this->state_.load();
  BATT_CHECK_GT(observed_state->use_count(), 0);

  boost::intrusive_ptr<const State> pinned_state{observed_state};
  BATT_CHECK_GT(observed_state->use_count(), 1);

  MemTable* const observed_mem_table = observed_state->mem_table_.get();
  const u64 observed_mem_table_id = observed_mem_table->id();

  ChangeLogWriter::Context& log_writer_context =
      this->per_thread_.get(this).log_writer_context(observed_mem_table_id);

  Status status = observed_mem_table->put(log_writer_context, key, value);

  if (status == batt::StatusCode::kResourceExhausted) {
    BATT_REQUIRE_OK(this->update_checkpoint(observed_state));

    // Limit the number of deltas that can build up.
    //
    BATT_REQUIRE_OK(this->deltas_size_->await_true([this](usize n) {
      return n <= this->checkpoint_distance_.load() * 2;
    }));

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
  Optional<ValueView> value = BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<14>{},
                                                          this->metrics_.mem_table_get_latency,
                                                          observed_state->mem_table_->get(key));

  if (value) {
    if (!value->needs_combine()) {
      this->metrics_.mem_table_get_count.add(1);
      return *value;
    }
  }

  // Second, check recently finalized MemTables (the deltas_ stack).
  //
  const usize observed_deltas_size = observed_state->deltas_.size();
  for (usize i = observed_deltas_size; i != 0;) {
    --i;
    const boost::intrusive_ptr<MemTable>& delta = observed_state->deltas_[i];

    Optional<ValueView> delta_value = BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<18>{},
                                                                  this->metrics_.delta_get_latency,
                                                                  delta->finalized_get(key));

    if (delta_value) {
      if (value) {
        *value = combine(*value, *delta_value);
        if (!value->needs_combine()) {
          this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)].add(1);
          return *value;
        }
      } else {
        if (!delta_value->needs_combine()) {
          this->metrics_.delta_log2_get_count[batt::log2_ceil(observed_deltas_size - i)].add(1);
          return *delta_value;
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
      BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<12>{},
                                  this->metrics_.checkpoint_get_latency,
                                  pinned_state->base_checkpoint_.find_key(query));

  if (value_from_checkpoint.ok()) {
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
  const State* const observed_state = this->state_.load();
  boost::intrusive_ptr<const State> pinned_state{observed_state};
  ThreadContext& thread_context = this->per_thread_.get(this);

  const usize n_mem_tables = observed_state->deltas_.size() + 1;

  using DeltaScannerMemory =
      std::aligned_storage_t<sizeof(MemTable::Scanner), alignof(MemTable::Scanner)>;

  // Allocate memory for MemTable scanners.
  //
  std::array<DeltaScannerMemory, 32> local_delta_scanners;
  DeltaScannerMemory* delta_scanners_memory = local_delta_scanners.data();
  if (n_mem_tables > local_delta_scanners.size()) {
    delta_scanners_memory = new DeltaScannerMemory[n_mem_tables];
  }

  // Construct the delta scanners.
  //
  MemTable::Scanner* delta_scanners = reinterpret_cast<MemTable::Scanner*>(delta_scanners_memory);

  usize delta_scanners_size = 0;

  // Arrange for delta scanners to be deleted when done.
  //
  auto on_scope_exit = batt::finally([&] {
    for (usize i = 0; i < delta_scanners_size; ++i) {
      delta_scanners[i].~Scanner();
    }
    if (delta_scanners_memory != local_delta_scanners.data()) {
      delete[] delta_scanners_memory;
    }
  });

  for (; delta_scanners_size < n_mem_tables - 1; ++delta_scanners_size) {
    const usize i = delta_scanners_size;
    new (&delta_scanners_memory[i]) MemTable::Scanner{*observed_state->deltas_[i], min_key};
  }

  // Construct the scanner for the live MemTable.
  //
  new (&delta_scanners_memory[delta_scanners_size])
      MemTable::Scanner{*observed_state->mem_table_, min_key};

  ++delta_scanners_size;

  // Create the merged delta scanner.
  //
  DeltasScanner deltas_scanner{as_slice(delta_scanners, delta_scanners_size), min_key};

  // Create the tree scanner.
  //
  TreeScanner checkpoint_scanner{thread_context.get_page_loader(),
                                 batt::make_copy(observed_state->base_checkpoint_.tree())};

  BATT_CHECK(observed_state->base_checkpoint_.tree()->is_serialized());

  checkpoint_scanner.seek_to(min_key);
  checkpoint_scanner.prepare();

  // Merge items from deltas and checkpoint.
  //
  Optional<EditView> next_delta_edit = deltas_scanner.next();
  Optional<EditView> next_checkpoint_edit = checkpoint_scanner.next();

  usize n_read = 0;

  const auto consume_one = [&items_out, &n_read](Optional<EditView>& next_edit, auto& scanner) {
    items_out[n_read].first = next_edit->key;
    items_out[n_read].second = next_edit->value;
    next_edit = scanner.next();
  };

  const auto consume_rest = [&items_out, &n_read](Optional<EditView>& next_edit, auto& scanner) {
    while (next_edit) {
      items_out[n_read].first = next_edit->key;
      items_out[n_read].second = next_edit->value;
      ++n_read;
      if (n_read == items_out.size()) {
        break;
      }
      next_edit = scanner.next();
    }
  };

  while (n_read < items_out.size()) {
    if (!next_delta_edit) {
      consume_rest(next_checkpoint_edit, checkpoint_scanner);
      break;
    }
    if (!next_checkpoint_edit) {
      consume_rest(next_delta_edit, deltas_scanner);
      break;
    }

    switch (batt::compare(get_key(*next_delta_edit), get_key(*next_checkpoint_edit))) {
      case batt::Order::Less:
        consume_one(next_delta_edit, deltas_scanner);
        break;

      case batt::Order::Equal: {
        EditView edit = combine(*next_delta_edit, *next_checkpoint_edit);
        items_out[n_read].first = edit.key;
        items_out[n_read].second = edit.value;
        next_delta_edit = deltas_scanner.next();
        next_checkpoint_edit = checkpoint_scanner.next();
        break;
      }

      case batt::Order::Greater:
        consume_one(next_checkpoint_edit, checkpoint_scanner);
        break;

      default:
        BATT_PANIC() << "Bad comparison result!";
        BATT_UNREACHABLE();
    }

    ++n_read;
  }

  return n_read;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::remove(const KeyView& key) noexcept /*override*/
{
  (void)key;

  return batt::StatusCode::kUnimplemented;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::update_checkpoint(const State* observed_state)
{
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
      /*max_byte_size=*/this->tree_options_.flush_size(),
      next_batch_id.to_mem_table_id(),
  });

  for (;;) {
    if (observed_state->mem_table_ != old_mem_table) {
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

  BATT_COLLECT_LATENCY(this->metrics_.compact_batch_latency, delta_batch->merge_compact_edits());

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
  if (delta_batch) {
    // Apply the finalized MemTable to the current checkpoint (in-memory).
    //
    StatusOr<usize> push_status =
        BATT_COLLECT_LATENCY(this->metrics_.push_batch_latency,
                             this->checkpoint_generator_.push_batch(std::move(delta_batch)));

    BATT_REQUIRE_OK(push_status);
    BATT_CHECK_EQ(*push_status, 1);

    this->checkpoint_batch_count_ += 1;
  }

  // If the batch count is below the checkpoint distance, we are done.
  //
  if (this->checkpoint_batch_count_ < this->checkpoint_distance_.load()) {
    return nullptr;
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
  StatusOr<std::unique_ptr<CheckpointJob>> checkpoint_job =
      BATT_COLLECT_LATENCY(this->metrics_.finalize_checkpoint_latency,
                           this->checkpoint_generator_.finalize_checkpoint(
                               std::move(checkpoint_token),
                               batt::make_copy(this->checkpoint_token_pool_)));

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
      BATT_COLLECT_LATENCY(this->metrics_.append_job_latency,                     //
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
  const i64 expires_at_epoch = this->current_epoch_.load() + 3;

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
  constexpr i64 kMinEpochUsec = 12500;
  constexpr i64 kMaxEpochUsec = 15000;

  std::default_random_engine rng{std::random_device{}()};
  std::uniform_int_distribution<i64> pick_delay_usec{kMinEpochUsec, kMaxEpochUsec};

  while (!this->halt_.get_value()) {
    std::this_thread::sleep_for(std::chrono::microseconds{pick_delay_usec(rng)});
    this->current_epoch_.fetch_add(1);

    std::vector<boost::intrusive_ptr<const State>> to_delete;
    {
      absl::MutexLock lock{&this->obsolete_states_mutex_};

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
std::function<void(std::ostream&)> KVStore::debug_info() noexcept
{
  return [this](std::ostream& out) {
    auto& kv_store = this->metrics_;
    auto& checkpoint_log = *this->checkpoint_log_;
    auto& cache = checkpoint_log.cache();
    auto& change_log_file = this->log_writer_->change_log_file();
    auto& change_log_writer = this->log_writer_->metrics();

    auto& cache_slot_pool = llfs::PageCacheSlot::Pool::Metrics::instance();

    auto& page_cache = cache.metrics();

    auto& node = InMemoryNode::metrics();

    auto& query_page_loader = PinningPageLoader::metrics();

    std::array<double, 32> page_reads_per_get;
    page_reads_per_get.fill(0);

    double total_get_count = kv_store.total_get_count();

    for (usize i = 12; i < 28; ++i) {
      page_reads_per_get[i] =
          (double)llfs::PageDeviceMetrics::instance().read_count_per_page_size_log2[i] /
          total_get_count;
    }

    double page_reads_per_get_4k = page_reads_per_get[12];
    double page_reads_per_get_8k = page_reads_per_get[13];
    double page_reads_per_get_16k = page_reads_per_get[14];
    double page_reads_per_get_32k = page_reads_per_get[15];
    double page_reads_per_get_64k = page_reads_per_get[16];
    double page_reads_per_get_128k = page_reads_per_get[17];
    double page_reads_per_get_256k = page_reads_per_get[18];
    double page_reads_per_get_512k = page_reads_per_get[19];
    double page_reads_per_get_1m = page_reads_per_get[20];
    double page_reads_per_get_2m = page_reads_per_get[21];
    double page_reads_per_get_4m = page_reads_per_get[22];
    double page_reads_per_get_8m = page_reads_per_get[23];
    double page_reads_per_get_16m = page_reads_per_get[24];
    double page_reads_per_get_32m = page_reads_per_get[25];
    double page_reads_per_get_64m = page_reads_per_get[26];

    out << "\n"
        << BATT_INSPECT(kv_store.mem_table_get_count) << "\n"                          //
        << BATT_INSPECT(kv_store.mem_table_get_latency) << "\n"                        //
        << "\n"                                                                        //
        << BATT_INSPECT(kv_store.delta_log2_get_count[0]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[1]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[2]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[3]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[4]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[5]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[6]) << "\n"                      //
        << BATT_INSPECT(kv_store.delta_log2_get_count[7]) << "\n"                      //
        << "\n"                                                                        //
        << BATT_INSPECT(kv_store.delta_get_latency) << "\n"                            //
        << "\n"                                                                        //
        << BATT_INSPECT(kv_store.checkpoint_get_count) << "\n"                         //
        << BATT_INSPECT(kv_store.checkpoint_get_latency) << "\n"                       //
        << BATT_INSPECT(kv_store.checkpoint_pinned_pages_stats) << "\n"                //
        << "\n"                                                                        //
        << BATT_INSPECT(node.level_depth_stats) << "\n"                                //
        << "\n"                                                                        //
        << BATT_INSPECT(page_cache.get_count) << "\n"                                  //
        << BATT_INSPECT(page_cache.allocate_page_alloc_latency) << "\n"                //
        << BATT_INSPECT(page_cache.page_read_latency) << "\n"                          //
        << BATT_INSPECT(page_cache.total_bytes_read) << "\n"                           //
        << "\n"                                                                        //
        << BATT_INSPECT(query_page_loader.prefetch_hint_latency) << "\n"               //
        << BATT_INSPECT(query_page_loader.hash_map_lookup_latency) << "\n"             //
        << BATT_INSPECT(query_page_loader.get_page_from_cache_latency) << "\n"         //
        << BATT_INSPECT(query_page_loader.get_page_count) << "\n"                      //
        << BATT_INSPECT(query_page_loader.hash_map_miss_count) << "\n"                 //
        << "\n"                                                                        //
        << BATT_INSPECT(kv_store.checkpoint_count) << "\n"                             //
        << BATT_INSPECT(kv_store.batch_edits_count) << "\n"                            //
        << BATT_INSPECT(kv_store.avg_edits_per_batch()) << "\n"                        //
        << BATT_INSPECT(kv_store.compact_batch_latency) << "\n"                        //
        << BATT_INSPECT(kv_store.push_batch_latency) << "\n"                           //
        << BATT_INSPECT(kv_store.finalize_checkpoint_latency) << "\n"                  //
        << BATT_INSPECT(kv_store.append_job_latency) << "\n"                           //
        << "\n"                                                                        //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_success_count) << "\n"      //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_failure_count) << "\n"      //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_latency) << "\n"            //
        << "\n"                                                                        //
        << BATT_INSPECT(BloomFilterMetrics::instance().word_count_stats) << "\n"       //
        << BATT_INSPECT(BloomFilterMetrics::instance().byte_size_stats) << "\n"        //
        << BATT_INSPECT(BloomFilterMetrics::instance().bit_size_stats) << "\n"         //
        << BATT_INSPECT(BloomFilterMetrics::instance().bit_count_stats) << "\n"        //
        << BATT_INSPECT(BloomFilterMetrics::instance().item_count_stats) << "\n"       //
        << "\n"                                                                        //
        << BATT_INSPECT(QuotientFilterMetrics::instance().byte_size_stats) << "\n"     //
        << BATT_INSPECT(QuotientFilterMetrics::instance().bit_size_stats) << "\n"      //
        << BATT_INSPECT(QuotientFilterMetrics::instance().item_count_stats) << "\n"    //
        << BATT_INSPECT(QuotientFilterMetrics::instance().bits_per_key_stats) << "\n"  //
        << "\n"                                                                        //
        << BATT_INSPECT(KeyQuery::metrics().reject_page_latency) << "\n"               //
        << BATT_INSPECT(KeyQuery::metrics().filter_lookup_latency) << "\n"             //
        << BATT_INSPECT(KeyQuery::metrics().total_filter_query_count) << "\n"          //
        << BATT_INSPECT(KeyQuery::metrics().no_filter_page_count) << "\n"              //
        << BATT_INSPECT(KeyQuery::metrics().filter_page_load_failed_count) << "\n"     //
        << BATT_INSPECT(KeyQuery::metrics().page_id_mismatch_count) << "\n"            //
        << BATT_INSPECT(KeyQuery::metrics().filter_reject_count) << "\n"               //
        << BATT_INSPECT(KeyQuery::metrics().try_pin_leaf_count) << "\n"                //
        << BATT_INSPECT(KeyQuery::metrics().try_pin_leaf_success_count) << "\n"        //
        << BATT_INSPECT(KeyQuery::metrics().sharded_view_find_count) << "\n"           //
        << BATT_INSPECT(KeyQuery::metrics().sharded_view_find_success_count) << "\n"   //
        << BATT_INSPECT(KeyQuery::metrics().filter_positive_count) << "\n"             //
        << BATT_INSPECT(KeyQuery::metrics().filter_false_positive_count) << "\n"       //
        << "\n"                                                                        //
        << BATT_INSPECT(KeyQuery::metrics().filter_false_positive_rate()) << "\n"      //
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
        << BATT_INSPECT(cache_slot_pool.hit_rate()) << "\n"                            //
        << BATT_INSPECT(cache_slot_pool.admit_byte_count) << "\n"                      //
        << BATT_INSPECT(cache_slot_pool.evict_byte_count) << "\n"                      //
        << BATT_INSPECT(cache_slot_pool.evict_lru_count) << "\n"                       //
        << BATT_INSPECT(cache_slot_pool.background_evict_count) << "\n"                //
        << BATT_INSPECT(cache_slot_pool.background_evict_fail_count) << "\n"           //
        << BATT_INSPECT(cache_slot_pool.background_evict_byte_count) << "\n"           //
        << BATT_INSPECT(cache_slot_pool.background_evict_latency) << "\n"              //
        << BATT_INSPECT(cache_slot_pool.background_evict_byte_latency) << "\n"         //
        << "\n"                                                                        //
        << BATT_INSPECT(page_reads_per_get_4k) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_8k) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_16k) << "\n"                                //
        << BATT_INSPECT(page_reads_per_get_32k) << "\n"                                //
        << BATT_INSPECT(page_reads_per_get_64k) << "\n"                                //
        << BATT_INSPECT(page_reads_per_get_128k) << "\n"                               //
        << BATT_INSPECT(page_reads_per_get_256k) << "\n"                               //
        << BATT_INSPECT(page_reads_per_get_512k) << "\n"                               //
        << BATT_INSPECT(page_reads_per_get_1m) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_2m) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_4m) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_8m) << "\n"                                 //
        << BATT_INSPECT(page_reads_per_get_16m) << "\n"                                //
        << BATT_INSPECT(page_reads_per_get_32m) << "\n"                                //
        << BATT_INSPECT(page_reads_per_get_64m) << "\n"                                //
        << "\n"                                                                        //
        << dump_memory_stats() << "\n"                                                 //
        ;
  };
}

}  // namespace turtle_kv
