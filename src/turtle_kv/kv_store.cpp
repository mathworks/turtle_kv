#include <turtle_kv/kv_store.hpp>
//

#include <turtle_kv/checkpoint_log.hpp>
#include <turtle_kv/file_utils.hpp>
#include <turtle_kv/page_file.hpp>

#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>

#include <turtle_kv/import/constants.hpp>

#include <llfs/bloom_filter_page_view.hpp>

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
/*static*/ Status KVStore::create(const std::filesystem::path& dir_path,  //
                                  const Config& config,                   //
                                  RemoveExisting remove) noexcept
{
  StatusOr<llfs::ScopedIoRing> scoped_io_ring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{4096},  //
                                   llfs::ThreadPoolSize{1});
  BATT_REQUIRE_OK(scoped_io_ring);

  auto p_storage_context =
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
    batt::TaskScheduler& task_scheduler,    //
    batt::WorkerPool& worker_pool,          //
    llfs::StorageContext& storage_context,  //
    const std::filesystem::path& dir_path,  //
    const TreeOptions& tree_options         //
    ) noexcept
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

  return {std::unique_ptr<KVStore>{new KVStore{task_scheduler,                //
                                               worker_pool,                   //
                                               tree_options,                  //
                                               std::move(change_log_writer),  //
                                               std::move(checkpoint_log_volume)}}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KVStore::KVStore(batt::TaskScheduler& task_scheduler,                   //
                              batt::WorkerPool& worker_pool,                         //
                              const TreeOptions& tree_options,                       //
                              std::unique_ptr<ChangeLogWriter>&& change_log_writer,  //
                              std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept
    : task_scheduler_{task_scheduler}
    , worker_pool_{worker_pool}
    , tree_options_{tree_options}
    , checkpoint_log_{std::move(checkpoint_log)}
    , mem_table_{new MemTable{
          std::move(change_log_writer),
          /*max_byte_size=*/this->tree_options_.leaf_data_size(),
          DeltaBatchId{1}.to_mem_table_id(),
      }}
    , deltas_{}
    , checkpoint_token_pool_{std::make_shared<batt::Grant::Issuer>(
          /*max_concurrent_checkpoint_jobs=*/1)}
    , base_checkpoint_{Checkpoint::empty_at_batch(DeltaBatchId::from_u64(0))}
    , checkpoint_generator_{
          this->worker_pool_,                                     //
          this->tree_options_,                                    //
          this->page_cache(),                                     //
          batt::make_copy(this->base_checkpoint_),  //
          *this->checkpoint_log_,                                 //
      }
    , info_task_{this->task_scheduler_.schedule_task(),
    [this]{this->info_task_main(); }, "KVStore::info_task",}
    , memtable_compact_thread_{[this]{this->memtable_compact_thread_main();}}
    , checkpoint_update_thread_{[this]{this->checkpoint_update_thread_main();}}
    , checkpoint_flush_thread_{[this]{this->checkpoint_flush_thread_main();}}
{
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
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KVStore::~KVStore() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::halt()
{
  this->halt_.set_value(true);
  this->memtable_compact_channel_.close();
  this->checkpoint_update_channel_.close();
  this->checkpoint_flush_channel_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::join()
{
  this->memtable_compact_thread_.join();
  this->checkpoint_update_thread_.join();
  this->checkpoint_flush_thread_.join();
  this->info_task_.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status KVStore::put(const KeyView& key, const ValueView& value) noexcept /*override*/
{
  Status status = this->mem_table_->put(key, value);

  if (status == batt::StatusCode::kResourceExhausted) {
    BATT_REQUIRE_OK(this->update_checkpoint());
    return this->put(key, value);
  }

  return status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> KVStore::get(const KeyView& key) noexcept /*override*/
{
  if ((this->query_count_.fetch_add(1) & 0xffff) == 0) {
    absl::WriterMutexLock checkpoint_lock{&this->base_checkpoint_mutex_};
    this->query_page_loader_.emplace(this->page_cache());
  }

  absl::ReaderMutexLock checkpoint_lock{&this->base_checkpoint_mutex_};

  Optional<ValueView> value = BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<13>{},
                                                          this->metrics_.mem_table_get_latency,
                                                          this->mem_table_->get(key));

  if (value) {
    if (!value->needs_combine()) {
      this->metrics_.mem_table_get_count.add(1);
      return *value;
    }
  }

  for (usize i = this->deltas_.size(); i != 0;) {
    --i;
    const boost::intrusive_ptr<MemTable>& delta = this->deltas_[i];

    Optional<ValueView> delta_value = BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<20>{},
                                                                  this->metrics_.delta_get_latency,
                                                                  delta->finalized_get(key));

    if (delta_value) {
      if (value) {
        *value = combine(*value, *delta_value);
        if (!value->needs_combine()) {
          this->metrics_.delta_log2_get_count[batt::log2_ceil(this->deltas_.size() - i)].add(1);
          return *value;
        }
      } else {
        if (!delta_value->needs_combine()) {
          this->metrics_.delta_log2_get_count[batt::log2_ceil(this->deltas_.size() - i)].add(1);
          return *delta_value;
        }
        value = delta_value;
      }
    }
  }

  llfs::PinnedPage pinned_page_out;

  BATT_CHECK(this->base_checkpoint_.tree()->is_serialized());

#if TURTLE_KV_ENABLE_LEAF_FILTERS
  FilteredKeyQuery query{this->page_cache(), *this->query_page_loader_, pinned_page_out, key};
#endif

  StatusOr<ValueView> value_from_checkpoint =
      BATT_COLLECT_LATENCY_SAMPLE(batt::Every2ToTheConst<12>{},
                                  this->metrics_.checkpoint_get_latency,
                                  this->base_checkpoint_.tree()->
  //----- --- -- -  -  -   -
#if TURTLE_KV_ENABLE_LEAF_FILTERS
                                  find_key_filtered(query)
  // ----- --- -- -  -  -   -
#else
                                  find_key(*this->query_page_loader_, pinned_page_out, key)
  // ----- --- -- -  -  -   -
#endif
      );

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
  (void)min_key;
  (void)items_out;

  return {batt::StatusCode::kUnimplemented};
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
Status KVStore::update_checkpoint() noexcept
{
  // The current MemTable is done accepting new updates!
  //
  std::unique_ptr<ChangeLogWriter> change_log_writer = this->mem_table_->finalize();

  // Gather some information from the current MemTable before we send it off.
  //
  const DeltaBatchId batch_id = DeltaBatchId::from_mem_table_id(this->mem_table_->id());
  const DeltaBatchId next_batch_id = batch_id.next();

  // Create a new MemTable to accept more updates.
  //
  boost::intrusive_ptr<MemTable> old_mem_table = std::move(this->mem_table_);

  this->mem_table_.reset(new MemTable{
      std::move(change_log_writer),
      /*max_byte_size=*/this->tree_options_.flush_size(),
      next_batch_id.to_mem_table_id(),
  });

  {
    absl::WriterMutexLock checkpoint_lock{&this->base_checkpoint_mutex_};

    // Add to deltas stack, for queries on recent data.
    //
    this->deltas_.emplace_back(old_mem_table);
  }

  BATT_REQUIRE_OK(this->memtable_compact_channel_.write(std::move(old_mem_table)));

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
void KVStore::memtable_compact_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      BATT_ASSIGN_OK_RESULT(boost::intrusive_ptr<MemTable> mem_table,
                            this->memtable_compact_channel_.read());

      if (mem_table == nullptr) {
        BATT_REQUIRE_OK(this->checkpoint_update_channel_.write(nullptr));
        continue;
      }

      // Convert the MemTable to a DeltaBatch; this compacts all updates per key.
      //
      std::unique_ptr<DeltaBatch> delta_batch = std::make_unique<DeltaBatch>(std::move(mem_table));

      BATT_COLLECT_LATENCY(this->metrics_.compact_batch_latency,
                           delta_batch->merge_compact_edits());

      this->metrics_.batch_edits_count.add(delta_batch->result_set_size());

      BATT_REQUIRE_OK(this->checkpoint_update_channel_.write(std::move(delta_batch)));
    }
  }();

  LOG(INFO) << "memtable_compact_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::checkpoint_update_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      BATT_ASSIGN_OK_RESULT(std::unique_ptr<DeltaBatch> delta_batch,
                            this->checkpoint_update_channel_.read());

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

      // If we have reached the target checkpoint distance, then flush the checkpoint and start a
      // new one.
      //
      if (this->checkpoint_batch_count_ >= this->checkpoint_distance_.load()) {
        this->metrics_.checkpoint_count.add(1);

        // Allocate a token for the checkpoint job.
        //
        BATT_ASSIGN_OK_RESULT(
            batt::Grant checkpoint_token,
            this->checkpoint_token_pool_->issue_grant(1, batt::WaitForResource::kTrue));

        // Serialize all pages and create the job.
        //
        StatusOr<std::unique_ptr<CheckpointJob>> checkpoint_job =                   //
            BATT_COLLECT_LATENCY(this->metrics_.finalize_checkpoint_latency,        //
                                 this->checkpoint_generator_.finalize_checkpoint(   //
                                     std::move(checkpoint_token),                   //
                                     batt::make_copy(this->checkpoint_token_pool_)  //
                                     ));

        BATT_REQUIRE_OK(checkpoint_job);

        // Number of batches in the current checkpoint resets to zero.
        //
        this->checkpoint_batch_count_ = 0;

        BATT_CHECK_NOT_NULLPTR(*checkpoint_job);
        BATT_CHECK((*checkpoint_job)->append_job_grant);
        BATT_CHECK((*checkpoint_job)->appendable_job);
        BATT_CHECK((*checkpoint_job)->prepare_slot_sequencer);
        BATT_CHECK_NE((*checkpoint_job)->batch_count, 0);

        BATT_REQUIRE_OK(this->checkpoint_flush_channel_.write(std::move(*checkpoint_job)));
      }
    }
  }();

  LOG(INFO) << "checkpoint_update_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KVStore::checkpoint_flush_thread_main()
{
  Status status = [this]() -> Status {
    for (;;) {
      BATT_ASSIGN_OK_RESULT(std::unique_ptr<CheckpointJob> checkpoint_job,
                            this->checkpoint_flush_channel_.read());

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
        absl::WriterMutexLock checkpoint_lock{&this->base_checkpoint_mutex_};

        // Save the old checkpoint's slot lower bound so we can trim to it later.
        //
        Optional<llfs::SlotRange> prev_slot_range = this->base_checkpoint_.slot_range();
        if (prev_slot_range) {
          prev_checkpoint_slot = prev_slot_range->lower_bound;
        }

        this->base_checkpoint_ = std::move(*checkpoint_job->checkpoint);
        this->base_checkpoint_.notify_durable(std::move(*slot_read_lock));
        this->deltas_.erase(this->deltas_.begin(),
                            this->deltas_.begin() + checkpoint_job->batch_count);

        BATT_CHECK(this->base_checkpoint_.tree()->is_serialized());
      }

      // Trim the checkpoint volume to free old pages.
      //
      if (prev_checkpoint_slot) {
        BATT_REQUIRE_OK(this->checkpoint_log_->trim(*prev_checkpoint_slot));
      }
    }
  }();

  LOG(INFO) << "checkpoint_flush_thread done: " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> KVStore::debug_info() noexcept
{
  return [this](std::ostream& out) {
    auto& kv_store = this->metrics_;
    auto& checkpoint_log = *this->checkpoint_log_;
    auto& cache = checkpoint_log.cache();
    auto& change_log_file = this->mem_table_->change_log_writer().change_log_file();
    auto& change_log_writer = this->mem_table_->change_log_writer().metrics();

    auto& leaf_cache = cache.metrics_for_page_size(this->tree_options_.leaf_size());
    auto& node_cache = cache.metrics_for_page_size(this->tree_options_.node_size());
    auto& filter_cache = cache.metrics_for_page_size(this->tree_options_.filter_page_size());

    auto& page_cache = cache.metrics();

    auto& node = InMemoryNode::metrics();

    auto& query_page_loader = PinningPageLoader::metrics();

    double leaf_query_count = (PackedLeafPage::metrics().find_key_failure_count.get() +
                               PackedLeafPage::metrics().find_key_success_count.get());

    double filter_positive_count = leaf_query_count;

    double filter_false_positive_rate =
        (double)PackedLeafPage::metrics().find_key_failure_count.get() / filter_positive_count;

    out << "\n"
        << BATT_INSPECT(kv_store.mem_table_get_count) << "\n"                               //
        << BATT_INSPECT(kv_store.mem_table_get_latency) << "\n"                             //
        << "\n"                                                                             //
        << BATT_INSPECT(kv_store.delta_log2_get_count[0]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[1]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[2]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[3]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[4]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[5]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[6]) << "\n"                           //
        << BATT_INSPECT(kv_store.delta_log2_get_count[7]) << "\n"                           //
        << "\n"                                                                             //
        << BATT_INSPECT(kv_store.delta_get_latency) << "\n"                                 //
        << "\n"                                                                             //
        << BATT_INSPECT(kv_store.checkpoint_get_count) << "\n"                              //
        << BATT_INSPECT(kv_store.checkpoint_get_latency) << "\n"                            //
        << "\n"                                                                             //
        << BATT_INSPECT(node.level_depth_stats) << "\n"                                     //
        << "\n"                                                                             //
        << BATT_INSPECT(page_cache.get_count) << "\n"                                       //
        << BATT_INSPECT(page_cache.allocate_page_alloc_latency) << "\n"                     //
        << BATT_INSPECT(page_cache.page_read_latency) << "\n"                               //
        << BATT_INSPECT(page_cache.total_bytes_read) << "\n"                                //
        << "\n"                                                                             //
        << BATT_INSPECT(query_page_loader.prefetch_hint_latency) << "\n"                    //
        << BATT_INSPECT(query_page_loader.hash_map_lookup_latency) << "\n"                  //
        << BATT_INSPECT(query_page_loader.get_page_from_cache_latency) << "\n"              //
        << BATT_INSPECT(query_page_loader.get_page_count) << "\n"                           //
        << BATT_INSPECT(query_page_loader.hash_map_miss_count) << "\n"                      //
        << "\n"                                                                             //
        << BATT_INSPECT(kv_store.checkpoint_count) << "\n"                                  //
        << BATT_INSPECT(kv_store.batch_edits_count) << "\n"                                 //
        << BATT_INSPECT(kv_store.avg_edits_per_batch()) << "\n"                             //
        << BATT_INSPECT(kv_store.compact_batch_latency) << "\n"                             //
        << BATT_INSPECT(kv_store.push_batch_latency) << "\n"                                //
        << BATT_INSPECT(kv_store.finalize_checkpoint_latency) << "\n"                       //
        << BATT_INSPECT(kv_store.append_job_latency) << "\n"                                //
        << "\n"                                                                             //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_success_count) << "\n"           //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_failure_count) << "\n"           //
        << BATT_INSPECT(PackedLeafPage::metrics().find_key_latency) << "\n"                 //
        << "\n"                                                                             //
        << BATT_INSPECT(filter_false_positive_rate) << "\n"                                 //
        << "\n"                                                                             //
        << BATT_INSPECT(BloomFilterMetrics::instance().word_count_stats) << "\n"            //
        << BATT_INSPECT(BloomFilterMetrics::instance().byte_size_stats) << "\n"             //
        << BATT_INSPECT(BloomFilterMetrics::instance().bit_size_stats) << "\n"              //
        << BATT_INSPECT(BloomFilterMetrics::instance().bit_count_stats) << "\n"             //
        << BATT_INSPECT(BloomFilterMetrics::instance().item_count_stats) << "\n"            //
        << "\n"                                                                             //
        << BATT_INSPECT(QuotientFilterMetrics::instance().byte_size_stats) << "\n"          //
        << BATT_INSPECT(QuotientFilterMetrics::instance().bit_size_stats) << "\n"           //
        << BATT_INSPECT(QuotientFilterMetrics::instance().item_count_stats) << "\n"         //
        << BATT_INSPECT(QuotientFilterMetrics::instance().bits_per_key_stats) << "\n"       //
        << "\n"                                                                             //
        << BATT_INSPECT(FilteredKeyQuery::metrics().reject_page_latency) << "\n"            //
        << BATT_INSPECT(FilteredKeyQuery::metrics().filter_lookup_latency) << "\n"          //
        << BATT_INSPECT(FilteredKeyQuery::metrics().total_filter_query_count) << "\n"       //
        << BATT_INSPECT(FilteredKeyQuery::metrics().no_filter_page_count) << "\n"           //
        << BATT_INSPECT(FilteredKeyQuery::metrics().filter_page_load_failed_count) << "\n"  //
        << BATT_INSPECT(FilteredKeyQuery::metrics().page_id_mismatch_count) << "\n"         //
        << BATT_INSPECT(FilteredKeyQuery::metrics().filter_reject_count) << "\n"            //
        << "\n"                                                                             //
        << BATT_INSPECT(checkpoint_log.root_log_space()) << "\n"                            //
        << BATT_INSPECT(checkpoint_log.root_log_size()) << "\n"                             //
        << BATT_INSPECT(checkpoint_log.root_log_capacity()) << "\n"                         //
        << "\n"                                                                             //
        << BATT_INSPECT(change_log_file.active_blocks()) << "\n"                            //
        << BATT_INSPECT(change_log_file.active_block_count()) << "\n"                       //
        << BATT_INSPECT(change_log_file.config().block_count) << "\n"                       //
        << BATT_INSPECT(change_log_file.capacity()) << "\n"                                 //
        << BATT_INSPECT(change_log_file.size()) << "\n"                                     //
        << BATT_INSPECT(change_log_file.space()) << "\n"                                    //
        << BATT_INSPECT(change_log_file.available_block_tokens()) << "\n"                   //
        << BATT_INSPECT(change_log_file.in_use_block_tokens()) << "\n"                      //
        << BATT_INSPECT(change_log_file.reserved_block_tokens()) << "\n"                    //
        << BATT_INSPECT(change_log_file.metrics().freed_blocks_count) << "\n"               //
        << BATT_INSPECT(change_log_file.metrics().reserved_blocks_count) << "\n"            //
        << "\n"                                                                             //
        << BATT_INSPECT(change_log_writer.received_user_byte_count) << "\n"                 //
        << BATT_INSPECT(change_log_writer.received_block_byte_count) << "\n"                //
        << BATT_INSPECT(change_log_writer.written_user_byte_count) << "\n"                  //
        << BATT_INSPECT(change_log_writer.written_block_byte_count) << "\n"                 //
        << BATT_INSPECT(change_log_writer.sleep_count) << "\n"                              //
        << BATT_INSPECT(change_log_writer.write_count) << "\n"                              //
        << BATT_INSPECT(change_log_writer.block_alloc_count) << "\n"                        //
        << BATT_INSPECT(change_log_writer.block_utilization_rate()) << "\n"                 //
        << "\n"                                                                             //
        << BATT_INSPECT(leaf_cache.hit_rate()) << "\n"                                      //
        << BATT_INSPECT(node_cache.hit_rate()) << "\n"                                      //
        << BATT_INSPECT(filter_cache.hit_rate()) << "\n"                                    //
        ;
  };
}

}  // namespace turtle_kv
