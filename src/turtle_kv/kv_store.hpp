#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_generator.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/mem_table.hpp>

#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/util/object_thread_storage.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>
#include <turtle_kv/util/pipeline_channel.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/hint.hpp>
#include <batteries/small_vec.hpp>

#include <absl/synchronization/mutex.h>

#include <boost/intrusive_ptr.hpp>

#include <filesystem>
#include <memory>
#include <thread>

namespace turtle_kv {

/** \brief A Key/Value store.
 */
class KVStore : public Table
{
 public:
  friend class KVStoreScanner;

  struct Config {
    TreeOptions tree_options = TreeOptions::with_default_values();
    u64 initial_capacity_bytes = 0;
    u64 change_log_size_bytes = 0;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static Config with_default_values() noexcept;
  };

  struct RuntimeOptions {
    usize initial_checkpoint_distance;
    bool use_threaded_checkpoint_pipeline;
    usize cache_size_bytes;
    usize memtable_compact_threads;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static RuntimeOptions with_default_values() noexcept;
  };

  struct ThreadContext {
    llfs::PageCache& page_cache;
    boost::intrusive_ptr<llfs::StorageContext> storage_context;
    Optional<PinningPageLoader> query_page_loader;
    Optional<PageSliceStorage> query_result_storage;
    Optional<PageSliceStorage> scan_result_storage;
    u64 query_count = 0;
    ChangeLogWriter& log_writer_;
    u64 current_mem_table_id = 0;
    Optional<ChangeLogWriter::Context> log_writer_context_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit ThreadContext(KVStore* kv_store) noexcept
        : page_cache{kv_store->page_cache()}
        , storage_context{kv_store->storage_context_}
        , query_page_loader{this->page_cache}
        , log_writer_{*kv_store->log_writer_}
        , log_writer_context_{this->log_writer_}
    {
    }

    ChangeLogWriter::Context& log_writer_context(u64 mem_table_id)
    {
      if (BATT_HINT_FALSE(mem_table_id != this->current_mem_table_id)) {
        this->log_writer_context_.emplace(this->log_writer_);
        this->current_mem_table_id = mem_table_id;
      }
      return *this->log_writer_context_;
    }

    llfs::PageLoader& get_page_loader();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Status configure_storage_context(llfs::StorageContext& storage_context,
                                          const TreeOptions& tree_options,
                                          const RuntimeOptions& runtime_options) noexcept;

  static Status create(llfs::StorageContext& storage_context,
                       const std::filesystem::path& dir_path,
                       const Config& config,
                       RemoveExisting remove) noexcept;

  static Status create(const std::filesystem::path& dir_path,
                       const Config& config,
                       RemoveExisting remove) noexcept;

  static StatusOr<std::unique_ptr<KVStore>> open(
      batt::TaskScheduler& task_scheduler,
      batt::WorkerPool& worker_pool,
      llfs::StorageContext& storage_context,
      const std::filesystem::path& dir_path,
      const TreeOptions& tree_options,
      Optional<RuntimeOptions> runtime_options = None,
      llfs::ScopedIoRing&& scoped_io_ring = llfs::ScopedIoRing{}) noexcept;

  static StatusOr<std::unique_ptr<KVStore>> open(
      const std::filesystem::path& dir_path,
      const TreeOptions& tree_options,
      Optional<RuntimeOptions> runtime_options = None) noexcept;

  static Status global_init();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ~KVStore() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void halt();

  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(const KeyView& key, const ValueView& value) noexcept override;

  StatusOr<ValueView> get(const KeyView& key) noexcept override;

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept override;

  Status remove(const KeyView& key) noexcept override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options() const
  {
    return this->tree_options_;
  }

  KVStoreMetrics& metrics() noexcept
  {
    return this->metrics_;
  }

  void set_checkpoint_distance(usize chi) noexcept;

  usize get_checkpoint_distance() const noexcept
  {
    return this->checkpoint_distance_.load();
  }

  Status force_checkpoint();

  std::function<void(std::ostream&)> debug_info() noexcept;

  llfs::PageCache& page_cache() noexcept
  {
    return this->checkpoint_log_->cache();
  }

  void reset_thread_context() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct State : batt::RefCounted<State> {
    mutable Optional<i64> last_epoch_;
    boost::intrusive_ptr<MemTable> mem_table_;
    std::vector<boost::intrusive_ptr<MemTable>> deltas_;
    Checkpoint base_checkpoint_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit KVStore(batt::TaskScheduler& task_scheduler,
                   batt::WorkerPool& worker_pool,
                   llfs::ScopedIoRing&& scoped_io_ring,
                   boost::intrusive_ptr<llfs::StorageContext>&& storage_context,
                   const TreeOptions& tree_options,
                   const RuntimeOptions& runtime_options,
                   std::unique_ptr<ChangeLogWriter>&& change_log_writer,
                   std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status update_checkpoint(const State* observed_state);

  void info_task_main() noexcept;

  std::unique_ptr<DeltaBatch> compact_memtable(boost::intrusive_ptr<MemTable>&& mem_table);

  void memtable_compact_thread_main(usize thread_i);

  StatusOr<std::unique_ptr<CheckpointJob>> apply_batch_to_checkpoint(
      std::unique_ptr<DeltaBatch>&& delta_batch);

  void checkpoint_update_thread_main();

  Status commit_checkpoint(std::unique_ptr<CheckpointJob>&& checkpoint_job);

  void checkpoint_flush_thread_main();

  void add_obsolete_state(const State* old_state);

  void epoch_thread_main();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStoreMetrics metrics_;

  batt::TaskScheduler& task_scheduler_;

  batt::WorkerPool& worker_pool_;

  llfs::ScopedIoRing scoped_io_ring_;

  boost::intrusive_ptr<llfs::StorageContext> storage_context_;

  TreeOptions tree_options_;

  RuntimeOptions runtime_options_;

  std::unique_ptr<ChangeLogWriter> log_writer_;

  std::atomic<usize> checkpoint_distance_;

  absl::Mutex base_checkpoint_mutex_;

  std::unique_ptr<llfs::Volume> checkpoint_log_;

  ObjectThreadStorage<KVStore::ThreadContext>::ScopedSlot per_thread_;

  std::atomic<i64> current_epoch_;

  std::atomic<const State*> state_;

  batt::CpuCacheLineIsolated<batt::Watch<usize>> deltas_size_;

  std::shared_ptr<batt::Grant::Issuer> checkpoint_token_pool_;

  batt::Watch<bool> halt_{false};

  batt::Task info_task_;

  // TODO [tastolfi 2025-04-11] Try moving the MemTable ordering to the compact thread (maintain a
  // heap?)
  //
  std::atomic<u64> next_mem_table_id_to_push_{MemTable::first_id()};

  std::unique_ptr<PipelineChannel<boost::intrusive_ptr<MemTable>>[]>
      memtable_compact_channels_storage_;

  Slice<PipelineChannel<boost::intrusive_ptr<MemTable>>> memtable_compact_channels_;

  std::atomic<u64> next_delta_batch_to_push_{MemTable::first_id()};

  //----- --- -- -  -  -   -
  // Checkpoint Update State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<DeltaBatch>> checkpoint_update_channel_;

  CheckpointGenerator checkpoint_generator_;

  usize checkpoint_batch_count_;

  //----- --- -- -  -  -   -
  // Obsolete states.
  //----- --- -- -  -  -   -

  absl::Mutex obsolete_states_mutex_;

  std::vector<boost::intrusive_ptr<const State>> obsolete_states_;

  //----- --- -- -  -  -   -
  // Checkpoint Flush State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<CheckpointJob>> checkpoint_flush_channel_;

  std::vector<std::thread> memtable_compact_threads_;

  Optional<std::thread> checkpoint_update_thread_;

  Optional<std::thread> checkpoint_flush_thread_;

  Optional<std::thread> epoch_thread_;
};

}  // namespace turtle_kv
