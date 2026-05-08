//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_KV_STORE_HPP

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/change_log/change_log_writer.hpp>
#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_generator.hpp>
#include <turtle_kv/kv_store_config.hpp>
#include <turtle_kv/kv_store_metrics.hpp>

#include <turtle_kv/mem_table/mem_table.hpp>

#include <turtle_kv/tree/filter_page_write_state.hpp>
#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>
#include <turtle_kv/util/pipeline_channel.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/object_thread_storage.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/toggle.hpp>
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
  using Self = KVStore;

  friend class KVStoreScanner;

  using Config = KVStoreConfig;
  using RuntimeOptions = KVStoreRuntimeOptions;

  struct ThreadContext {
    llfs::PageCache& page_cache;
    boost::intrusive_ptr<llfs::StorageContext> storage_context;
    Optional<PinningPageLoader> query_page_loader;
    Optional<PageSliceStorage> query_result_storage;
    Optional<PageSliceStorage> scan_result_storage;
    u64 query_count = 0;
    ChangeLogWriter& change_log_writer_;
    Optional<ChangeLogWriter::Context> log_context_;
    ChangeLogWriter::Context& change_log_writer_context_{*this->log_context_};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit ThreadContext(KVStore* kv_store) noexcept
        : page_cache{kv_store->page_cache()}
        , storage_context{kv_store->storage_context_}
        , query_page_loader{this->page_cache}
        , change_log_writer_{*kv_store->change_log_writer_}
        , log_context_{this->change_log_writer_}
    {
    }

    /** \brief Releases all resources for this context; the thread that calls this function MUST NOT
     * use the KVStore instance afterwards!
     */
    void release() noexcept
    {
      this->log_context_ = None;
      this->scan_result_storage = None;
      this->query_result_storage = None;
      this->query_page_loader = None;
      this->storage_context = nullptr;
    }

    llfs::PageLoader& get_page_loader();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief We allow 2 deltas, which are finalized MemTables, plus 1 active MemTable.
   *
   * This is because of the following update pipeline:
   *
   *```
   * ┌───────────┐
   * │   State   │  time ───▶
   * │───────────│
   * │  MemTable:│  M1─────────────────────M2──────────────────────M3───────────────────────────
   * │    Deltas:│  []────────────────────[M1]────────────────────[M1, M2]────────────────[M2]──
   * │Checkpoint:│  C0─────────────────────────────────────────────────────────────────────C1───
   * └───────────┘ ┌─────────────────────┐┌───────────────────────┐┌─────────────────────── ▲
   *               │filling MemTable M1  ││filling MemTable M2    ││filling MemTable M3     │
   *               └─────────────────────┘└───────────────────────┘└─────────────────────── │
   *                         finalize M1 │            finalize M2 │                         │
   *                                     ▼                        ▼                         │
   *                                     ┌────────────────────────┐┌─────────────────────── │
   *                                     │building Checkpoint C1  ││building Checkpoint C2  │
   *                                     └────────────────────────┘└─────────────────────── │
   *                                                    commit C1 │                         │
   *                                                              ▼            update State │
   *                                                              ┌─────────────────────────┐
   *                                                              │writing Checkpoint C1    │
   *                                                              └─────────────────────────┘
   *```
   */
  static constexpr usize kMaxDeltasSize = 2;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Performs various process-wide initialization.
   */
  static Status global_init();

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

  /** \brief Registers all required page layouts.  Must be done once per page cache instance.
   */
  static Status register_page_layouts(llfs::PageCache& page_cache);

  /** \brief Returns the latest checkpoint recovered from the passed volume.
   */
  static StatusOr<Checkpoint> recover_latest_checkpoint(llfs::Volume& checkpoint_log_volume);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStore(const KVStore&) = delete;
  KVStore& operator=(const KVStore&) = delete;

  ~KVStore() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Start shutting down the KVStore.
   *
   * This initiates, but does not wait for the completion of, shutdown.  See also KVStore::join.
   */
  void halt();

  /** \brief Waits for the KVStore to finish shutting down.
   */
  void join();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(const KeyView& key, const ValueView& value) noexcept override;

  StatusOr<ValueView> get(const KeyView& key) noexcept override;

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept override;

  StatusOr<usize> scan_keys(const KeyView& min_key, const Slice<KeyView>& keys_out) noexcept;

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

  /** \brief Finalizes the current active MemTable (if non-empty) and returns an EditOffset
   * corresponding to the upper bound of all edits up to that point.
   */
  StatusOr<EditOffset> force_checkpoint() noexcept;

  /** \brief Waits for the current checkpoint to reach the given EditOffset upper bound.
   */
  Status wait_for_checkpoint(EditOffset target) noexcept;

  std::function<void(std::ostream&)> debug_info() const noexcept;

  void collect_stats(
      std::function<void(std::string_view /*name*/, double /*value*/)> fn) const noexcept;

  llfs::PageCache& page_cache() noexcept
  {
    return this->page_cache_;
  }

  /** \brief Clears any caches for this KVStore scoped to the current thread.
   */
  void reset_thread_context() noexcept;

  /** \brief
   */
  void release_thread_context() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  static constexpr i32 kRecoveryNotStarted = 0;
  static constexpr i32 kRecoveryStarted = 1;
  static constexpr i32 kRecoveryComplete = 2;
  static constexpr i32 kRecoveryFailed = 3;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct State {
    /** \brief The active MemTable; this is where new edits are inserted/buffered.
     */
    boost::intrusive_ptr<MemTable> mem_table_;

    /** \brief Finalized MemTables which are still in-scope; i.e., their edit offset upper bound is
     * after that of this_>base_checkpoint_.  Stored in oldest(front)-to-newest(back) order.
     */
    std::vector<boost::intrusive_ptr<MemTable>> deltas_;

    /** \brief The most recent checkpoint; covers everything older than the deltas.
     */
    Optional<Checkpoint> base_checkpoint_;
  };

  static_assert(std::default_initializable<State>);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief (internal use only) KVStore instances must be created via `KVStore::open`.
   */
  explicit KVStore(batt::TaskScheduler& task_scheduler,
                   batt::WorkerPool& worker_pool,
                   llfs::ScopedIoRing&& scoped_io_ring,
                   boost::intrusive_ptr<llfs::StorageContext>&& storage_context,
                   const TreeOptions& tree_options,
                   const RuntimeOptions& runtime_options,
                   std::unique_ptr<llfs::Volume>&& checkpoint_log,
                   Checkpoint&& latest_recovered_checkpoint) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Initializes the `State` of the KVStore.
   */
  void initialize_state(Checkpoint&& latest_recovered_checkpoint);

  /** \brief Opens the change log file and recovers state from it; this is necessary to properly
   * initialize the KVStore.
   */
  Status run_recovery(const std::filesystem::path& change_log_file_path) noexcept;

  /** \brief Creates and returns a new MemTable, with current checkpoint distance settings and the
   * specified EditOffset lower bound.
   */
  boost::intrusive_ptr<MemTable> create_mem_table(EditOffset edit_offset_lower_bound);

  /** \brief Finalizes the MemTable in `observed_state`, creating a new MemTable to replace it (or
   * waiting for another thread to do so), and finally handing off the old MemTable to the
   * checkpoint update pipeline.
   */
  Status finalize_mem_table(boost::intrusive_ptr<MemTable>&& mem_table);

  /** \brief Waits for the passed MemTable to be the next one that should be pushed to
   * `this->finalized_mem_table_channel_`, and then pushes it to the channel.
   */
  Status push_mem_table_to_channel(boost::intrusive_ptr<MemTable>&& mem_table);

  /** \brief Creates a new MemTable (with the passed EditOffset as its lower bound) and swaps it in
   * to the active state.
   */
  Status reset_active_mem_table(EditOffset current_edit_offset);

  /** \brief Passes the given MemTable to the checkpoint update pipeline.
   *
   * This should be called only after `reset_active_mem_table` has installed a new MemTable.
   */
  Status hand_off_finalized_mem_table(boost::intrusive_ptr<MemTable>&& old_mem_table);

  /** \brief Blocks the caller until either the KVStore is `halt()`-ed or a new MemTable with
   * `edit_offset_lower_bound() >= target_edit_offset` is activated.
   */
  Status wait_for_new_mem_table(EditOffset target_edit_offset);

  /** \brief The entry point of a background task whose sole purpose is to report metrics and other
   * diagnostic information as `BATT_DEBUG_INFO` (so it will be printed when the debug info signal
   * is trapped).
   */
  void info_task_main() noexcept;

  template <typename Fn>
    requires std::invocable<Fn, std::unique_ptr<DeltaBatch>>
  Status scan_mem_table_to_build_batches(boost::intrusive_ptr<MemTable>&& mem_table,
                                         Fn&& consume_fn);

  void mem_table_batch_scanner_thread_main();

  StatusOr<std::unique_ptr<CheckpointJob>> apply_batch_to_checkpoint(
      std::unique_ptr<DeltaBatch>&& delta_batch);

  void checkpoint_update_thread_main();

  bool should_create_checkpoint() const
  {
    // If the batch count is greater than or equal to the checkpoint distance, we need to create a
    // checkpoint.
    //
    return this->checkpoint_batch_count_ >= this->checkpoint_distance_.load();
  }

  Status commit_checkpoint(std::unique_ptr<CheckpointJob>&& checkpoint_job);

  void checkpoint_flush_thread_main();

  /** \brief Called during recovery to recover MemTables one edit at a time.
   */
  Status recover_put(FirstVisitToBlock first_visit,
                     ChangeLogBlock* block,
                     EditOffset edit_offset,
                     ConstBuffer payload);

  void set_recovery_status(i32) noexcept;

  Status wait_for_recovery() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KVStoreMetrics metrics_;

  batt::TaskScheduler& task_scheduler_;

  batt::WorkerPool& worker_pool_;

  llfs::ScopedIoRing scoped_io_ring_;

  boost::intrusive_ptr<llfs::StorageContext> storage_context_;

  llfs::PageCache& page_cache_;

  TreeOptions tree_options_;

  RuntimeOptions runtime_options_;

  MemTablePageCacheAllocationTracker mem_table_allocation_tracker_;

  std::unique_ptr<ChangeLogWriter> change_log_writer_;

  std::atomic<i32> recovery_status_;

  Optional<EditOffset> next_edit_offset_to_recover_;

  // How frequently we take checkpoints, where the units of distance are number of MemTables.
  // (i.e. if checkpoint_distance_ == 3, we take a checkpoint every time 3 MemTables are filled up)
  //
  std::atomic<usize> checkpoint_distance_;

  std::unique_ptr<llfs::Volume> checkpoint_log_;

  boost::intrusive_ptr<FilterPageWriteState> filter_page_write_state_;

  ObjectThreadStorage<KVStore::ThreadContext>::ScopedSlot per_thread_;

  batt::Toggle<State> state_;

  batt::CpuCacheLineIsolated<batt::Watch<usize>> deltas_size_;

  std::shared_ptr<batt::Grant::Issuer> checkpoint_token_pool_;

  batt::Watch<bool> halt_;

  Optional<batt::Task> info_task_;

  // The EditOffset lower bound of the next finalized MemTable to be pushed to the channel.
  //
  batt::Watch<i64> next_mem_table_edit_offset_;

  PipelineChannel<boost::intrusive_ptr<MemTable>> finalized_mem_table_channel_;

  //----- --- -- -  -  -   -
  // Checkpoint Update State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<DeltaBatch>> checkpoint_update_channel_;

  Optional<CheckpointGenerator> checkpoint_generator_;

  usize checkpoint_batch_count_;

  //----- --- -- -  -  -   -
  // Checkpoint Flush State.
  //----- --- -- -  -  -   -

  PipelineChannel<std::unique_ptr<CheckpointJob>> checkpoint_flush_channel_;

  //----- --- -- -  -  -   -
  // Threads for the Checkpoint update pipeline stages.
  //----- --- -- -  -  -   -

  Optional<std::thread> mem_table_batch_scanner_thread_;

  Optional<std::thread> checkpoint_update_thread_;

  Optional<std::thread> checkpoint_flush_thread_;
};

}  // namespace turtle_kv
