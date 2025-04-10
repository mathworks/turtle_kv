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
#include <turtle_kv/util/pipeline_channel.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

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
  struct Config {
    TreeOptions tree_options = TreeOptions::with_default_values();
    u64 initial_capacity_bytes = 0;
    u64 change_log_size_bytes = 0;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static Config with_default_values() noexcept;
  };

  struct ThreadContext {
    Optional<PinningPageLoader> query_page_loader;
    u64 query_count = 0;
    ChangeLogWriter& log_writer;
    u64 current_mem_table_id = 0;
    Optional<ChangeLogWriter::Context> log_writer_context_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit ThreadContext(KVStore* kv_store) noexcept
        : query_page_loader{kv_store->page_cache()}
        , log_writer{kv_store->log_writer_ref_}
        , log_writer_context_{this->log_writer}
    {
    }

    ChangeLogWriter::Context& log_writer_context(u64 mem_table_id)
    {
      if (BATT_HINT_FALSE(mem_table_id != this->current_mem_table_id)) {
        this->log_writer_context_.emplace(this->log_writer);
        this->current_mem_table_id = mem_table_id;
      }
      return *this->log_writer_context_;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Status create(llfs::StorageContext& storage_context,  //
                       const std::filesystem::path& dir_path,  //
                       const Config& config,                   //
                       RemoveExisting remove) noexcept;

  static Status create(const std::filesystem::path& dir_path,  //
                       const Config& config,                   //
                       RemoveExisting remove) noexcept;

  static StatusOr<std::unique_ptr<KVStore>> open(batt::TaskScheduler& task_scheduler,    //
                                                 batt::WorkerPool& worker_pool,          //
                                                 llfs::StorageContext& storage_context,  //
                                                 const std::filesystem::path& dir_path,  //
                                                 const TreeOptions& tree_options) noexcept;

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

  KVStoreMetrics& metrics() noexcept
  {
    return this->metrics_;
  }

  void set_checkpoint_distance(usize chi) noexcept
  {
    BATT_CHECK_GT(chi, 0);
    // TODO [tastolfi 2025-02-21] check `chi` against the change log max size.

    this->checkpoint_distance_.store(chi);
  }

  std::function<void(std::ostream&)> debug_info() noexcept;

  llfs::PageCache& page_cache() noexcept
  {
    return this->checkpoint_log_->cache();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit KVStore(batt::TaskScheduler& task_scheduler,                   //
                   batt::WorkerPool& worker_pool,                         //
                   const TreeOptions& tree_options,                       //
                   std::unique_ptr<ChangeLogWriter>&& change_log_writer,  //
                   std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status update_checkpoint(MemTable* observed_mem_table, u64 observed_mem_table_id) noexcept;

  void info_task_main() noexcept;

  void memtable_compact_thread_main();

  void checkpoint_update_thread_main(const Checkpoint& base_checkpoint);

  void checkpoint_flush_thread_main();

  Optional<PinningPageLoader>& query_page_loader();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::TaskScheduler& task_scheduler_;

  batt::WorkerPool& worker_pool_;

  TreeOptions tree_options_;

  ChangeLogWriter& log_writer_ref_;

  std::atomic<usize> checkpoint_distance_{1};

  absl::Mutex base_checkpoint_mutex_;

  std::unique_ptr<llfs::Volume> checkpoint_log_;

  boost::intrusive_ptr<MemTable> mem_table_;  // ABSL_GUARDED_BY(base_checkpoint_mutex_);

  ObjectThreadStorage<KVStore::ThreadContext>::ScopedSlot per_thread_;

  // Recent MemTables that have been compacted/finalized; newest=back, oldest=front.
  //
  std::vector<boost::intrusive_ptr<MemTable>> deltas_ ABSL_GUARDED_BY(base_checkpoint_mutex_);

  std::shared_ptr<batt::Grant::Issuer> checkpoint_token_pool_;

  Checkpoint base_checkpoint_ ABSL_GUARDED_BY(base_checkpoint_mutex_);

  KVStoreMetrics metrics_;

  batt::Watch<bool> halt_{false};

  batt::Task info_task_;

  std::atomic<usize> query_count_{0};

  PipelineChannel<boost::intrusive_ptr<MemTable>> memtable_compact_channel_;

  PipelineChannel<std::unique_ptr<DeltaBatch>> checkpoint_update_channel_;

  PipelineChannel<std::unique_ptr<CheckpointJob>> checkpoint_flush_channel_;

  std::thread memtable_compact_thread_;

  std::thread checkpoint_update_thread_;

  std::thread checkpoint_flush_thread_;
};

}  // namespace turtle_kv
