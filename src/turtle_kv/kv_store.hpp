#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_generator.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/mem_table.hpp>

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/table.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <batteries/small_vec.hpp>

#include <boost/intrusive_ptr.hpp>

#include <filesystem>
#include <memory>

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
  explicit KVStore(batt::TaskScheduler& task_scheduler,                   //
                   batt::WorkerPool& worker_pool,                         //
                   const TreeOptions& tree_options,                       //
                   std::unique_ptr<ChangeLogWriter>&& change_log_writer,  //
                   std::unique_ptr<llfs::Volume>&& checkpoint_log) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status update_checkpoint() noexcept;

  Status flush_checkpoint() noexcept;

  void info_task_main() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::TaskScheduler& task_scheduler_;

  batt::WorkerPool& worker_pool_;

  TreeOptions tree_options_;

  std::atomic<usize> checkpoint_distance_{1};

  std::unique_ptr<llfs::Volume> checkpoint_log_;

  boost::intrusive_ptr<MemTable> mem_table_;

  // Recent MemTables that have been compacted/finalized; newest=back, oldest=front.
  //
  std::vector<boost::intrusive_ptr<MemTable>> deltas_;

  std::shared_ptr<batt::Grant::Issuer> checkpoint_token_pool_;

  usize checkpoint_batch_count_{0};

  std::unique_ptr<llfs::PageCacheJob> reader_job_ = this->page_cache().new_job();

  Checkpoint base_checkpoint_;

  CheckpointGenerator checkpoint_generator_;

  KVStoreMetrics metrics_;

  batt::Watch<bool> halt_{false};

  batt::Task info_task_;
};

}  // namespace turtle_kv
