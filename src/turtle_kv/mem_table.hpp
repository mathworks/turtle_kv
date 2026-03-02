#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/concurrent_hash_index.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/mem_table_entry.hpp>
#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/art.hpp>
#include <turtle_kv/util/atomic.hpp>
#include <turtle_kv/util/env_param.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_cache_overcommit.hpp>

#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <algorithm>
#include <string_view>
#include <vector>

namespace turtle_kv {

TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_hash_index, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_ordered_index, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_count_latest_update_only, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_log, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_art, true);
TURTLE_KV_ENV_PARAM(u32, turtlekv_memtable_hash_bucket_div, 32);
TURTLE_KV_ENV_PARAM(usize, turtlekv_memtable_art_overhead_pct, 0);

namespace {
BATT_STATIC_ASSERT_TYPE_EQ(KeyView, std::string_view);
}

class MemTable : public batt::RefCounted<MemTable>
{
 public:
  using Self = MemTable;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct RuntimeOptions {
    /** \brief If true, then only count the latest version of each key update towards the size limit
     * of the MemTable; otherwise, count all edits (including key overwrites).
     */
    bool limit_size_by_latest_updates_only;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static RuntimeOptions with_default_values() noexcept;
  };

  class Scanner;

#if TURTLE_KV_BIG_MEM_TABLES

  /** \brief Produces a series of compacted batches from a finalized MemTable.
   */
  class BatchCompactor;

#endif  // TURTLE_KV_BIG_MEM_TABLES

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The base-2 log of the maximum number of hash index lock shards.
   */
  static constexpr i32 kMaxShardsLog2 = 8;

  /** \brief The maximum number of hash index lock shards.  The actual number of shards is the
   * minimum of this value and the number of available hardware threads at runtime.
   */
  static constexpr usize kMaxShards = usize{1} << MemTable::kMaxShardsLog2;

  /** \brief The number of change log block slots to pre-allocate in this object.
   */
  static constexpr usize kBlockListPreAllocSize = 4096;

  //----- --- -- -  -  -   -

#if !TURTLE_KV_BIG_MEM_TABLES

  static constexpr u32 kCompactionState_Todo = 0;
  static constexpr u32 kCompactionState_InProgress = 1;
  static constexpr u32 kCompactionState_Complete = 3;

#endif  // !TURTLE_KV_BIG_MEM_TABLES

  /** \brief this->magic_num_ is initialized to this value when a MemTable is constructed.
   */
  static constexpr u64 kAliveMagicNum = 0xeeb37c44b3a4598dull;

  /** \brief this->magic_num_ is set to this value when a MemTable is destructed.
   */
  static constexpr u64 kDeadMagicNum = 0xc910d14e24d0a51aull;

  /** \brief The number of new CacheLogBlock objects to allocate between updates to the PageCache
   * external allocation.  This config option trades overhead for accuracy; lower values favor
   * tighter (more accurate) tracking of cache space to the actual memory footprint of the MemTable,
   * whereas higher values favor lower overhead per MemTable update.
   */
  static constexpr usize kBlocksPerExternalCacheAllocUpdate = 128;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u64 first_id()
  {
    return 0x10000;
  }

  static u64 next_id()
  {
    static std::atomic<u64> next{MemTable::first_id()};
    return next.fetch_add(0x10000);
  }

  static u64 batch_id_from(u64 id)
  {
    return id & ~u64{0xffff};
  }

  static u64 block_id_from(u64 id)
  {
    return id & u64{0xffff};
  }

  static u64 next_id_for(u64 id)
  {
    return id + 0x10000;
  }

  static u64 prev_id_for(u64 id)
  {
    return id - 0x10000;
  }

  /** \brief Returns an integer indicating the position (starting at 0) of MemTable id `id` in the
   * sequence of all ids.
   */
  static u64 ordinal_from_id(u64 id)
  {
    return (id - Self::first_id()) >> 16;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemTable(llfs::PageCache& page_cache,
                    KVStoreMetrics& metrics,
                    usize max_bytes_per_batch,
                    usize max_batch_count,
                    Optional<u64> id = None) noexcept;

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  ~MemTable() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 id() const noexcept
  {
    return this->self_id_;
  }

  Status put(ChangeLogWriter::Context& context,
             const KeyView& key,
             const ValueView& value) noexcept;

  Optional<ValueView> get(const KeyView& key) noexcept;

  usize scan(const KeyView& min_key,
             const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  [[nodiscard]] bool finalize() noexcept;

  bool is_finalized() const
  {
    return this->is_finalized_.load();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  usize finalized_scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool has_hash_index() const noexcept
  {
    return bool{this->hash_index_};
  }

  ConcurrentHashIndex& hash_index()
  {
    BATT_CHECK(this->hash_index_);
    return *this->hash_index_;
  }

  bool has_ordered_index() const noexcept
  {
    return bool{this->ordered_index_};
  }

  ART<void>& ordered_index()
  {
    BATT_CHECK(this->ordered_index_);
    return *this->ordered_index_;
  }

  bool has_art_index() const noexcept
  {
    return bool{this->art_index_};
  }

  ART<MemTableValueEntry>& art_index()
  {
    BATT_CHECK(this->art_index_);
    return *this->art_index_;
  }

#if TURTLE_KV_BIG_MEM_TABLES

  /** \brief Returns the index of the last batch to be compacted from this MemTable.
   */
  u64 max_batch_index() const noexcept
  {
    return this->max_batch_index_.load();
  }

  /** \brief The last batch id for this MemTable; only accurate after all batches have been
   * compacted/consumed.
   */
  DeltaBatchId max_batch_id() const
  {
    return DeltaBatchId::from_mem_table_id(this->id(), this->max_batch_index());
  }

#else

  /** \brief Compact the entire MemTable so it can be used as an update batch.
   */
  MergeCompactor::ResultSet</*decay_to_items=*/false> compact() noexcept;

  /** \brief Returns the sorted, compacted edits of this MemTable as a single Slice, if compact()
   * has completed; None otherwise.  This function does not block.
   */
  Optional<Slice<const EditView>> poll_compacted_edits() const;

  /** \brief Waits for compacted edits to become available, then returns them.  Does not trigger
   * compaction; only waits for it to be completed.
   */
  Slice<const EditView> await_compacted_edits() const;

#endif  // !TURTLE_KV_BIG_MEM_TABLES

  /** \brief Returns the current maximum byte size limit.
   *
   * This can decrease depending on the maximum item size encountered.
   */
  usize max_byte_size() const
  {
    return BATT_CHECKED_CAST(usize, this->max_byte_size_.load());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct StorageImpl {
    MemTable& mem_table;
    ChangeLogWriter::Context& context;
    Status status;

    template <typename SerializeFn = void(u32 /*locator*/, const MutableBuffer&)>
    void store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> get_impl(const MemTableQuery& query, u64 shard_i) noexcept;

  usize scan_keys_impl(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  u64 get_next_block_owner_id() const noexcept
  {
    return this->self_id_ | (this->blocks_.size() & 0xffff);
  }

  ConstBuffer fetch_slot(u32 locator) const noexcept;

#if !TURTLE_KV_BIG_MEM_TABLES

  Slice<const EditView> compacted_edits_slice_impl() const;

  std::vector<EditView> compact_hash_index();

  std::vector<EditView> compact_art_index();

#endif  // !TURTLE_KV_BIG_MEM_TABLES

  i64 calculate_max_byte_size() const;

  /** \brief Returns the number of bytes to claim (if positive) or release (negative)
   * as external allocation from the PageCache.
   *
   * This function only calculates a non-zero value every `kBlocksPerExternalCacheAllocUpdate` new
   * ChangeLogBlocks added to the MemTable.
   */
  i64 update_external_cache_alloc();

  /** \brief Increases or decreases the PageCache external alloc by the specified number of bytes
   * `cache_alloc_delta`.
   */
  void handle_external_cache_alloc(i64 cache_alloc_delta);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> magic_num_{Self::kAliveMagicNum};

  llfs::PageCache& page_cache_;

  KVStoreMetrics& metrics_;

  ARTBase::Metrics art_metrics_;

  std::atomic<bool> is_finalized_;

  RuntimeOptions runtime_options_ = RuntimeOptions::with_default_values();

  //----- --- -- -  -  -   -
  //
  Optional<ConcurrentHashIndex> hash_index_;
  Optional<ART<void>> ordered_index_;
  //
  //----- --- -- -  -  -   -

  Optional<ART<MemTableValueEntry>> art_index_;

  const i64 max_bytes_per_batch_;

  const i64 max_batch_count_;

  std::atomic<i64> max_item_size_{32};

  std::atomic<i64> max_byte_size_;

  std::atomic<i64> current_byte_size_;

  u64 self_id_;

  u64 next_block_owner_id_;

  std::atomic<u32> version_;

  absl::Mutex block_list_mutex_;

  batt::SmallVec<ChangeLogBlock*, MemTable::kBlockListPreAllocSize> blocks_;

#if TURTLE_KV_BIG_MEM_TABLES

  std::atomic<u64> max_batch_index_{0};

#else  // TURTLE_KV_BIG_MEM_TABLES

  std::atomic<u32> compaction_state_{0};

  MergeCompactor::ResultSet</*decay_to_items=*/false> compacted_edits_;

#endif  // TURTLE_KV_BIG_MEM_TABLES

  // The total size (in bytes) of all change log block buffers owned by this MemTable.
  //
  usize block_size_total_ = 0;

  // The total size (in bytes) of all PageCache external allocations to account for the ART index.
  //
  i64 art_reserved_size_ = 0;

  // The number of calls to update_external_cache_alloc() since we actually updated the PageCache
  // external allocation.
  //
  usize since_last_cache_alloc_update_ = 0;

  // The total size (in bytes) of change log block buffers added to this since the last PageCache
  // external allocation.
  //
  usize block_size_last_update_ = 0;

  // Set to true when some thread calling MemTable::put is currently updating the external cache
  // alloc; if a thread tries to update the alloc and finds this is true, it will skip the update.
  //
  bool cache_alloc_in_progress_ = false;

  // Space in the PageCache that is allocated to cover the footprint of this MemTable, in order to
  // bound the overall memory footprint of a KVStore.
  //
  llfs::PageCache::ExternalAllocation total_cache_alloc_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SerializeFn>
void MemTable::StorageImpl::store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept
{
  this->status = batt::to_status(this->context.append_slot(
      this->mem_table.next_block_owner_id_,
      n_bytes,
      [&](ChangeLogWriter::BlockBuffer* buffer, const MutableBuffer& dst) {
        MemTable& mem_table = this->mem_table;

        BATT_CHECK_EQ(MemTable::batch_id_from(buffer->owner_id()), mem_table.self_id_);

        if (buffer->ref_count() == 1) {
          buffer->add_ref(1);
          mem_table.metrics_.mem_table_log_bytes_allocated.add(buffer->block_size());
          i64 cache_alloc_delta = 0;
          {
            absl::MutexLock lock{&this->mem_table.block_list_mutex_};

            mem_table.block_size_total_ += buffer->block_size();
            cache_alloc_delta = mem_table.update_external_cache_alloc();
            mem_table.blocks_.emplace_back(buffer);
            mem_table.next_block_owner_id_ = mem_table.get_next_block_owner_id();
          }
          mem_table.handle_external_cache_alloc(cache_alloc_delta);
        }

        const u32 block_id = MemTable::block_id_from(buffer->owner_id());
        const u32 slot_index = buffer->slot_count();
        const u32 slot_locator = (block_id << 16) | (slot_index & 0xffff);

        serialize_fn(slot_locator, dst);
      }));
}

/** \brief Returns the greatest ordered DeltaBatchId included in the passed MemTable.
 */
inline DeltaBatchId get_batch_upper_bound(const MemTable& mem_table)
{
  return mem_table.max_batch_id();
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

#if TURTLE_KV_BIG_MEM_TABLES

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Produces a series of compacted key/value runs, each of which is limited to a maximum
 * size, and can be applied to a checkpoint tree using batch update.
 */
class MemTable::BatchCompactor
{
 public:
  using Self = BatchCompactor;

  using ARTScanner =
      ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kFalse, /*kValuesOnly=*/true>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BatchCompactor(MemTable& mem_table, usize byte_size_limit) noexcept;

  BatchCompactor(const BatchCompactor&) = delete;
  BatchCompactor& operator=(const BatchCompactor&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff this finalized MemTable has more batches to compact.
   */
  bool has_next() const;

  /** \brief Collects, consumes, and returns the next batch of compacted updates.
   */
  MergeCompactor::ResultSet</*decay_to_items=*/false> consume_next() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  MemTable& mem_table_;

  const usize byte_size_limit_;

  u64 batch_count_;

  ARTScanner scanner_;
};

#endif  // TURTLE_KV_BIG_MEM_TABLES

}  // namespace turtle_kv
