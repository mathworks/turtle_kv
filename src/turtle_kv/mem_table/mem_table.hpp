//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/mem_table/mem_table_allocation_tracker_impl.hpp>
#include <turtle_kv/mem_table/mem_table_entry.hpp>
#include <turtle_kv/mem_table/mem_table_index.hpp>
#include <turtle_kv/mem_table/mem_table_metrics.hpp>
#include <turtle_kv/mem_table/mem_table_storage_impl.hpp>

#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/change_log/change_log_writer.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

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
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_cache_overcommit.hpp>

#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <algorithm>
#include <bit>
#include <string_view>
#include <vector>

namespace turtle_kv {

TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_count_latest_update_only, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_log, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_cache_alloc_art, true);
TURTLE_KV_ENV_PARAM(u32, turtlekv_memtable_hash_bucket_div, 32);
TURTLE_KV_ENV_PARAM(usize, turtlekv_memtable_art_overhead_pct, 0);

namespace {
BATT_STATIC_ASSERT_TYPE_EQ(KeyView, std::string_view);
}

class MemTableBase : public batt::RefCounted<MemTableBase>
{
 public:
  MemTableBase(const MemTableBase&) = delete;
  MemTableBase& operator=(const MemTableBase&) = delete;

  virtual ~MemTableBase() = default;

 protected:
  MemTableBase() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief An in-memory index for recent key/value updates.
 */
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
class BasicMemTable : public MemTableBase
{
 public:
  using Self = BasicMemTable;

  using AllocationTracker = AllocationTrackerT;

  using Storage = StorageT;
  using StorageWriter = typename Storage::Writer;
  using StorageWriterContext = typename Storage::WriterContext;
  using StorageBlockBuffer = typename Storage::BlockBuffer;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief A temporary object that captures the per-thread storage context needed to perform a
   * `put` operation.
   */
  class PerOpStorageContext;

  /** \brief Produces a series of compacted batches from a finalized MemTable.
   */
  class BatchCompactor;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The default assumed maximum item size in bytes.
   */
  static constexpr usize kDefaultItemSize = 32;

  /** \brief The number of change log block slots to pre-allocate in this object.
   */
  static constexpr usize kBlockListPreAllocSize = 4096;

  /** \brief this->magic_num_ is initialized to this value when a MemTable is constructed.
   */
  static constexpr u64 kAliveMagicNum = 0xeeb37c44b3a4598dull;

  /** \brief this->magic_num_ is set to this value when a MemTable is destructed.
   */
  static constexpr u64 kDeadMagicNum = 0xc910d14e24d0a51aull;

  /** \brief The number of new CacheLogBlock objects to allocate between updates to the
   * AllocationTracker external allocation.  This config option trades overhead for accuracy; lower
   * values favor tighter (more accurate) tracking of cache space to the actual memory footprint of
   * the MemTable, whereas higher values favor lower overhead per MemTable update.
   */
  static constexpr usize kBlocksPerExternalCacheAllocUpdate = 128;

  /** \brief Mask defining bits that are set in MemTable::prepare_total_ to indicate that
   * MemTable::finalize() has been called.
   */
  static constexpr i64 kFinalizedMask = i64{1} << 62;  // single bit, non-negative.
  static_assert(kFinalizedMask > 0);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicMemTable(AllocationTracker& allocation_tracker,
                         MemTableMetrics& metrics,
                         EditOffset edit_offset_lower_bound,
                         usize max_bytes_per_batch,
                         usize max_batch_count) noexcept;

  /** \brief BasicMemTable is not copyable.
   */
  BasicMemTable(const BasicMemTable&) = delete;

  /** \brief BasicMemTable is not copyable.
   */
  BasicMemTable& operator=(const BasicMemTable&) = delete;

  /** \brief Destroys the BasicMemTable, releasing all AllocationTracker allocations and
   * ChangeLogBlock references.
   */
  ~BasicMemTable() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Called during recovery to reconstruct a MemTable from the change log.
   *
   * Behaves the same as `put`, except that nothing is written to storage.
   */
  Status put_recovered_slot(FirstVisitToBlock first_visit,
                            StorageBlockBuffer* block,
                            EditOffset edit_offset,
                            const KeyView& key,
                            const ValueView& value);

  /** \brief Applies a single key/value update to the MemTable, recording the update in the
   * change log via the passed context.
   */
  StatusOr<EditOffset> put(StorageWriterContext& context,
                           const KeyView& key,
                           const ValueView& value) noexcept;

  /** \brief Returns the value currently bound to the passed key, if present; otherwise, returns
   * None.
   */
  Optional<ValueView> get(const KeyView& key) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Marks the MemTable as finalized (read-only), waits for any in-progress updates to
   * complete, and then returns true iff the calling thread is the first to call finalize().
   *
   * This function will only return true for a single (concurrent) caller.
   *
   * \return true iff this is the first call to finalize for this MemTable.
   */
  [[nodiscard]] bool finalize(const SmallFn<EditOffset()>& get_next_edit_offset) noexcept;

  /** \brief Blocks the caller until the MemTable is finalized; *does not cause the MemTable to
   * become finalized*.
   */
  void await_finalize() noexcept;

  /** \brief Returns true iff the MemTable is finalized.
   *
   * If a caller wishes to block waiting for the MemTable to be finalized, they may call either
   * `this->finalize(...)` (which will also cause the MemTable to *become* finalized) or
   * `this->await_finalized()` (which simply blocks the caller until some other thread/task
   * finalizes the MemTable).
   */
  bool is_finalized() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Same as `get`, but with synchronization; only safe to call after this->finalize() has
   * returned.
   */
  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART<MemTableValueEntry>& art_index()
  {
    return this->art_index_;
  }

  /** \brief Returns the starting EditOffset passed in at construction time.
   */
  EditOffset edit_offset_lower_bound() const
  {
    return this->edit_offset_lower_bound_;
  }

  /** \brief Returns the final EditOffset when this MemTable was finalized.  WARNING: Will panic if
   * this MemTable has not been finalized yet.
   *
   * \see is_finalized
   */
  EditOffset edit_offset_upper_bound() const
  {
    BATT_CHECK(this->is_finalized())
        << "The edit offset upper bound is not known until the MemTable is finalized!";

    return EditOffset{this->edit_offset_upper_bound_.load()};
  }

  /** \brief Returns the current maximum byte size limit.
   *
   * This can decrease depending on the maximum item size encountered.
   */
  usize max_byte_size() const
  {
    return BATT_CHECKED_CAST(usize, this->max_byte_size_.load());
  }

  /** \brief Returns true if no edits have been added to this MemTable.
   */
  bool empty()
  {
    return this->art_index_.empty();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Returns the maximum size (in bytes) to which this MemTable is allowed to grow,
   * considering its configured batch size, max batch count, and the maximum size of an edit seen so
   * far (which bound the amount of wasted space at the end of each batch, since edits may not be
   * split).
   */
  i64 calculate_max_byte_size() const;

  /** \brief Updates the cached maximum edit size and attempts to reserve space in the MemTable to
   * insert an edit of the specified byte count `packed_edit_size`.  If there is not enough room in
   * the MemTable, will revert changes to `this->prepared_bytes_total_` and return
   * batt::StatusCode::kResourceExhausted.  Otherwise, `this->prepared_bytes_total_` is increased.
   *
   * NOTE: any call to prepare_edit which returns an Ok status must be followed by a call to
   * commit_edit with the same amount once the edit has been added to the index and the change log.
   * Attempts to finalize a MemTable will block while there are pending edits (i.e., while the
   * 'committed' byte count is less than the 'prepared' byte count).
   *
   * Returns the previous value of `this->prepared_bytes_total_`.
   */
  StatusOr<i64> prepare_edit(i64 packed_edit_size);

  /** \brief Completes an edit operation initiated by `this->prepare_edit`.
   */
  void commit_edit(i64 packed_edit_size);

  /** \brief Adds `block_buffer` to `this->block_buffers_` after increasing its ref count by 1 via
   * `add_ref`.  If the caller is holding a lock on `this->block_list_mutex_`, then `lock_is_held`
   * must be passed as `LockIsHeld{true}`; otherwise it must be false.
   */
  void attach_block_buffer(StorageBlockBuffer* block_buffer, LockIsHeld lock_is_held);

  /** \brief Returns the number of bytes to claim (if positive) or release (negative)
   * as external allocation from the AllocationTracker.
   *
   * This function only calculates a non-zero value every `kBlocksPerExternalCacheAllocUpdate` new
   * ChangeLogBlocks added to the MemTable.
   *
   * IMPORTANT: This function must be called while holding the block_list_mutex_, and its return
   * value must be passed to `handle_external_cache_alloc` once the mutex has been released.
   */
  [[nodiscard]] i64 update_external_cache_alloc();

  /** \brief Increases or decreases the AllocationTracker external alloc by the specified number of
   * bytes `cache_alloc_delta`.
   */
  void handle_external_cache_alloc(i64 cache_alloc_delta);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> magic_num_{Self::kAliveMagicNum};

  // Used to limit the total memory footprint of the MemTable (storage blocks plus ART).
  //
  AllocationTracker& allocation_tracker_;

  // Diagnostic metrics for this object.
  //
  MemTableMetrics& metrics_;

  // Passed in at construction time.
  //
  const EditOffset edit_offset_lower_bound_;

  // Should be set after the MemTable is finalized.
  //
  std::atomic<i64> edit_offset_upper_bound_{this->edit_offset_lower_bound_.value() - 1};

  // The maximum size of a batch (produced post-finalization by MemTable::BatchCompactor).  Passed
  // in at construction time; used to calculate when the MemTable is full.
  //
  const i64 max_bytes_per_batch_;

  // The maximum number of batches (to be produced post-finalization by MemTable::BatchCompactor).
  // Passed in at construction time; used to calculate when the MemTable is full.
  //
  const i64 max_batch_count_;

  // Diagnostic metrics for `this->art_index_`.
  //
  ARTBase::Metrics art_metrics_;

  // In-memory index used for scans and point queries.
  //
  ART<MemTableValueEntry> art_index_;

  // Tracks the maximum observed key-value pair size (in bytes); this is used to estimate the
  // worst-case space wasted in a future batch.
  //
  batt::CpuCacheLineIsolated<std::atomic<i64>> max_item_size_{Self::kDefaultItemSize};

  // Updated whenever `this->max_item_size_` changes.
  //
  std::atomic<i64> max_byte_size_;

  // Set to true if an external alloc ever tiggers overcommit; the MemTable stops accepting new
  // edits when this happens.
  //
  std::atomic<bool> overcommit_triggered_{false};

  // Set to true if this MemTable ever fails an update because the change log reports it is out of
  // space.
  //
  std::atomic<bool> storage_is_full_{false};

  // Incremented by the packed size of an edit before that edit is added to the index.  This signals
  // to other threads that there is an ongoing update; it also allows the thread that increments
  // this field to determine whether the MemTable is at capacity (or has been finalized).
  //
  batt::CpuCacheLineIsolated<std::atomic<i64>> prepared_bytes_total_{0};

  // Incremented by the packed size of an edit once a thread is done adding that edit to the index.
  // In some cases this field is incremented even when an edit is rejected post-finalization; see
  // comments in the definition of `prepare_edit` for more details.
  //
  batt::CpuCacheLineIsolated<std::atomic<i64>> committed_bytes_total_{0};

  // Protects `this->block_buffers_`.
  //
  absl::Mutex block_list_mutex_;

  // The block buffers that hold packed key/value updates for this MemTable.  A ref count to each is
  // held to make sure the memory stays in scope for as long as the MemTable needs it.
  //
  batt::SmallVec<StorageBlockBuffer*, Self::kBlockListPreAllocSize> block_buffers_;

  // The total size (in bytes) of all change log block buffers owned by this MemTable.
  //
  usize block_size_total_ = 0;

  // The total size (in bytes) of all AllocationTracker external allocations to account for the ART
  // index.
  //
  i64 art_reserved_size_ = 0;

  // The number of calls to update_external_cache_alloc() since we actually updated the
  // AllocationTracker external allocation.
  //
  usize since_last_cache_alloc_update_ = 0;

  // The total size (in bytes) of change log block buffers added to this since the last
  // AllocationTracker external allocation.
  //
  usize block_size_last_update_ = 0;

  // Set to true when some thread calling MemTable::put is currently updating the external cache
  // alloc; if a thread tries to update the alloc and finds this is true, it will skip the update.
  //
  bool cache_alloc_in_progress_ = false;

  // Space in the AllocationTracker that is allocated to cover the footprint of this MemTable, in
  // order to bound the overall memory footprint of a KVStore.
  //
  typename AllocationTracker::ExternalAllocation total_cache_alloc_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

/** \brief A temporary object that captures the per-thread storage context needed to perform a
 * `put` operation.
 */
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
class BasicMemTable<StorageT, AllocationTrackerT>::PerOpStorageContext
{
 public:
  explicit PerOpStorageContext(BasicMemTable& mem_table,
                               StorageWriterContext& storage_writer_context) noexcept
      : mem_table_{mem_table}
      , storage_writer_context_{storage_writer_context}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename SerializeFn>
    requires(std::invocable<SerializeFn, MutableBuffer, EditOffset>)
  Status store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept
  {
    // Must be set to true before calling attach_block_buffer *iff* this function is holding a lock
    // on the block_list_mutex_.
    //
    bool holding_lock = false;

    // Set to true in the loop below if this thread observes the MemTable to have at least one
    // attached block buffers.
    //
    bool observed_attached_buffers = false;

    // Fills the prepared slot with the key/value update data.
    //
    const auto write_slot_data = [this, &serialize_fn, &holding_lock](FirstVisitToBlock first_visit,
                                                                      StorageBlockBuffer* buffer,
                                                                      MutableBuffer dst,
                                                                      EditOffset slot_edit_offset) {
      BATT_CHECK_EQ(buffer->slot_count() == 0, first_visit);
      if (first_visit) {
        this->mem_table_.attach_block_buffer(buffer, LockIsHeld{holding_lock});
      }
      serialize_fn(dst, slot_edit_offset);
    };

    // We first attempt to append the log without doing any waiting.  If this fails, we may be able
    // to retry after waiting for the log to be trimmed, so the logic below is wrapped in a loop.
    //
    for (;;) {
      // Happy path: append the slot without waiting.
      //
      Status status =
          this->storage_writer_context_.append_slot(this->mem_table_.edit_offset_lower_bound(),
                                                    n_bytes,
                                                    batt::WaitForResource::kFalse,
                                                    write_slot_data);

      // Great!  Nothing like success, baby.
      //
      if (status.ok()) {
        return status;
      }

      // If this isn't our first time through the loop, we may have already observed there to be
      // block buffers attached to the MemTable.  If this is the case, then it is pointless to wait,
      // since this MemTable itself may be the very thing preventing trim from happening.  Update
      // metrics and fail.
      //
      if (observed_attached_buffers) {
        this->mem_table_.storage_is_full_.store(true);
        this->mem_table_.metrics_.storage_full_count.add(1);
        return status;
      }

      // One thread will acquire a lock, others will block at this point.
      //
      absl::MutexLock lock{&this->mem_table_.block_list_mutex_};

      // If there are no block buffers attached to the MemTable, then we may just have to wait until
      // the checkpoint update pipeline catches up.  If there are block buffers attached, then its
      // possible this thread may have been blocked acquiring the lock while the first thread did a
      // blocking `append_slot`.  In this case, it makes sense to try again, since the same trim
      // that freed up space in the log to unblock the first thread is likely to have also freed up
      // enough space for this thread to succeed as well.  Note that we have observed block buffers
      // attached, and retry.
      //
      if (!this->mem_table_.block_buffers_.empty()) {
        observed_attached_buffers = true;
        continue;
      }

      // Since we might end up calling MemTable::attach_block_buffers while holding
      // `block_list_mutex_`, set `holding_lock` to true so we don't try to re-lock inside
      // `write_slot_data`.
      //
      holding_lock = true;

      this->mem_table_.metrics_.wait_for_trim_count.add(1);

      // Wait for the log to be trimmed, then write the slot.  Never retry on this path, because
      // there is no reason to.
      //
      return this->storage_writer_context_.append_slot(this->mem_table_.edit_offset_lower_bound(),
                                                       n_bytes,
                                                       batt::WaitForResource::kTrue,
                                                       write_slot_data);
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  BasicMemTable& mem_table_;
  StorageWriterContext& storage_writer_context_;
};

/** \brief Returns the lower bound EditOffset included in the passed MemTable.
 */
template <typename StorageT, typename AllocationTrackerT>
inline EditOffset get_edit_offset_lower_bound(
    const BasicMemTable<StorageT, AllocationTrackerT>& mem_table)
{
  return mem_table.edit_offset_lower_bound();
}

/** \brief Returns the min upper bound EditOffset included in the passed MemTable.
 */
template <typename StorageT, typename AllocationTrackerT>
inline EditOffset get_edit_offset_upper_bound(
    const BasicMemTable<StorageT, AllocationTrackerT>& mem_table)
{
  return mem_table.edit_offset_upper_bound();
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Produces a series of compacted key/value runs, each of which is limited to a maximum
 * size, and can be applied to a checkpoint tree using batch update.
 */
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
class BasicMemTable<StorageT, AllocationTrackerT>::BatchCompactor
{
 public:
  using Self = BatchCompactor;

  using ARTScanner =
      ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kFalse, /*kValuesOnly=*/true>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BatchCompactor(BasicMemTable& mem_table, usize byte_size_limit) noexcept;

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
  // A finalized MemTable from which to generate batches.
  //
  BasicMemTable& mem_table_;

  // Batches are cut off when including the next edit (in key order) would exceed this limit.
  //
  const usize byte_size_limit_;

  // The number of batches generated so far.
  //
  u64 batch_count_;

  // Used to scan over edits in key order.
  //
  ARTScanner scanner_;
};

using MemTable = BasicMemTable<MemTableChangeLogStorage, MemTablePageCacheAllocationTracker>;

}  // namespace turtle_kv
