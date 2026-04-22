//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_HPP

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
                         const StorageWriter& storage_writer,
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

  Status put_recovered_slot(FirstVisitToBlock first_visit,
                            StorageBlockBuffer* block,
                            EditOffset edit_offset,
                            const KeyView& key,
                            const ValueView& value);

  /** \brief Applies a single key/value update to the MemTable, recording the update in the
   * change log via the passed context.
   */
  Status put(StorageWriterContext& context, const KeyView& key, const ValueView& value) noexcept;

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
  [[nodiscard]] bool finalize() noexcept;

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

  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // TODO: [Gabe Bornstein 1/7/25] The art index is public? That doesn't feel right...
  // tastolfi -> gbornste: I agree it is a bit messy; this is used by KVStoreScanner to construct an
  // ART scanner to merge all the different depth sorted runs (see kv_store_scanner.cpp:57).  The
  // reason it is currently expressed as a concrete type (rather than an abstract base class) is to
  // try to keep the overhead as low as we can for scanning.
  //
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
  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  void attach_block_buffer(StorageBlockBuffer* block_buffer);

  /** \brief Returns the number of bytes to claim (if positive) or release (negative)
   * as external allocation from the AllocationTracker.
   *
   * This function only calculates a non-zero value every `kBlocksPerExternalCacheAllocUpdate` new
   * ChangeLogBlocks added to the MemTable.
   */
  i64 update_external_cache_alloc();

  /** \brief Increases or decreases the AllocationTracker external alloc by the specified number of
   * bytes `cache_alloc_delta`.
   */
  void handle_external_cache_alloc(i64 cache_alloc_delta);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<u64> magic_num_{Self::kAliveMagicNum};

  AllocationTracker& allocation_tracker_;

  const StorageWriter& storage_writer_;

  MemTableMetrics& metrics_;

  // Passed in at construction time.
  //
  const EditOffset edit_offset_lower_bound_;

  // Should be set after the MemTable is finalized.
  //
  std::atomic<i64> edit_offset_upper_bound_{this->edit_offset_lower_bound_.value() - 1};

  const i64 max_bytes_per_batch_;

  const i64 max_batch_count_;

  ARTBase::Metrics art_metrics_;

  ART<MemTableValueEntry> art_index_;

  batt::CpuCacheLineIsolated<std::atomic<i64>> max_item_size_{Self::kDefaultItemSize};

  std::atomic<i64> max_byte_size_;

  // Set to true if an external alloc ever tiggers overcommit; the MemTable stops accepting new
  // edits when this happens.
  //
  std::atomic<bool> overcommit_triggered_{false};

  batt::CpuCacheLineIsolated<std::atomic<i64>> prepared_bytes_total_{0};

  batt::CpuCacheLineIsolated<std::atomic<i64>> committed_bytes_total_{0};

  absl::Mutex block_list_mutex_;

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

  template <typename SerializeFn>
    requires(std::invocable<SerializeFn, MutableBuffer, EditOffset>)
  Status store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept
  {
    return this->storage_writer_context_.append_slot(
        n_bytes,
        [&](FirstVisitToBlock first_visit,
            StorageBlockBuffer* buffer,
            MutableBuffer dst,
            EditOffset slot_edit_offset) {
          BATT_CHECK_EQ(buffer->slot_count() == 0, first_visit);
          if (first_visit) {
            this->mem_table_.attach_block_buffer(buffer);
          }
          serialize_fn(dst, slot_edit_offset);
        });
  }

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
  BasicMemTable& mem_table_;

  const usize byte_size_limit_;

  u64 batch_count_;

  ARTScanner scanner_;

  //----- --- -- -  -  -   -
  // TODO [tastolfi 2026-04-21] REMOVE!!!
  std::string_view prev_key_;
  //----- --- -- -  -  -   -
};

using MemTable = BasicMemTable<MemTableChangeLogStorage, MemTablePageCacheAllocationTracker>;

}  // namespace turtle_kv
