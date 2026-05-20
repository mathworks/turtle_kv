//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_IPP

#include <turtle_kv/mem_table/mem_table.hpp>
//

#include <turtle_kv/import/env.hpp>

#include <turtle_kv/core/packed_sizeof_edit.hpp>

#include <turtle_kv/util/atomic.hpp>

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
/*explicit*/ BasicMemTable<StorageT, AllocationTrackerT>::BasicMemTable(
    AllocationTrackerT& allocation_tracker,
    MemTableMetrics& metrics,
    EditOffset edit_offset_lower_bound,
    usize max_bytes_per_batch,
    usize max_batch_count) noexcept
    : allocation_tracker_{allocation_tracker}
    , metrics_{metrics}
    , edit_offset_lower_bound_{edit_offset_lower_bound}
    , max_bytes_per_batch_{BATT_CHECKED_CAST(i64, max_bytes_per_batch)}
    , max_batch_count_{BATT_CHECKED_CAST(i64, max_batch_count)}
    , art_metrics_{}
    , art_index_{this->art_metrics_}
    , max_byte_size_{this->calculate_max_byte_size()}
    , block_list_mutex_{}
    , block_buffers_{}
{
  BATT_CHECK_LT(this->edit_offset_upper_bound_.load(), this->edit_offset_lower_bound_.value());
  this->metrics_.alloc_count.add(1);
  this->metrics_.count_stats.update(this->metrics_.alloc_count.get() -
                                    this->metrics_.free_count.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
BasicMemTable<StorageT, AllocationTrackerT>::~BasicMemTable() noexcept
{
  // Try to detect double-deletions.
  //
  BATT_CHECK_EQ(this->magic_num_.exchange(Self::kDeadMagicNum), Self::kAliveMagicNum);

  this->metrics_.log_bytes_freed.add(this->block_size_total_);

  for (StorageBlockBuffer* buffer : this->block_buffers_) {
    buffer->remove_ref(1);
  }

  this->metrics_.free_count.add(1);
  this->metrics_.count_stats.update(this->metrics_.alloc_count.get() -
                                    this->metrics_.free_count.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
StatusOr<EditOffset> BasicMemTable<StorageT, AllocationTrackerT>::put(
    StorageWriterContext& storage_writer_context,
    const KeyView& key,
    const ValueView& value) noexcept
{
  // First, make sure the key/value pair will fit in this MemTable (and make sure the MemTable
  // hasn't been finalized).
  //
  const i64 edit_size = PackedSizeOfEdit{}(key.size(), value.size());
  BATT_REQUIRE_OK(this->prepare_edit(edit_size));

  // Now that we have successfully reserved space in the MemTable, we *must* increase
  // this->committed_bytes_total_ by the same amount after we are done modifying the log/index;
  // otherwise whoever calls this->finalize() will have no way of knowing that all writers are done.
  //
  auto on_scope_exit = batt::finally([this, edit_size] {
    this->commit_edit(edit_size);
  });

  PerOpStorageContext op_storage_context{*this, storage_writer_context};
  {
    MemTableValueEntryInserter<PerOpStorageContext> inserter{
        op_storage_context,
        key,
        value,
    };

    BATT_REQUIRE_OK(this->art_index_.insert(key, inserter));
    BATT_CHECK_NOT_NULLPTR(inserter.entry_out);

    return inserter.entry_out->edit_offset() +
           EditOffsetDelta{(i64)packed_key_value_slot_size(key, value)};
  }

  return OkStatus();
  //
  // ~on_scope_exit calls commit_edit.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
StatusOr<i64> BasicMemTable<StorageT, AllocationTrackerT>::prepare_edit(i64 packed_edit_size)
{
  // Update the maximum item size.  We track this so that we can make a conservative estimate of how
  // much space might be wasted per batch, once the MemTable is finalized/compacted.
  //
  if (packed_edit_size > atomic_clamp_min(*this->max_item_size_, packed_edit_size)) {
    //
    // If we're in here, it's because `packed_edit_size` was larger than the most recently observed
    // value of `this->max_item_size_`; larger max item size means more bytes per batch might be
    // wasted in the worst case, which means we must re-calculate the maximum byte size for the
    // MemTable, since it might have just gone down.
    //
    atomic_clamp_max(this->max_byte_size_, this->calculate_max_byte_size());
  }

  //----- --- -- -  -  -   -
  // Try to reserve `packed_edit_size` bytes in the MemTable.
  //
  const i64 prior_value = this->prepared_bytes_total_->fetch_add(packed_edit_size);

  // If the new value of prepared_bytes_total_ is under the limit, then the edit can be accepted. If
  // the MemTable has been finalized, then `prior_value` with have the 2nd-most-significant bit set
  // (see kFinalizedMask), so this check will certainly fail.
  //
  if (prior_value + packed_edit_size <= this->max_byte_size_.load() &&
      this->overcommit_triggered_.load() == false) {
    BATT_CHECK_EQ((prior_value & Self::kFinalizedMask), 0);
    return {prior_value};
  }

  //----- --- -- -  -  -   -
  // The prepare did not succeed; try to revert the prepare so a smaller edit might succeed.
  //
  const i64 observed_value = this->prepared_bytes_total_->fetch_sub(packed_edit_size);

  // If observed_value has the finalized bit set, then we need to update committed_bytes_total_,
  // since there may be a thread blocked inside MemTable::finalize() waiting on
  // committed_bytes_total_.  First re-apply the prepare, then commit.
  //
  if ((observed_value & Self::kFinalizedMask) != 0) {
    this->prepared_bytes_total_->fetch_add(packed_edit_size);
    this->committed_bytes_total_->fetch_add(packed_edit_size);
    this->committed_bytes_total_->notify_all();
  }

  return {batt::StatusCode::kResourceExhausted};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
void BasicMemTable<StorageT, AllocationTrackerT>::commit_edit(i64 packed_edit_size)
{
  const i64 prior_value = this->committed_bytes_total_->fetch_add(packed_edit_size);
  if ((prior_value & Self::kFinalizedMask) != 0) {
    this->committed_bytes_total_->notify_all();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
Optional<ValueView> BasicMemTable<StorageT, AllocationTrackerT>::get(const KeyView& key) noexcept
{
  Optional<MemTableValueEntry> entry = this->art_index_.find(key);
  if (!entry) {
    return None;
  }

  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
Optional<ValueView> BasicMemTable<StorageT, AllocationTrackerT>::finalized_get(
    const KeyView& key) noexcept
{
  const MemTableValueEntry* entry = this->art_index_.unsynchronized_find(key);
  if (!entry) {
    return None;
  }
  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
bool BasicMemTable<StorageT, AllocationTrackerT>::finalize(
    const SmallFn<EditOffset()>& get_next_edit_offset) noexcept
{
  // Update committed_bytes_total first, so that if prepared_bytes_total_ is observed to have the
  // kFinalizedMask bit set, we can assume committed_bytes_total_ does too.
  //
  const i64 prior_committed = this->committed_bytes_total_->fetch_or(Self::kFinalizedMask);
  const i64 prior_prepared = this->prepared_bytes_total_->fetch_or(Self::kFinalizedMask);

  const bool newly_finalized = (prior_committed & Self::kFinalizedMask) == 0;

  i64 observed_prepared = prior_prepared;
  i64 observed_committed = prior_committed;

  while (observed_committed < observed_prepared) {
    BATT_CHECK_LE(observed_committed, observed_prepared)
        << "MemTable::committed_bytes_total_ should never be greater than "
           "MemTable::prepared_bytes_total_!";

    this->committed_bytes_total_->wait(observed_committed);

    observed_prepared = this->prepared_bytes_total_->load();
    observed_committed = this->committed_bytes_total_->load();
  }

  // If this is the first thread to call finalize, then we must set the upper bound.
  //
  if (newly_finalized) {
    const EditOffset finalized_upper_bound = get_next_edit_offset();
    BATT_CHECK_GE(finalized_upper_bound, this->edit_offset_lower_bound_);
    this->edit_offset_upper_bound_.store(finalized_upper_bound.value());
    this->edit_offset_upper_bound_.notify_all();

    // Update metrics.
    //
    const EditOffsetDelta finalized_size =
        this->edit_offset_upper_bound() - this->edit_offset_lower_bound();

    this->metrics_.finalize_size_stats.update(finalized_size.value());

    if (this->storage_is_full_.load()) {
      this->metrics_.storage_full_count.add(1);
      this->metrics_.storage_full_size_stats.update(finalized_size.value());
    }

  } else {
    // For all other threads that find their way in here, wait until the first has set the true
    // value of edit_offset_upper_bound_.
    //
    this->await_finalize();
  }

  return newly_finalized;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
void BasicMemTable<StorageT, AllocationTrackerT>::await_finalize() noexcept
{
  for (;;) {
    const EditOffset observed_upper_bound{this->edit_offset_upper_bound_.load()};
    if (observed_upper_bound >= this->edit_offset_lower_bound_) {
      break;
    }
    this->edit_offset_upper_bound_.wait(observed_upper_bound.value());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
bool BasicMemTable<StorageT, AllocationTrackerT>::is_finalized() const
{
  return (this->committed_bytes_total_->load() & Self::kFinalizedMask) != 0 &&
         this->edit_offset_lower_bound_ <= EditOffset{this->edit_offset_upper_bound_.load()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
Status BasicMemTable<StorageT, AllocationTrackerT>::put_recovered_slot(
    FirstVisitToBlock first_visit,
    StorageBlockBuffer* block_buffer,
    EditOffset edit_offset,
    const KeyView& key,
    const ValueView& value)
{
  // As in normal operation, check to make sure the edit doesn't overflow the MemTable.
  // (Since we aren't writing any new data, however, we don't need to save the total bytes prepared
  // before--the "ok" return value of prepare_edit--since that is only needed to calculate when to
  // rebase.)
  //
  const i64 edit_size = PackedSizeOfEdit{}(key.size(), value.size());
  BATT_REQUIRE_OK(this->prepare_edit(edit_size));

  // What's prepared must be committed.
  //
  auto on_scope_exit = batt::finally([this, edit_size] {
    this->commit_edit(edit_size);
  });

  if (first_visit) {
    this->attach_block_buffer(block_buffer, LockIsHeld{false});
  }

  MemTableRecoveryInserter inserter{
      edit_offset,
      key,
      value,
  };

  BATT_REQUIRE_OK(this->art_index_.insert(key, inserter));

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
void BasicMemTable<StorageT, AllocationTrackerT>::attach_block_buffer(
    StorageBlockBuffer* block_buffer,
    LockIsHeld lock_is_held)
{
  block_buffer->add_ref(1);
  this->metrics_.log_bytes_allocated.add(block_buffer->block_size());
  i64 cache_alloc_delta = 0;
  {
    Optional<absl::MutexLock> lock;
    if (!lock_is_held) {
      lock.emplace(&this->block_list_mutex_);
    }

    this->block_size_total_ += block_buffer->block_size();
    this->block_buffers_.emplace_back(block_buffer);

    cache_alloc_delta = this->update_external_cache_alloc();
  }
  this->handle_external_cache_alloc(cache_alloc_delta);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
i64 BasicMemTable<StorageT, AllocationTrackerT>::update_external_cache_alloc()
{
  // The number of bytes to claim (if positive) or release (negative) as external allocation
  // from the PageCache; the return value of this function.
  //
  i64 cache_alloc_delta = 0;

  ++this->since_last_cache_alloc_update_;

  if (!this->cache_alloc_in_progress_ &&
      this->since_last_cache_alloc_update_ >=
          BasicMemTable<StorageT, AllocationTrackerT>::kBlocksPerExternalCacheAllocUpdate) {
    this->cache_alloc_in_progress_ = true;
    this->since_last_cache_alloc_update_ = 0;

    if (getenv_param<turtlekv_memtable_cache_alloc_log>()) {
      BATT_CHECK_GE(this->block_size_total_, this->block_size_last_update_);
      const i64 block_delta = this->block_size_total_ - this->block_size_last_update_;
      cache_alloc_delta += block_delta;
      this->block_size_last_update_ = this->block_size_total_;
    }

    if (getenv_param<turtlekv_memtable_cache_alloc_art>()) {
      const i64 new_art_size = (i64)this->art_metrics_.bytes_in_use();
      const i64 art_delta = new_art_size - this->art_reserved_size_;
      cache_alloc_delta += art_delta;
      this->art_reserved_size_ = new_art_size;
    }

    if (cache_alloc_delta == 0) {
      this->cache_alloc_in_progress_ = false;
    }
  }

  return cache_alloc_delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
void BasicMemTable<StorageT, AllocationTrackerT>::handle_external_cache_alloc(i64 cache_alloc_delta)
{
  if (cache_alloc_delta > 0) {
    const i64 observed_current_size = this->prepared_bytes_total_->load();

    llfs::PageCacheOvercommit overcommit;
    overcommit.allow(true);

    typename AllocationTracker::ExternalAllocation alloc =
        this->allocation_tracker_.allocate_external(cache_alloc_delta, overcommit);

    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      this->total_cache_alloc_.subsume(std::move(alloc));
      this->cache_alloc_in_progress_ = false;
    }

    // If we trigger cache overcommit, then cut the size limit down so we don't make things too
    // much worse.
    //
    if (overcommit.is_triggered()) {
      this->overcommit_triggered_.store(true);

      this->allocation_tracker_.on_overcommit([this, observed_current_size](std::ostream& out) {
        out << "Truncating MemTable size due to cache overcommit;"
            << BATT_INSPECT(this->prepared_bytes_total_) << BATT_INSPECT(this->max_bytes_per_batch_)
            << BATT_INSPECT(observed_current_size);
      });
    }

  } else if (cache_alloc_delta < 0) {
    StatusOr<typename AllocationTracker::ExternalAllocation> alloc_to_release;
    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      alloc_to_release = this->total_cache_alloc_.split(-cache_alloc_delta);
      this->cache_alloc_in_progress_ = false;
    }
    BATT_CHECK_OK(alloc_to_release)
        << BATT_INSPECT(cache_alloc_delta) << BATT_INSPECT(this->total_cache_alloc_.size());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
i64 BasicMemTable<StorageT, AllocationTrackerT>::calculate_max_byte_size() const
{
  const i64 max_wasted_per_batch = this->max_item_size_->load() - 1;
  const i64 min_full_batch_size = this->max_bytes_per_batch_ - max_wasted_per_batch;
  return this->max_batch_count_ * min_full_batch_size;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class BasicMemTable<StorageT, AllocationTrackerT>::BatchCompactor

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
/*explicit*/ BasicMemTable<StorageT, AllocationTrackerT>::BatchCompactor::BatchCompactor(
    BasicMemTable& mem_table,
    usize byte_size_limit) noexcept
    : mem_table_{mem_table}
    , byte_size_limit_{byte_size_limit}
    , batch_count_{0}
    , scanner_{this->mem_table_.art_index_,
               /*min_key=*/std::string_view{}}
{
  BATT_CHECK(this->mem_table_.is_finalized());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
bool BasicMemTable<StorageT, AllocationTrackerT>::BatchCompactor::has_next() const
{
  return !this->scanner_.is_done();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <MemTableStorage StorageT, MemTableAllocationTracker AllocationTrackerT>
MergeCompactor::ResultSet</*decay_to_items=*/false>
BasicMemTable<StorageT, AllocationTrackerT>::BatchCompactor::consume_next() noexcept
{
  MergeCompactor::ResultSet</*decay_to_items=*/false> compacted_edits;

  compacted_edits.append([&]() -> std::vector<EditView> {
    std::vector<EditView> edits_out;
    //----- --- -- -  -  -   -
    PackedSizeOfEdit packed_size_of;
    usize total_size = 0;

    for (; !this->scanner_.is_done(); this->scanner_.advance()) {
      const MemTableValueEntry& entry = this->scanner_.get_value();

      EditView edit{entry.key_view(), entry.value_view()};
      total_size += packed_size_of(edit);

      if (total_size < this->byte_size_limit_) {
        edits_out.emplace_back(edit);
      } else {
        break;
      }
    }
    //----- --- -- -  -  -   -
    return edits_out;
  }());

  if (!compacted_edits.empty()) {
    ++this->batch_count_;
  }

  return compacted_edits;
}

}  // namespace turtle_kv
