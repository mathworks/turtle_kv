//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHECKPOINT_HPP

#include <turtle_kv/checkpoint_lock.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/packed_checkpoint.hpp>

#include <turtle_kv/tree/filter_page_write_state.hpp>
#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/util/use_counter.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/slot.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/case_of.hpp>

#include <boost/intrusive_ptr.hpp>

namespace turtle_kv {

class DeltaBatch;

class Checkpoint
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a valid, in-memory Checkpoint from the given packed checkpoint data. Requires
   * that a valid PackedCheckpoint, and a valid Slot Range for the checkpoint is passed.
   */
  static StatusOr<Checkpoint> recover(llfs::Volume& checkpoint_volume,
                                      llfs::SlotParse& slot,
                                      const PackedCheckpoint& checkpoint) noexcept;

  /** \brief Returns the canonical, empty initial Checkpoint with invalid llfs::PageId (indicating
   * there is no root page) and EditOffset upper bound of 0.
   */
  static Checkpoint make_empty() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Checkpoint() noexcept;

  explicit Checkpoint(
      // The PageId of the tree root.
      //
      Optional<llfs::PageId> root_id,

      // The checkpoint tree.
      //
      std::shared_ptr<Subtree>&& tree,

      // The height of the tree.
      //
      i32 tree_height,

      // The upper-bound of the deltas covered by this checkpoint.
      //
      std::variant<EditOffset, DeltaBatchId> checkpoint_upper_bound,

      // WAL lock covering the slot range of the checkpoint.
      //
      CheckpointLock&& checkpoint_lock) noexcept;

  Checkpoint(const Checkpoint&) = default;
  Checkpoint& operator=(const Checkpoint&) = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the PageId of the root of this Checkpoint's tree.
   */
  llfs::PageId root_id() const;

  Optional<llfs::PageId> maybe_root_id() const noexcept
  {
    return this->root_id_;
  }

  /** \brief Returns the in-memory view of the checkpoint tree.
   */
  const std::shared_ptr<Subtree>& tree() const
  {
    return this->tree_;
  }

  /** \brief Returns the height of the tree.
   */
  i32 tree_height() const
  {
    return this->tree_height_;
  }

  /** \brief Returns true iff this is an empty Checkpoint.
   */
  bool is_empty() const noexcept
  {
    return this->maybe_root_id() == Optional<llfs::PageId>(llfs::kInvalidPageId) &&
           this->edit_offset_upper_bound() == EditOffset{0};
  }

  /** \brief The slot upper bound (one past the last byte) of deltas that are included in this
   * Checkpoint.
   */
  EditOffset edit_offset_upper_bound() const noexcept
  {
    return batt::case_of(
        this->checkpoint_upper_bound_,
        [](const EditOffset& edit_offset) -> EditOffset {
          return edit_offset;
        },
        [](const DeltaBatchId& batch_id) -> EditOffset {
          return batch_id.edit_offset_upper_bound();
        });
  }

  /** \brief The slot offset range at which this Checkpoint was committed in the Checkpoint Log.
   *
   * If `this->notify_durable` returns true, then `this->slot_range()` goes from returning `None` to
   * the slot range of the lock passed to `notify_durable`.
   *
   * \return None if not yet durable; the slot range otherwise
   */
  Optional<llfs::SlotRange> slot_range() const;

  /** \brief Sets the state of this checkpoint to durably committed; this->slot_range() is updated
   * with the passed slot range.
   *
   * \return `true` if this checkpoint was not previously in the durably committed state; `false` if
   * it was (in which case, no change is made to the held slot lock or `this->slot_range()`)
   */
  bool notify_durable(llfs::SlotReadLock&& slot_read_lock);

  /** \brief Blocks the caller until the checkpoint is durable.
   */
  Status await_durable();

  /** \brief Returns true iff this checkpoint is known to be durable.
   */
  bool is_durable() const noexcept;

  /** \brief Performs various sanity checks to make sure batches are in-order and gapless.
   */
  Status validate_next_batch_id(const DeltaBatchId& batch_id) const noexcept;

  /** \brief Applies a batch update to this Checkpoint's tree to produce a new Checkpoint.
   */
  StatusOr<Checkpoint> apply_batch(batt::WorkerPool& worker_pool,
                                   llfs::PageCacheJob& job,
                                   const TreeOptions& tree_options,
                                   BatchUpdateMetrics& metrics,
                                   llfs::PageCacheOvercommit& overcommit,
                                   std::unique_ptr<DeltaBatch>&& batch,
                                   const batt::CancelToken& cancel_token) noexcept;

  /** \brief Performs sanity checks to validate that it is OK to call serialize.
   */
  Status validate_ready_to_serialize() const noexcept;

  /** \brief Serializes all pages in the Checkpoint to prepare to write it.
   */
  StatusOr<Checkpoint> serialize(
      const TreeOptions& tree_options,
      llfs::PageCacheJob& job,
      llfs::PageCacheOvercommit& overcommit,
      batt::WorkerPool& worker_pool,
      const boost::intrusive_ptr<FilterPageWriteState>& filter_page_write_state) const noexcept;

  /** \brief Returns a copy of this Checkpoint's CheckpointLock.
   */
  CheckpointLock clone_checkpoint_lock() const noexcept
  {
    return batt::make_copy(this->checkpoint_lock_);
  }

  Checkpoint clone() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<ValueView> find_key(KeyQuery& query) const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The id of the root page for the starting point tree.
  //
  Optional<llfs::PageId> root_id_;

  // The current cached/mutable tree.
  //
  std::shared_ptr<Subtree> tree_;

  // 0 means empty; 1 means only a leaf; etc.
  //
  i32 tree_height_;

  // When the Checkpoint is "clean" (no batches applied after the last serialize), holds the edit
  // offset upper bound for the checkpoint; otherwise, holds the most recent delta batch id applied.
  //
  std::variant<EditOffset, DeltaBatchId> checkpoint_upper_bound_;

  // Read lock that keeps this Checkpoint from being recycled by the llfs::Volume; the locked slot
  // range is from the start of the PrepareJob slot where the checkpoint tree root was introduced,
  // to the end of the CommitJob slot that finalizes all the pages in the checkpoint.
  //
  CheckpointLock checkpoint_lock_;
};

}  // namespace turtle_kv
