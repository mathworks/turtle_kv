#pragma once

#include <turtle_kv/kv/checkpoint_lock.hpp>
#include <turtle_kv/kv/delta_batch_id.hpp>
#include <turtle_kv/kv/status.hpp>
#include <turtle_kv/kv/tree.hpp>

#include <turtle_kv/util/use_counter.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/slot.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/cancel_token.hpp>

#include <boost/intrusive_ptr.hpp>

namespace turtle_kv {

class DeltaBatch;

class Checkpoint
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<Checkpoint> recover(
      llfs::Volume& checkpoint_volume,
      const llfs::SlotWithPayload<TabletCheckpoint>& checkpoint) noexcept;

  static Checkpoint empty_at_batch(DeltaBatchId batch_id) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Checkpoint() noexcept;

  explicit Checkpoint(
      // The PageId of the tree root.
      //
      Optional<llfs::PageId> root_id,

      // The checkpoint tree.
      //
      std::shared_ptr<const TreeView>&& tree,

      // The upper-bound of the deltas covered by this checkpoint.
      //
      DeltaBatchId batch_upper_bound,

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

  /** \brief Serializes all pages in the Checkpoint to prepare to write it.
   */
  StatusOr<Checkpoint> serialize(const TreeOptions& tree_options,
                                 llfs::PageCacheJob& job,
                                 batt::WorkerPool& worker_pool) const noexcept;

  /** \brief Returns the in-memory view of the checkpoint tree.
   */
  const std::shared_ptr<const TreeView>& tree() const;

  /** \brief The slot upper bound (one past the last byte) of deltas that are included in this
   * Checkpoint.
   */

  DeltaBatchId batch_upper_bound() const noexcept
  {
    return this->batch_upper_bound_;
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

  /** \brief Applys a batch update to this Checkpoint's tree to produce a new Checkpoint.
   */
  StatusOr<Checkpoint> flush_batch(batt::WorkerPool& worker_pool,
                                   llfs::PageCacheJob& job,
                                   const TreeOptions& tree_options,
                                   std::unique_ptr<DeltaBatch>&& batch,
                                   const batt::CancelToken& cancel_token) noexcept;

  /** \brief Returns a copy of this Checkpoint's CheckpointLock.
   */
  CheckpointLock clone_checkpoint_lock() const noexcept
  {
    return batt::make_copy(this->checkpoint_lock_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Optional<llfs::PageId> root_id_;
  std::shared_ptr<const TreeView> tree_;
  DeltaBatchId batch_upper_bound_;

  // Read lock that keeps this Checkpoint from being recycled by the llfs::Volume; the locked slot
  // range is from the start of the PrepareJob slot where the checkpoint tree root was introduced,
  // to the end of the CommitJob slot that finalizes all the pages in the checkpoint.
  //
  CheckpointLock checkpoint_lock_;
};

}  // namespace turtle_kv
