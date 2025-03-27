#pragma once

#include <turtle_kv/change_log_read_lock.hpp>
#include <turtle_kv/delta_batch.hpp>

#include <turtle_kv/import/optional.hpp>

#include <llfs/slot_read_lock.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/shared_ptr.hpp>

#include <variant>

namespace turtle_kv {

class DeltaBatch;

class CheckpointLock
{
 public:
  class Impl : public batt::RefCounted<Impl>
  {
   public:
    struct EmptyState {
    };

    // While the checkpoint is only speculatively committed, we must lock it against trimming by
    // locking the delta batch slots it rolls up plus the previous checkpoint upon which it is
    // based.
    //
    struct SpeculativeState {
      std::unique_ptr<DeltaBatch> delta_batch;
      batt::SharedPtr<Impl> base_checkpoint_lock;
    };

    // Once the checkpoint is durably committed, we need only lock the WAL slot range where it was
    // appended.
    //
    struct DurableState {
      llfs::SlotReadLock checkpoint_slot_lock;
    };

    // Reflects the current state of the checkpoint.
    //
    batt::Mutex<std::variant<EmptyState, SpeculativeState, DurableState>> state;

    /** \brief Used to signal that the checkpoint is durable.
     */
    batt::Watch<bool> is_durable{false};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static CheckpointLock make_speculative(std::unique_ptr<DeltaBatch>&& delta_batch,
                                         CheckpointLock&& base_checkpoint_lock);

  static CheckpointLock make_durable(llfs::SlotReadLock&& checkpoint_slot_lock);

  static CheckpointLock make_durable_detached();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff the checkpoint is in a durable state.
   */
  bool is_durable() const;

  /** \brief Called once to notify this lock that the Checkpoint it protects has been durably
   * committed; this releases the backward chain of locks from this point.
   *
   * Returns true if the state changed, false if it was already durable.
   */
  bool notify_durable(llfs::SlotReadLock&& checkpoint_slot_lock);

  /** \brief Blocks the caller until the state of the checkpoint changes to Durable.
   */
  batt::Status await_durable();

  /** \brief If the checkpoint is durable, returns the checkpoint volume slot range where it was
   * appended; otherwise returns None.
   */
  Optional<llfs::SlotRange> slot_range() const;

  /** \brief Returns true iff this lock is actively protecting some checkpoint.
   */
  explicit operator bool() const
  {
    return this->impl_ != nullptr;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  CheckpointLock() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::SharedPtr<Impl> impl_ = nullptr;
};

}  // namespace turtle_kv
