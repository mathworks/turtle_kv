#pragma once

#include <turtle_kv/change_log_read_lock.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/latch.hpp>

#include <boost/intrusive_ptr.hpp>

namespace turtle_kv {

/** \brief A per-thread/task buffer that receives formatted slot data.
 *
 * Instances of Buffer are created via placement-new at the beginning of aligned memory regions
 * that include the actual buffer memory.  Buffer objects can be linked together to form a list
 * via the `next_` pointer.  IMPORTANT: changes to a Buffer linked-list are not thread-safe; one
 * must "claim" exclusive access to a "stack" of Buffers before making changes.
 */
class ChangeLogBlock
{
 public:
  using Self = ChangeLogBlock;

  static constexpr usize kDefaultAlign = 512;
  static constexpr usize kDefaultSize = 8192;
  static constexpr usize kMinSize = 512;

  /** \brief Magic (random) value used to tag the memory as being a valid ChangeLogBlock object.
   */
  static constexpr u64 kMagic = 0x8d4727d6801bb070ull;

  /** \brief Another magic value used to indicate a ChangeLogBlock object's destructor has been
   * called.
   */
  static constexpr u64 kExpired = 0xfdc038ae91507827ull;

  /** \brief Internal structure used to delineate chunks of formatted slot data within the buffer.
   */
  struct SlotInfo {
    /** \brief The offset (from `this`, the start of the block buffer) of the start of this slot.
     */
    u16 offset;
  };

  /** \brief ChangeLogBlock objects must be deallocated by calling ChangeLogBlock::remove_ref(); the
   * delete operator is disabled to enforce this.
   */
  void operator delete(void* ptr) noexcept = delete;

  /** \brief Allocates and returns a buffer of the specifed size.
   */
  static ChangeLogBlock* allocate(u64 owner_id, batt::Grant&& grant, usize n_bytes) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogBlock(const ChangeLogBlock&) = delete;
  ChangeLogBlock& operator=(const ChangeLogBlock&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 owner_id() const noexcept
  {
    return this->owner_id_;
  }

  /** \brief Adds `count` references to this buffer.
   */
  void add_ref(i32 count) noexcept;

  /** \brief Removes `count` references from this buffer, possibly freeing it.
   */
  void remove_ref(i32 count) noexcept;

  i32 ref_count() const noexcept
  {
    return this->ref_count_;
  }

  usize slot_count() const noexcept
  {
    return this->slot_count_;
  }

  /** \brief Return the total block size (including the ChangeLogBlock at the front).
   */
  usize block_size() const noexcept
  {
    return this->block_size_;
  }

  /** \brief Returns the number of committed bytes in the buffer.
   */
  usize slots_total_size() const noexcept
  {
    return this->slots_rend()->offset - sizeof(ChangeLogBlock);
  }

  /** \brief The number of bytes remaining in the block for new slots.
   */
  usize space() const noexcept
  {
    return this->space_;
  }

  /** \brief Returns the part of the buffer that is available for formatting slot data.
   */
  MutableBuffer output_buffer() noexcept
  {
    BATT_CHECK_EQ(this->xxh3_seed_, 0);

    return MutableBuffer{
        (void*)advance_pointer((void*)this, this->slots_rend()->offset),
        this->space(),
    };
  }

  /** \brief Finalize the buffer and return a ConstBuffer that can be written to storage.
   */
  ConstBuffer prepare_to_flush() noexcept;

  /** \brief Moves the next `n_bytes` bytes from the beginning of the "available" region to the
   * end of the "ready" region, and assigns the given index (sequence number or logical timestamp)
   * to the data.
   */
  void commit_slot(usize n_bytes) noexcept;

  /** \brief Returns the next ChangeLogBlock in the stack/linked-list (if any).
   */
  ChangeLogBlock* get_next() const noexcept
  {
    return this->next_;
  }

  /** \brief Sets the next pointer of this ChangeLogBlock to `new_next`.
   * WARNING: not thead-safe!
   */
  void set_next(ChangeLogBlock* new_next) noexcept
  {
    this->next_ = new_next;
  }

  /** \brief Returns the data buffer for the i-th slot.
   */
  ConstBuffer get_slot(usize i) const noexcept;

  /** \brief Sets this ChangeLogBlock's next pointer to `new_next` and returns the previous value.
   * WARNING: not thead-safe!
   */
  ChangeLogBlock* swap_next(ChangeLogBlock* new_next) noexcept
  {
    std::swap(new_next, this->next_);
    return new_next;
  }

  /** \brief Releases all Grant held by this ChangeLogBlock.  Exactly enough Grant to cover the
   * _current_ ready region is returned; the rest is released to the Grant::Issuer pool.
   */
  batt::Grant consume_grant() noexcept;

  /** \brief Perform basic sanity checks to make sure this is a valid ChangeLogBlock object.
   */
  void verify() const noexcept;

  /** \brief Checks to make sure all space within the buffer is accounted for.
   */
  void check_buffer_invariant() const noexcept;

  /** \brief Sets the read lock for this block, indicating that its position in the change log file
   * has been finalized.
   */
  void set_read_lock(ChangeLogReadLock&& read_lock) noexcept;

  StatusOr<Interval<i64>> await_flush_begin() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief The members of this object which live outside the block buffer.
   */
  struct EphemeralState {
    /** \brief Used to track whether this block has been flushed.
     */
    batt::Latch<boost::intrusive_ptr<ChangeLogReadLock>> read_lock_;

    /** \brief The Volume root log Grant passed in at construction time; a pre-reservation of
     * space in the Volume root log for the slot data that will be appended to this buffer.
     */
    batt::Grant grant_;

    //----- --- -- -  -  -   -

    explicit EphemeralState(batt::Grant&& grant) noexcept : read_lock_{}, grant_{std::move(grant)}
    {
    }
  };

  using EphemeralStatePtr = std::unique_ptr<EphemeralState>;

  using EphemeralStateStorage =
      std::aligned_storage_t<sizeof(EphemeralStatePtr), alignof(EphemeralStatePtr)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new ChangeLogBlock; must only be called from (static)
   * ChangeLogBlock::allocate.
   */
  explicit ChangeLogBlock(u64 owner_id, batt::Grant&& grant, usize block_size) noexcept;

  /** \brief Marks the ChangeLogBlock as expired; the Grant is released.
   */
  ~ChangeLogBlock() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SlotInfo* slots_rbegin() noexcept
  {
    return (SlotInfo*)(advance_pointer((void*)this, this->block_size_)) - 1;
  }

  const SlotInfo* slots_rbegin() const noexcept
  {
    return (SlotInfo*)(advance_pointer((void*)this, this->block_size_)) - 1;
  }

  SlotInfo* slots_rend() noexcept
  {
    return this->slots_rbegin() - this->slot_count_;
  }

  const SlotInfo* slots_rend() const noexcept
  {
    return this->slots_rbegin() - this->slot_count_;
  }

  EphemeralStatePtr& ephemeral_state_ptr() noexcept
  {
    return reinterpret_cast<EphemeralStatePtr&>(this->ephemeral_state_storage_);
  }

  EphemeralState& ephemeral_state() noexcept
  {
    return *this->ephemeral_state_ptr();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Initialized to (int)this XOR kMagic while this object is valid; set to kExpired when
   * it is destructed.
   */
  u64 magic_;

  /** \brief The id of the MemTable that owns this block.
   */
  u64 owner_id_;

  /** \brief The total size of the block, including this object.
   */
  u16 block_size_;

  /** \brief The number of slots written to this buffer.
   */
  u16 slot_count_;

  /** \brief The available free space.
   */
  u16 space_;

  // Pad the next field (this->ref_count_) out to (void*) this + 24 bytes;
  //
  u8 padding0_[2];

  /** \brief Atomic reference counter to manage the lifetime of the buffer.
   */
  std::atomic<i32> ref_count_;

  // Pad the next field (this->next_) out to (void*) this + 32 bytes;
  //
  u8 padding1_[4];

  /** \brief The next ChangeLogBlock in the current stack.
   */
  ChangeLogBlock* next_;

  /** \brief The XXH3 hash value of the data contents of this block.  Used during recovery to
   * detect and reject partial flushes.
   */
  u64 xxh3_checksum_;

  /** \brief A randomized seed value for the data integrity hash; to protect against collision
   * attacks (XXHash family is non-cryptographic).
   */
  u64 xxh3_seed_;

  EphemeralStateStorage ephemeral_state_storage_;
};

namespace {

BATT_STATIC_ASSERT_EQ(sizeof(ChangeLogBlock), 64);

}

}  // namespace turtle_kv
