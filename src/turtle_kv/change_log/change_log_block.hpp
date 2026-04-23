//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_BLOCK_HPP

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/latch.hpp>
#include <batteries/require.hpp>

#include <llfs/ioring_file.hpp>

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

  static constexpr usize kDefaultAlign = 4096;
  static constexpr usize kDefaultSize = 8192;
  static constexpr usize kMinSize = 512;

  /** \brief Magic (random) value used to tag the memory as being a valid ChangeLogBlock object.
   */
  static constexpr u64 kMagic = 0x8d4727d6801bb070ull;

  /** \brief Another magic value used to indicate a ChangeLogBlock object's destructor has been
   * called.
   */
  static constexpr u64 kExpired = 0xfdc038ae91507827ull;

  struct Metrics {
    FastCountMetric<i64> block_alloc_count{0};
    FastCountMetric<i64> block_free_count{0};
  };

  static Metrics& metrics() noexcept
  {
    static Metrics m_;
    return m_;
  }

  /** \brief Internal structure used to delineate chunks of formatted slot data within the buffer.
   */
  struct alignas(2) SlotInfo {
    /** \brief The offset (from `this`, the start of the block buffer) of the start of this slot.
     */
    little_u16 offset;
  };

  static_assert(sizeof(SlotInfo) == 2);
  static_assert(alignof(SlotInfo) == 2);

  class ScopedMemory
  {
   public:
    using Self = ScopedMemory;

    explicit ScopedMemory(void* ptr, usize size) noexcept : buffer_{ptr, size}
    {
    }

    ScopedMemory(const Self&) = delete;
    Self& operator=(const Self&) = delete;

    ScopedMemory(Self&& other) noexcept : buffer_{std::exchange(other.buffer_, {})}
    {
    }

    Self& operator=(Self&& other) noexcept
    {
      if (this != &other) {
        // Free any memory currently owned by *this beore overwriting it.
        //
        if (this->buffer_.data() != nullptr) {
          free(this->buffer_.data());
        }

        this->buffer_ = std::exchange(other.buffer_, {});
      }
      return *this;
    }

    ~ScopedMemory() noexcept
    {
      if (this->buffer_.data() != nullptr) {
        free(this->buffer_.data());
      }
    }

    void* data() const
    {
      return this->buffer_.data();
    }

    usize size() const
    {
      return this->buffer_.size();
    }

    MutableBuffer buffer() const
    {
      return this->buffer_;
    }

    void* release_ownership()
    {
      void* released_ptr = this->buffer_.data();
      this->buffer_ = MutableBuffer{};
      return released_ptr;
    }

   private:
    MutableBuffer buffer_;
  };

  /** \brief ChangeLogBlock objects must be deallocated by calling ChangeLogBlock::remove_ref(); the
   * delete operator is disabled to enforce this.
   */
  void operator delete(void* ptr) noexcept = delete;

  /** \brief Allocates and returns a pointer of the specifed size aligned to
   * ChangeLogBlock::kDefaultAlign bytes.
   */
  static ScopedMemory allocate_aligned(usize n_bytes) noexcept;

  /** \brief Allocates and returns a buffer of the specifed size.
   */
  static ChangeLogBlock* allocate(EditOffset edit_offset_lower_bound,
                                  batt::Grant&& grant,
                                  usize n_bytes) noexcept;

  /** \brief Deallocates the dynamic memory of block.
   */
  static void free_allocated(ChangeLogBlock* block)
  {
    block->~ChangeLogBlock();
    std::free(block);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class RecoveryChecksPassed;

  /** \brief Read a ChangeLogBlock from the ChangeLogFile into the buffer, buf. Returns an error
   * status if malformed or unsuccessful.
   */
  static StatusOr<boost::intrusive_ptr<ChangeLogBlock>> recover(ScopedMemory memory);

  /** \brief Serializes the passed `delta` to the front of `dst`, advancing `dst` beyond the written
   * value.
   * \return OkStatus if dst has enough room
   */
  static Status write_slot_edit_offset_delta(MutableBuffer& dst, SlotEditOffsetDelta delta) noexcept
  {
    BATT_REQUIRE_GE(dst.size(), sizeof(PackedEditOffsetDelta));
    *static_cast<PackedEditOffsetDelta*>(dst.data()) = delta.value();
    dst += sizeof(PackedEditOffsetDelta);
    return OkStatus();
  }

  /** \brief Parses the slot edit offset delta from the beginning of `src`, advancing `src` beyond
   * the parsed value.
   * \return The parsed delta value
   */
  template <typename ConstBufferT>
    requires std::assignable_from<ConstBuffer&, ConstBufferT&&>
  static StatusOr<SlotEditOffsetDelta> read_slot_edit_offset_delta(ConstBufferT&& src) noexcept
  {
    BATT_REQUIRE_GE(src.size(), sizeof(PackedEditOffsetDelta));
    SlotEditOffsetDelta delta{static_cast<const PackedEditOffsetDelta*>(src.data())->value()};
    src += sizeof(PackedEditOffsetDelta);
    return delta;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogBlock(const ChangeLogBlock&) = delete;
  ChangeLogBlock& operator=(const ChangeLogBlock&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the greatest EditOffset value that is not less than the slots in this Block.
   */
  EditOffset edit_offset_lower_bound() const noexcept
  {
    return EditOffset{(i64)this->edit_offset_lower_bound_};
  }

  /** \brief Returns the least EditOffset value that is greater than the slots in this Block.
   */
  EditOffset edit_offset_upper_bound() const noexcept
  {
    const usize slot_count = this->slot_count();

    if (slot_count == 0) {
      return this->edit_offset_lower_bound();
    }

    ConstBuffer last_slot = this->get_slot(slot_count - 1);
    SlotEditOffsetDelta slot_delta =
        BATT_OK_RESULT_OR_PANIC(Self::read_slot_edit_offset_delta(last_slot));

    // Return the EditOffset where the final slot *ends*.
    //
    return this->edit_offset_lower_bound() + slot_delta +
           EditOffsetDelta{static_cast<i64>(last_slot.size())};
  }

  /** \brief Returns the EditOffset of the slot at the specified index `i`.
   */
  EditOffset slot_edit_offset(usize i) const noexcept
  {
    return this->edit_offset_lower_bound() +
           BATT_OK_RESULT_OR_PANIC(Self::read_slot_edit_offset_delta(this->get_slot(i)));
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

  /** \brief Return a referenece to this ChangeLogBlock's underlying grant.
   */
  batt::Grant& get_grant();

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

  /** \brief Returns the next available `n_bytes` bytes.
   */
  MutableBuffer output_buffer(usize n_bytes) noexcept
  {
    BATT_CHECK_EQ(this->xxh3_seed_, 0);

    return MutableBuffer{
        (void*)advance_pointer((void*)this, this->slots_rend()->offset),
        n_bytes,
    };
  }

  /** \brief Returns the part of the buffer that is available for formatting slot data.
   */
  MutableBuffer output_buffer() noexcept
  {
    return this->output_buffer(this->space());
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

  /** \brief Returns the data buffer for the i-th slot; the returned buffer starts with the
   * automatically prepended PackedEditOffsetDelta.
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
  batt::Status verify() const noexcept;

  /** \brief Recomputes the xxh3 hash and verifies that it matches the saved xxh3 hash.
   */
  batt::Status verify_hash() const noexcept;

  /** \brief Checks to make sure all space within the buffer is accounted for.
   */
  void check_buffer_invariant() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief The members of this object which live outside the block buffer.
   */
  struct EphemeralState;

  using EphemeralStatePtr = std::unique_ptr<EphemeralState>;

  using EphemeralStateStorage =
      std::aligned_storage_t<sizeof(EphemeralStatePtr), alignof(EphemeralStatePtr)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new ChangeLogBlock; must only be called from (static)
   * ChangeLogBlock::allocate.
   */
  explicit ChangeLogBlock(EditOffset edit_offset_lower_bound,
                          batt::Grant&& grant,
                          usize block_size) noexcept;

  /** \brief Marks the ChangeLogBlock as expired; the Grant is released.
   */
  ~ChangeLogBlock() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize get_slot_offset(usize slot_i) const
  {
    return (this->slots_rbegin() - slot_i)->offset;
  }

  const void* get_slot_begin(usize slot_i) const
  {
    return advance_pointer((const void*)this, this->get_slot_offset(slot_i));
  }

  SlotInfo* slots_rbegin() noexcept
  {
    return (SlotInfo*)(advance_pointer((void*)this, this->block_size_)) - 1;
  }

  const SlotInfo* slots_rbegin() const noexcept
  {
    return (const SlotInfo*)(advance_pointer((void*)this, this->block_size_)) - 1;
  }

  SlotInfo* slots_rend() noexcept
  {
    return this->slots_rbegin() - this->slot_count_;
  }

  const SlotInfo* slots_rend() const noexcept
  {
    return this->slots_rbegin() - this->slot_count_;
  }

  void set_ref_count(i64 ref_count)
  {
    this->ref_count_.store(ref_count);
  }

  /** \brief Helper function to initialize the ephemeral state of this ChangeLogBlock. Transfers
   * ownership of grant to ChangeLogBlock, and initializes the reference count to ref_count.
   */
  void init_ephemeral_state(batt::Grant&& grant);

  /** \brief Helper function to initialize the ephemeral state of this ChangeLogBlock. Transfers
   * ownership of grant to ChangeLogBlock, and initializes the reference count to ref_count.
   */
  void init_ephemeral_state(RecoveryChecksPassed&& token);

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
  big_u64 magic_;

  /** \brief The id of this block. It is equivalent to the minimum lower bound edit offset of all
   * slots stored by this block.
   */
  little_i64 edit_offset_lower_bound_;

  /** \brief The total size (byte) of the block, including this object.
   */
  little_u16 block_size_;

  /** \brief The number of slots written to this buffer.
   */
  little_u16 slot_count_;

  /** \brief The available free space.
   */
  little_u16 space_;

  // Pad the next field (this->ref_count_) out to (void*) this + 24 bytes;
  //
  u8 padding0_[6];

  /** \brief Atomic reference counter to manage the lifetime of the buffer.
   */
  std::atomic<i32> ref_count_;  // TODO [tastolfi 2025-12-16] move to ephemeral state

  /** \brief The next ChangeLogBlock in the current stack.
   */
  ChangeLogBlock* next_;  // TODO [tastolfi 2025-12-16] move to ephemeral state

  /** \brief The XXH3 hash value of the data contents of this block.  Used during recovery to
   * detect and reject partial flushes.
   */
  little_u64 xxh3_checksum_;

  /** \brief A randomized seed value for the data integrity hash; to protect against collision
   * attacks (XXHash family is non-cryptographic).
   */
  little_u64 xxh3_seed_;

  /** \brief Pointer to state object which only exists while this object is deserialized in memory.
   */
  EphemeralStateStorage ephemeral_state_storage_;

  static_assert(sizeof(EphemeralStateStorage) == 8);
};

static_assert(sizeof(ChangeLogBlock) == 64);

/** \brief Free function necessary for intrusive_ptr usage. Adds a reference to the ChangeLogBlock.
 */
inline void intrusive_ptr_add_ref(ChangeLogBlock* block) noexcept
{
  block->add_ref(1);
}

/** \brief Free function necessary for intrusive_ptr usage. Removes a reference from the
 * ChangeLogBlock.
 */
inline void intrusive_ptr_release(ChangeLogBlock* block) noexcept
{
  block->remove_ref(1);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class ChangeLogBlock::RecoveryChecksPassed
{
  friend /*static*/ StatusOr<boost::intrusive_ptr<ChangeLogBlock>> ChangeLogBlock::recover(
      ScopedMemory memory);

 public:
  RecoveryChecksPassed(const RecoveryChecksPassed&) = delete;
  RecoveryChecksPassed& operator=(const RecoveryChecksPassed&) = delete;

  RecoveryChecksPassed(RecoveryChecksPassed&&) = default;
  RecoveryChecksPassed& operator=(RecoveryChecksPassed&&) = default;

  ~RecoveryChecksPassed() = default;

 private:
  RecoveryChecksPassed() = default;
};

struct ChangeLogBlock::EphemeralState {
  // TODO: [Gabe Bornstein 2/2/26] Consider turning grant and read lock into Variant
  //

  /** \brief The Volume root log Grant passed in at construction time; a pre-reservation of
   * space in the Volume root log for the slot data that will be appended to this buffer.
   */
  std::variant<batt::Grant, RecoveryChecksPassed> token_;

  //----- --- -- -  -  -   -

  explicit EphemeralState(batt::Grant&& grant) noexcept : token_{std::move(grant)}
  {
  }

  explicit EphemeralState(RecoveryChecksPassed&& token) noexcept : token_{std::move(token)}
  {
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline batt::Grant& ChangeLogBlock::get_grant()
{
  BATT_CHECK(batt::is_case<batt::Grant>(this->ephemeral_state().token_));
  return std::get<batt::Grant>(this->ephemeral_state().token_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void ChangeLogBlock::init_ephemeral_state(batt::Grant&& grant)
{
  BATT_CHECK_EQ(grant.size(), 1);
  new (&this->ephemeral_state_storage_) EphemeralStatePtr{new EphemeralState{std::move(grant)}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void ChangeLogBlock::init_ephemeral_state(RecoveryChecksPassed&& token)
{
  new (&this->ephemeral_state_storage_) EphemeralStatePtr{new EphemeralState{std::move(token)}};
}

}  // namespace turtle_kv
