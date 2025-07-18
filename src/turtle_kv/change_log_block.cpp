#include <turtle_kv/change_log_block.hpp>
//

#include <llfs/page_cache_slot.hpp>

#include <xxhash.h>

#include <pcg_random.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ ChangeLogBlock* ChangeLogBlock::allocate(u64 owner_id,
                                                    batt::Grant&& grant,
                                                    usize n_bytes) noexcept
{
  BATT_CHECK_GE(n_bytes, Self::kMinSize);

  void* const memory = std::aligned_alloc(Self::kDefaultAlign, n_bytes);
  BATT_CHECK_NOT_NULLPTR(memory);

  ChangeLogBlock* buffer = new (memory) ChangeLogBlock{owner_id, std::move(grant), n_bytes};

  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogBlock::ChangeLogBlock(u64 owner_id,
                                            batt::Grant&& grant,
                                            usize block_size) noexcept
    : magic_{reinterpret_cast<u64>(this) ^ ChangeLogBlock::kMagic}
    , owner_id_{owner_id}
    , block_size_{BATT_CHECKED_CAST(u16, block_size)}
    , slot_count_{0}
    , space_{BATT_CHECKED_CAST(u16,
                               this->block_size_ - (sizeof(ChangeLogBlock) + sizeof(SlotInfo)))}
    , ref_count_{1}
    , next_{nullptr}
    , xxh3_checksum_{0}
    , xxh3_seed_{0}
{
  new (&this->ephemeral_state_storage_) EphemeralStatePtr{new EphemeralState{std::move(grant)}};

  BATT_CHECK_EQ(this->ephemeral_state().grant_.size(), 1);

  this->slots_rbegin()->offset = sizeof(ChangeLogBlock);

  this->check_buffer_invariant();

  // llfs::PageCacheSlot::Pool::Metrics::instance().admit_byte_count.add(this->block_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogBlock::~ChangeLogBlock() noexcept
{
  this->magic_ = ChangeLogBlock::kExpired;
  this->ephemeral_state_ptr().~EphemeralStatePtr();

  // llfs::PageCacheSlot::Pool::Metrics::instance().evict_byte_count.add(this->block_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::add_ref(i32 count) noexcept
{
  this->ref_count_.fetch_add(count, std::memory_order_relaxed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::remove_ref(i32 count) noexcept
{
  BATT_CHECK_GE(count, 0);

  const i32 old_count = this->ref_count_.fetch_sub(count, std::memory_order_release);
  if (old_count == count) {
    // Load the ref count as a sanity check and with acquire order to complete the fence.
    //
    BATT_CHECK_EQ(0, this->ref_count_.load(std::memory_order_acquire));
    this->~ChangeLogBlock();
    free(this);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::commit_slot(usize n_bytes) noexcept
{
  BATT_CHECK_EQ(this->xxh3_seed_, 0);
  BATT_CHECK_GT(n_bytes, 0);
  BATT_CHECK_LE(n_bytes, this->space());

  // Need to add a new SlotInfo.  One SlotInfo is always pre-allocated at the end of
  // the available buffer, so it is valid to just back up the `slots_rend_` pointer.
  //
  ++this->slot_count_;
  SlotInfo* const slot_info = this->slots_rend();
  slot_info[0].offset = slot_info[1].offset + n_bytes;

  // Restore the invariant that one unused SlotInfo is pre-allocated at the end of
  // `this->available_`.  If there is not enough room, that's fine, we just set
  // available_.size to 0 so no more commits can happen.
  //
  this->space_ -= std::min<u16>(this->space_, n_bytes + sizeof(SlotInfo));

  this->check_buffer_invariant();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer ChangeLogBlock::get_slot(usize i) const noexcept
{
  BATT_CHECK_LT(i, this->slot_count());

  const SlotInfo* p_slot = (this->slots_rbegin() - i);

  return ConstBuffer{
      advance_pointer((const void*)this, p_slot[0].offset),
      static_cast<usize>(p_slot[-1].offset) - static_cast<usize>(p_slot[0].offset),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer ChangeLogBlock::prepare_to_flush() noexcept
{
  thread_local pcg64_unique hash_seed_rng;

  BATT_CHECK_EQ(this->xxh3_seed_, 0);

  do {
    this->xxh3_seed_ = hash_seed_rng();
  } while (this->xxh3_seed_ == 0);

  this->xxh3_checksum_ = XXH3_64bits(this + 1, this->block_size() - sizeof(ChangeLogBlock));

  return ConstBuffer{(const void*)this, this->block_size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Grant ChangeLogBlock::consume_grant() noexcept
{
  return std::move(this->ephemeral_state().grant_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::verify() const noexcept
{
  BATT_CHECK_NE(this->magic_, ChangeLogBlock::kExpired);
  BATT_CHECK_EQ(this->magic_, reinterpret_cast<u64>(this) ^ ChangeLogBlock::kMagic);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::check_buffer_invariant() const noexcept
{
  BATT_CHECK_EQ(this->slot_count_, this->slots_rbegin() - this->slots_rend());

  // TODO [tastolfi 2025-02-22] handle edge cases around full blocks better to make this an _EQ.
  //
  BATT_CHECK_LE(sizeof(Self) + this->slots_total_size() + this->space_ +
                    sizeof(SlotInfo) * (this->slot_count_ + ((this->space_ != 0) ? 1 : 0)),
                this->block_size_)
      << BATT_INSPECT(sizeof(Self)) << BATT_INSPECT(this->slots_total_size())
      << BATT_INSPECT(this->space_) << BATT_INSPECT(sizeof(SlotInfo))
      << BATT_INSPECT(this->slot_count_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::set_read_lock(ChangeLogReadLock&& read_lock) noexcept
{
  this->ephemeral_state().read_lock_.set_value(
      boost::intrusive_ptr<ChangeLogReadLock>{new ChangeLogReadLock{std::move(read_lock)}});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Interval<i64>> ChangeLogBlock::await_flush_begin() noexcept
{
  BATT_ASSIGN_OK_RESULT(boost::intrusive_ptr<ChangeLogReadLock> p_read_lock,
                        this->ephemeral_state().read_lock_.await());

  return p_read_lock->block_range();
}

}  // namespace turtle_kv
