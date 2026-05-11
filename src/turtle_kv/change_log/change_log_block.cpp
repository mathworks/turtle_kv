#include <turtle_kv/change_log/change_log_block.hpp>
//

#include <llfs/page_cache_slot.hpp>

#include <batteries/require.hpp>

#include <xxhash.h>

#include <pcg_random.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ ChangeLogBlock::ScopedMemory ChangeLogBlock::allocate_aligned(usize n_bytes) noexcept
{
  BATT_CHECK_GE(n_bytes, Self::kMinSize);

  // Limit the alignment to at most `n_bytes` (and make sure it is a power of 2, if we reduce it)
  //
  const usize align_size =
      std::min<usize>(usize{1} << batt::log2_ceil(n_bytes), Self::kDefaultAlign);

  // Round up to the nearest multiple of `align_size`.
  //
  const usize align_mask = align_size - 1;
  n_bytes = (n_bytes + align_mask) & ~align_mask;

  void* const memory = std::aligned_alloc(align_size, n_bytes);

  BATT_CHECK_NOT_NULLPTR(memory);

  return ChangeLogBlock::ScopedMemory{memory, n_bytes};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ ChangeLogBlock* ChangeLogBlock::allocate(batt::Grant&& grant, usize n_bytes) noexcept
{
  Self::metrics().block_alloc_count.add(1);

  ChangeLogBlock::ScopedMemory memory = ChangeLogBlock::allocate_aligned(n_bytes);

  void* data = memory.release_ownership();

  ChangeLogBlock* buffer = new (data) ChangeLogBlock{std::move(grant), n_bytes};

  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<boost::intrusive_ptr<ChangeLogBlock>> ChangeLogBlock::recover(
    ChangeLogBlock::ScopedMemory memory,
    BlockIndex block_index)
{
  ChangeLogBlock* block = reinterpret_cast<ChangeLogBlock*>(memory.data());

  // Need to check if block_size is zero. It indicates we have read an unitialized block.
  //
  if (block->block_size() == 0) {
    return {batt::StatusCode::kOutOfRange};
  }

  if (block->block_size() != memory.size()) {
    return {batt::StatusCode::kDataLoss};
  }

  batt::Status verify_status = block->verify();

  if (!verify_status.ok()) {
    return {batt::StatusCode::kDataLoss};
  }

  batt::Status hash_status = block->verify_hash();

  if (!hash_status.ok()) {
    return {batt::StatusCode::kDataLoss};
  }

  block->init_ephemeral_state(RecoveryChecksPassed{}, block_index);

  // ref_count is 2 after reading from the change log. We want to initialize it to 1.
  //
  block->set_ref_count(1);

  Self::metrics().block_alloc_count.add(1);

  return boost::intrusive_ptr<ChangeLogBlock>{
      reinterpret_cast<ChangeLogBlock*>(memory.release_ownership()),
      false};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogBlock::ChangeLogBlock(batt::Grant&& grant, usize block_size) noexcept
    : magic_{ChangeLogBlock::kMagic}
    , edit_offset_lower_bound_{0}
    , block_size_{BATT_CHECKED_CAST(u16, block_size)}
    , slot_count_{0}
    , space_{ChangeLogBlock::initial_space(this->block_size_)}
    , ref_count_{1}
    , next_{nullptr}
    , xxh3_checksum_{0}
    , xxh3_seed_{0}
{
  this->init_ephemeral_state(std::move(grant));

  this->slots_rbegin()->offset = sizeof(ChangeLogBlock);

  this->check_buffer_invariant();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogBlock::~ChangeLogBlock() noexcept
{
  this->magic_ = ChangeLogBlock::kExpired;
  this->ephemeral_state_ptr().~EphemeralStatePtr();

  Self::metrics().block_free_count.add(1);
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

    //----- --- -- -  -  -   -
    ChangeLogBlock::free_allocated(this);
    //----- --- -- -  -  -   -
  }
  BATT_CHECK_NE(old_count, 0);
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
usize ChangeLogBlock::slot_size(usize i) const noexcept
{
  const SlotInfo* p_slot = (this->slots_rbegin() - i);
  return static_cast<usize>(p_slot[-1].offset) - static_cast<usize>(p_slot[0].offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<BlockIndex> ChangeLogBlock::get_block_index()
{
  return this->ephemeral_state().block_index_;
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

  BATT_CHECK_LE(this->get_slot_begin(this->slot_count()), (const void*)this->slots_rend())
      << "The last slot must end before the start of the SlotInfo array!";

  return ConstBuffer{(const void*)this, this->block_size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Grant ChangeLogBlock::consume_grant(Optional<BlockIndex> block_index) noexcept
{
  BATT_CHECK(batt::is_case<batt::Grant>(this->ephemeral_state().token_));
  this->ephemeral_state().block_index_ = block_index;
  return std::move(std::get<batt::Grant>(this->ephemeral_state().token_));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status ChangeLogBlock::verify() const noexcept
{
  static_assert(ChangeLogBlock::kMagic != ChangeLogBlock::kExpired);
  BATT_REQUIRE_EQ(this->magic_, ChangeLogBlock::kMagic);
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status ChangeLogBlock::verify_hash() const noexcept
{
  // XXH3_64bits requires a size >= 0 or else it will segfault
  //
  BATT_CHECK_GE(this->block_size() - sizeof(ChangeLogBlock), 0);

  u64 xxh3_hash = XXH3_64bits(this + 1, this->block_size() - sizeof(ChangeLogBlock));
  BATT_REQUIRE_EQ(this->xxh3_checksum_, xxh3_hash);
  return batt::OkStatus();
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
Status ChangeLogBlock::truncate_edit_offset_upper_bound(EditOffset recovered_upper_bound,
                                                        const ChangeLogConfig& config,
                                                        llfs::IoRing::File& log_file) noexcept
{
  // If this block is already below the recovered bound, nothing needs to be done!
  //
  if (this->edit_offset_upper_bound() <= recovered_upper_bound) {
    return OkStatus();
  }

  // First reset the checksum seed, to indicate this block is no longer finalized.
  //
  this->xxh3_seed_ = 0;
  this->xxh3_checksum_ = 0;

  // If this block is entirely after the recovered upper bound, then clear it out.
  //
  if (this->edit_offset_lower_bound() >= recovered_upper_bound) {
    this->edit_offset_lower_bound_ = recovered_upper_bound.value();
    this->slot_count_ = 0;
    this->space_ = ChangeLogBlock::initial_space(this->block_size_);

  } else {
    // Remove slots until we are under the limit.
    //
    while (this->slot_count() > 0 && this->edit_offset_upper_bound() > recovered_upper_bound) {
      this->revert_last_slot();
    }

    if (this->slot_count() == 0) {
      this->edit_offset_lower_bound_ = recovered_upper_bound.value();
    }
  }
  this->check_buffer_invariant();

  // Data checksums need to be recalculated.
  //
  ConstBuffer data_to_write = this->prepare_to_flush();
  FileOffset file_offset = config.block_offset_from_index(this->get_block_index().value_or_panic());

  // Flush the updated block.
  //
  BATT_REQUIRE_OK(log_file.write_all(file_offset, data_to_write));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogBlock::revert_last_slot() noexcept
{
  this->space_ += this->slot_size(this->slot_count_ - 1) + sizeof(SlotInfo);
  this->slot_count_ -= 1;
}

}  // namespace turtle_kv
