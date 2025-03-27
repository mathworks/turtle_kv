#include <turtle_kv/checkpoint_lock.hpp>
//

#include <batteries/case_of.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ CheckpointLock CheckpointLock::make_speculative(
    std::unique_ptr<DeltaBatch>&& delta_batch,
    CheckpointLock&& base_checkpoint_lock)
{
  CheckpointLock self;

  self.impl_ = batt::make_shared<Impl>();
  *self.impl_->state.lock() = Impl::SpeculativeState{
      std::move(delta_batch),
      batt::make_copy(base_checkpoint_lock.impl_),
  };

  return self;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ CheckpointLock CheckpointLock::make_durable(
    llfs::SlotReadLock&& checkpoint_wal_slot_lock)
{
  CheckpointLock self;

  self.impl_ = batt::make_shared<Impl>();
  *self.impl_->state.lock() = Impl::DurableState{std::move(checkpoint_wal_slot_lock)};
  self.impl_->is_durable.set_value(true);

  return self;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ CheckpointLock CheckpointLock::make_durable_detached()
{
  return CheckpointLock::make_durable(llfs::SlotReadLock::null_lock());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool CheckpointLock::is_durable() const
{
  return this->impl_ != nullptr && batt::is_case<Impl::DurableState>(*this->impl_->state.lock());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool CheckpointLock::notify_durable(llfs::SlotReadLock&& checkpoint_wal_slot_lock)
{
  auto locked = this->impl_->state.lock();
  if (batt::is_case<Impl::DurableState>(*locked)) {
    return false;
  }
  *locked = Impl::DurableState{std::move(checkpoint_wal_slot_lock)};
  this->impl_->is_durable.set_value(true);
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status CheckpointLock::await_durable()
{
  BATT_CHECK_NOT_NULLPTR(this->impl_);
  BATT_REQUIRE_OK(this->impl_->is_durable.await_equal(true));
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<llfs::SlotRange> CheckpointLock::slot_range() const
{
  if (this->impl_ == nullptr) {
    return None;
  }
  return batt::case_of(
      *this->impl_->state.lock(),
      [](const Impl::DurableState& durable) -> Optional<llfs::SlotRange> {
        return durable.checkpoint_slot_lock.slot_range();
      },
      [](const auto& /*not_durable*/) -> Optional<llfs::SlotRange> {
        return None;
      });
}

}  // namespace turtle_kv
