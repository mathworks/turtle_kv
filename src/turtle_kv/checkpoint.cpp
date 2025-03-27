#include <turtle_kv/checkpoint.hpp>
//

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <llfs/status_code.hpp>

#include <batteries/async/cancel_token.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<Checkpoint> Checkpoint::recover(
    llfs::Volume& checkpoint_volume,
    const llfs::SlotWithPayload<TabletCheckpoint>& packed_checkpoint) noexcept
{
  const llfs::PageId tree_root_id = packed_checkpoint.payload.new_tree_root.as_page_id();

  BATT_ASSIGN_OK_RESULT(
      std::shared_ptr<const TreeView> tree,
      TreeView::from_page(
          checkpoint_volume.cache().get_page(tree_root_id, llfs::OkIfNotFound{false})));

  if (static_cast<i16>(tree->height()) != packed_checkpoint.payload.new_tree_height) {
    // return make_db_status(turtle_db::DBStatusCodes::kBadRecoveredTreeHeight);
    return {batt::StatusCode::kDataLoss};  // TODO [tastolfi 2025-02-20]
  }

  BATT_ASSIGN_OK_RESULT(llfs::SlotReadLock slot_read_lock,
                        checkpoint_volume.lock_slots(packed_checkpoint.slot_range,
                                                     llfs::LogReadMode::kDurable,
                                                     /*lock_holder=*/"Checkpoint::recover"));

  return Checkpoint{
      tree_root_id,
      std::move(tree),
      DeltaBatchId::from_u64(packed_checkpoint.payload.slot_upper_bound),
      CheckpointLock::make_durable(std::move(slot_read_lock)),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Checkpoint Checkpoint::empty_at_batch(DeltaBatchId batch_id) noexcept
{
  return Checkpoint{llfs::PageId{llfs::kInvalidPageId},
                    TreeView::new_empty(),
                    batch_id,
                    CheckpointLock::make_durable_detached()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Checkpoint::Checkpoint() noexcept
    : root_id_{llfs::PageId{llfs::kInvalidPageId}}
    , tree_{TreeView::new_empty()}
    , batch_upper_bound_{0}
    , checkpoint_lock_{CheckpointLock::make_durable_detached()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Checkpoint::Checkpoint(Optional<llfs::PageId> root_id,
                       std::shared_ptr<const TreeView>&& tree,
                       DeltaBatchId batch_upper_bound,
                       CheckpointLock&& checkpoint_lock) noexcept
    : root_id_{root_id}
    , tree_{std::move(tree)}
    , batch_upper_bound_{batch_upper_bound}
    , checkpoint_lock_{std::move(checkpoint_lock)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageId Checkpoint::root_id() const
{
  BATT_CHECK(this->root_id_) << "Forget to call Checkpoint::serialize()?";
  return *this->root_id_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Checkpoint> Checkpoint::serialize(const TreeOptions& tree_options,
                                           llfs::PageCacheJob& job,
                                           batt::WorkerPool& worker_pool) const noexcept
{
  if (this->tree_->is_serialized()) {
    BATT_CHECK(this->root_id_);
    return {batt::make_copy(*this)};
  }

  TreeSerializeContext serialize_context{tree_options, job, worker_pool};

  BATT_REQUIRE_OK(this->tree_->start_serialize(serialize_context));
  BATT_REQUIRE_OK(serialize_context.build_all_pages());
  BATT_ASSIGN_OK_RESULT(llfs::PinnedPage pinned_root_page,
                        this->tree_->finish_serialize(serialize_context));

  const llfs::PageId new_tree_root_id = pinned_root_page.page_id();

  return Checkpoint{
      new_tree_root_id,
      batt::make_copy(this->tree_),
      this->batch_upper_bound_,
      batt::make_copy(this->checkpoint_lock_),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<llfs::SlotRange> Checkpoint::slot_range() const
{
  return this->checkpoint_lock_.slot_range();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool Checkpoint::notify_durable(llfs::SlotReadLock&& slot_read_lock)
{
  return this->checkpoint_lock_.notify_durable(std::move(slot_read_lock));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Checkpoint::await_durable()
{
  return this->checkpoint_lock_.await_durable();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool Checkpoint::is_durable() const noexcept
{
  return this->checkpoint_lock_.is_durable();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Checkpoint> Checkpoint::flush_batch(batt::WorkerPool& worker_pool,
                                             llfs::PageCacheJob& job,
                                             const TreeOptions& tree_options,
                                             std::unique_ptr<DeltaBatch>&& delta_batch,
                                             const batt::CancelToken& cancel_token) noexcept
{
  BatchUpdate update{
      .worker_pool = worker_pool,
      .page_loader = job,
      .cancel_token = cancel_token,
      .result_set = delta_batch->consume_result_set(),
      .edit_size_totals = None,
  };

  BATT_ASSIGN_OK_RESULT(i32 tree_height, this->tree_->get_height(job));

  BATT_REQUIRE_OK(this->tree_->apply_batch_update(tree_options,
                                                  ParentNodeHeight{tree_height + 1},
                                                  update,
                                                  /*key_upper_bound=*/global_max_key(),
                                                  IsRoot{true}));

  return Checkpoint{
      /*root_page_id=*/this->tree_->get_page_id(),
      batt::make_copy(this->tree_),
      delta_batch->batch_id(),
      CheckpointLock::make_speculative(std::move(delta_batch),
                                       batt::make_copy(this->checkpoint_lock_)),
  };
}

}  // namespace turtle_kv
