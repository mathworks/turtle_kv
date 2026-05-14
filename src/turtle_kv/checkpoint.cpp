//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/checkpoint.hpp>
//

#include <turtle_kv/tree/in_memory_leaf.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <llfs/page_cache_overcommit.hpp>
#include <llfs/status_code.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/case_of.hpp>
#include <batteries/utility.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<Checkpoint> Checkpoint::recover(
    llfs::Volume& checkpoint_volume,
    llfs::SlotParse& slot,
    const PackedCheckpoint& packed_checkpoint) noexcept
{
  VLOG(1) << "Entering Checkpoint::recover";

  BATT_CHECK_GT(packed_checkpoint.edit_offset_upper_bound, 0)
      << "Invalid PackedCheckpoint: batch_upper_bound==0 indicates no checkpoint.";

  const llfs::PageId tree_root_id = packed_checkpoint.new_tree_root.as_page_id();

  Subtree tree = Subtree::from_page_id(tree_root_id);

  batt::StatusOr<i32> height =
      tree.get_height(checkpoint_volume.cache(), llfs::PageCacheOvercommit::not_allowed());

  BATT_REQUIRE_OK(height);
  BATT_ASSIGN_OK_RESULT(llfs::SlotReadLock slot_read_lock,

                        checkpoint_volume.lock_slots(slot.offset,
                                                     llfs::LogReadMode::kDurable,
                                                     /*lock_holder=*/"Checkpoint::recover"));

  VLOG(1) << "Exiting Checkpoint::recover";
  return Checkpoint{
      tree_root_id,
      std::make_shared<Subtree>(std::move(tree)),
      *height,
      EditOffset{packed_checkpoint.edit_offset_upper_bound.value()},
      CheckpointLock::make_durable(std::move(slot_read_lock)),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Checkpoint Checkpoint::make_empty() noexcept
{
  return Checkpoint{};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Checkpoint::Checkpoint() noexcept
    : root_id_{llfs::PageId{llfs::kInvalidPageId}}
    , tree_{std::make_shared<Subtree>(Subtree::make_empty())}
    , tree_height_{0}
    , checkpoint_upper_bound_{EditOffset{0}}
    , checkpoint_lock_{CheckpointLock::make_durable_detached()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Checkpoint::Checkpoint(Optional<llfs::PageId> root_id,
                       std::shared_ptr<Subtree>&& tree,
                       i32 tree_height,
                       std::variant<EditOffset, DeltaBatchId> checkpoint_upper_bound,
                       CheckpointLock&& checkpoint_lock) noexcept
    : root_id_{root_id}
    , tree_{std::move(tree)}
    , tree_height_{tree_height}
    , checkpoint_upper_bound_{checkpoint_upper_bound}
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
Status Checkpoint::validate_ready_to_serialize() const noexcept
{
  BATT_REQUIRE_OK(batt::case_of(
      this->checkpoint_upper_bound_,
      [](const EditOffset&) -> Status {
        // If tree is not serialized, we should have a DeltaBatchId here, not an EditOffset!
        //
        return batt::StatusCode::kInternal;
      },
      [](const DeltaBatchId& batch_id) -> Status {
        if (!batch_id.is_last_in_group()) {
          return batt::StatusCode::kFailedPrecondition;
        }
        return OkStatus();
      }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Checkpoint> Checkpoint::serialize(
    const TreeOptions& tree_options,
    llfs::PageCacheJob& job,
    llfs::PageCacheOvercommit& overcommit,
    batt::WorkerPool& worker_pool,
    const boost::intrusive_ptr<FilterPageWriteState>& filter_page_write_state) const noexcept
{
  if (this->tree_->is_serialized()) {
    BATT_CHECK(this->root_id_);
    return {batt::make_copy(*this)};
  }

  BATT_REQUIRE_OK(this->validate_ready_to_serialize());

  TreeSerializeContext serialize_context{
      tree_options,
      job,
      worker_pool,
      overcommit,
      batt::make_copy(filter_page_write_state),
  };

  BATT_REQUIRE_OK(this->tree_->start_serialize(serialize_context));
  BATT_REQUIRE_OK(serialize_context.build_all_pages());
  BATT_ASSIGN_OK_RESULT(const llfs::PageId new_tree_root_id,
                        this->tree_->finish_serialize(serialize_context));

  BATT_ASSIGN_OK_RESULT(const i32 serialized_height, this->tree_->get_height(job, overcommit));
  BATT_CHECK_EQ(serialized_height, this->tree_height_);

  return Checkpoint{
      new_tree_root_id,
      batt::make_copy(this->tree_),
      this->tree_height_,
      this->edit_offset_upper_bound(),
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
Status Checkpoint::validate_next_batch_id(const DeltaBatchId& new_batch_id) const noexcept
{
  {
    // New batch groups must start with index 0!  (Batch groups are uniquely identified by their
    // edit offset upper bound.)
    //
    const EditOffset current_edit_offset = this->edit_offset_upper_bound();
    if (new_batch_id.edit_offset_upper_bound() > current_edit_offset &&
        new_batch_id.index_in_group() != 0) {
      return {batt::StatusCode::kInvalidArgument};
    }
  }

  BATT_REQUIRE_OK(  //
      batt::case_of(
          this->checkpoint_upper_bound_,
          [&new_batch_id](const EditOffset& old_edit_offset) -> Status {
            // If we are applying a batch over a (serialized) edit offset, make sure we are going
            // strictly forwards.
            //
            if (new_batch_id.edit_offset_upper_bound() <= old_edit_offset) {
              return {batt::StatusCode::kInvalidArgument};
            }
            return OkStatus();
          },
          [&new_batch_id](const DeltaBatchId& old_batch_id) -> Status {
            if (new_batch_id.edit_offset_upper_bound() == old_batch_id.edit_offset_upper_bound()) {
              // We can't apply a batch in the same group *after* the last one!
              //
              if (old_batch_id.is_last_in_group()) {
                return {batt::StatusCode::kFailedPrecondition};
              }

              // Within the same group, batches must be applied in-order and gapless.
              //
              if (new_batch_id.index_in_group() != old_batch_id.index_in_group() + 1) {
                return {batt::StatusCode::kInvalidArgument};
              }

              return OkStatus();
            }

            // It is not allowed to apply batches for an older group.
            //
            if (new_batch_id.edit_offset_upper_bound() < old_batch_id.edit_offset_upper_bound()) {
              return {batt::StatusCode::kInvalidArgument};
            }

            BATT_CHECK_GT(new_batch_id.edit_offset_upper_bound(),
                          old_batch_id.edit_offset_upper_bound());

            // If starting a new group, the batch in the old group must be marked as last.
            //
            if (!old_batch_id.is_last_in_group()) {
              return {batt::StatusCode::kFailedPrecondition};
            }

            return OkStatus();
          }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Checkpoint> Checkpoint::apply_batch(batt::WorkerPool& worker_pool,
                                             llfs::PageCacheJob& job,
                                             const TreeOptions& tree_options,
                                             BatchUpdateMetrics& metrics,
                                             llfs::PageCacheOvercommit& overcommit,
                                             std::unique_ptr<DeltaBatch>&& delta_batch,
                                             const batt::CancelToken& cancel_token) noexcept
{
  BATT_REQUIRE_OK(this->validate_next_batch_id(delta_batch->batch_id()));

  BatchUpdate update{
      .context =
          BatchUpdateContext{
              .worker_pool = worker_pool,
              .page_loader = job,
              .cancel_token = cancel_token,
              .metrics = metrics,
              .overcommit = overcommit,
          },
      .result_set = delta_batch->consume_result_set(),
      .edit_size_totals = None,
  };

  BATT_CHECK_NE(update.result_set.size(), 0);

  BATT_REQUIRE_OK(this->tree_->apply_batch_update(tree_options,
                                                  ParentNodeHeight{this->tree_height_ + 1},
                                                  update,
                                                  /*key_upper_bound=*/global_max_key(),
                                                  IsRoot{true}));

  BATT_ASSIGN_OK_RESULT(i32 new_tree_height, this->tree_->get_height(job, overcommit));

  return Checkpoint{
      /*root_page_id=*/this->tree_->get_page_id(),
      batt::make_copy(this->tree_),
      /*tree_height=*/new_tree_height,
      delta_batch->batch_id(),
      CheckpointLock::make_speculative(std::move(delta_batch),
                                       batt::make_copy(this->checkpoint_lock_)),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Checkpoint Checkpoint::clone() const noexcept
{
  return Checkpoint{this->root_id_,
                    std::make_shared<Subtree>(this->tree_->clone_serialized_or_panic()),
                    this->tree_height_,
                    this->checkpoint_upper_bound_,
                    this->clone_checkpoint_lock()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> Checkpoint::find_key(KeyQuery& query) const
{
  return this->tree_->find_key(ParentNodeHeight{this->tree_height_ + 1}, query);
}

}  // namespace turtle_kv
