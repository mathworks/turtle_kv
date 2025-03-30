#include <turtle_kv/checkpoint_generator.hpp>
//

#include <turtle_kv/checkpoint_log_events.hpp>

#include <batteries/async/backoff.hpp>
#include <batteries/env.hpp>
#include <batteries/status.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ CheckpointGenerator::CheckpointGenerator(batt::WorkerPool& worker_pool,    //
                                                      const TreeOptions& tree_options,  //
                                                      llfs::PageCache& cache,           //
                                                      Checkpoint&& base_checkpoint,     //
                                                      llfs::Volume& checkpoint_volume) noexcept
    : worker_pool_{worker_pool}
    , tree_options_{tree_options}
    , cache_{cache}
    , base_checkpoint_{std::move(base_checkpoint)}
    , stop_requested_{false}
    , checkpoint_volume_{checkpoint_volume}
{
  Optional<llfs::SlotRange> prev_slot_range = base_checkpoint.slot_range();
  if (prev_slot_range) {
    this->slot_sequencer_.set_current(*prev_slot_range);
    this->slot_sequencer_ = this->slot_sequencer_.get_next();
  }
  this->initialize_job();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CheckpointGenerator::~CheckpointGenerator() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CheckpointGenerator::halt() noexcept
{
  this->stop_requested_.set_value(true);
  {
    auto locked = this->cancel_token_.lock();

    if (locked->is_valid()) {
      VLOG(1) << "Cancelling batch update...";
      locked->cancel();
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CheckpointGenerator::join() noexcept
{
  // Nothing to do!  No background tasks.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> CheckpointGenerator::push_batch(std::unique_ptr<DeltaBatch>&& batch) noexcept
{
  VLOG(1) << "CheckpointGenerator::push_batch()" << BATT_INSPECT(batch->debug_info());

  // Skip unless base_checkpoint.rollup_slot_upper_bound() <= batch->slot_range.lower_bound
  //
  if (batch->batch_id() <= this->base_checkpoint_.batch_upper_bound()) {
    VLOG(1) << " -- Old batch; ignoring...";
    return {0u};
  }

  // Make sure we have an active job.
  //
  this->initialize_job();
  BATT_CHECK_NOT_NULLPTR(this->job_);

  VLOG(2) << "checkpoint task: flushing batch to create new checkpoint tree";

  {
    Optional<llfs::PageId> root_id = this->base_checkpoint_.maybe_root_id();
    if (root_id) {
      this->job_->new_root(*root_id);
      this->roots_to_remove_.emplace_back(*root_id);
    }
  }

  batt::CancelToken cancel_token;

  *this->cancel_token_.lock() = cancel_token;
  auto on_scope_exit = batt::finally([this] {
    *this->cancel_token_.lock() = batt::None;
  });

  StatusOr<Checkpoint> new_checkpoint = this->base_checkpoint_.flush_batch(this->worker_pool_,
                                                                           *this->job_,
                                                                           this->tree_options_,
                                                                           std::move(batch),
                                                                           cancel_token);

  BATT_REQUIRE_OK(new_checkpoint);

  this->base_checkpoint_ = std::move(*new_checkpoint);

  // Periodically serialize to unpin some pages, controlling total memory usage.
  //
  this->current_batch_count_ += 1;
  if (this->current_batch_count_ > 32) {
    VLOG(1) << "serializing checkpoint to cull pinned pages...";

    const i64 pinned_before_count = this->job_->pinned_page_count();
    const i64 new_before_count = this->job_->new_page_count();

    BATT_REQUIRE_OK(this->serialize_checkpoint());
    BATT_CHECK_EQ(this->current_batch_count_, 0)
        << "serialize_checkpoint() should reset the batch counter!";

    const llfs::PageId root_id = batt::get_or_panic(this->base_checkpoint_.maybe_root_id());
    this->job_->new_root(root_id);
    this->clear_old_roots();

    BATT_REQUIRE_OK(this->job_->prune(/*callers=*/0));

    this->job_->delete_root(root_id);
    this->job_->unpin_all();

    const i64 pinned_after_count = this->job_->pinned_page_count();
    const i64 new_after_count = this->job_->new_page_count();

    VLOG(1) << "pinned pages: "  //
            << pinned_before_count << " -> " << pinned_after_count << " ("
            << ((pinned_after_count > pinned_before_count) ? "+" : "")
            << (pinned_after_count - pinned_before_count) << ")"  //
            << "  new pages: "                                    //
            << new_before_count << " -> " << new_after_count << " ("
            << ((new_after_count > new_before_count) ? "+" : "")
            << (new_after_count - new_before_count) << ")";
  }

  return {1u};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CheckpointGenerator::initialize_job()
{
  if (this->job_ == nullptr) {
    this->job_ = this->cache_.new_job();
    this->job_->set_base_job(this->base_job_);
    // this->job_->set_default_unpin_on_prune(llfs::UnpinOnPrune{false});

    // this->job_->debug_mask.set(llfs::PageCacheJob::kDebugLogLoadUnpinned);
    // this->job_->debug_mask.set(llfs::PageCacheJob::kDebugLogUnpinAll);
    // this->job_->debug_mask.set(llfs::PageCacheJob::kDebugLogPrune);
    this->job_->debug_mask.set(llfs::PageCacheJob::kDebugLogCacheSlotsFull);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status CheckpointGenerator::serialize_checkpoint() noexcept
{
  BATT_CHECK_NOT_NULLPTR(this->job_);

  // Serialize the checkpoint so we know its root page id.
  //
  BATT_ASSIGN_OK_RESULT(
      this->base_checkpoint_,
      this->base_checkpoint_.serialize(this->tree_options_, *this->job_, this->worker_pool_));

  this->current_batch_count_ = 0;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CheckpointGenerator::clear_old_roots() noexcept
{
  for (const llfs::PageId root_id : this->roots_to_remove_) {
    this->job_->delete_root(root_id);
  }
  this->roots_to_remove_.clear();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Grant> CheckpointGenerator::reserve_slot_grant_for_checkpoints(usize slot_grant_size)
{
  return this->checkpoint_volume_.reserve(slot_grant_size, batt::WaitForResource::kTrue);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<CheckpointJob>> CheckpointGenerator::finalize_checkpoint(
    batt::Grant&& token,
    std::shared_ptr<batt::Grant::Issuer>&& token_issuer) noexcept
{
  VLOG(1) << "CheckpointGenerator::finalize_checkpoint()";

  BATT_CHECK_EQ(token.size(), 1u);

  BATT_CHECK_NOT_NULLPTR(this->job_)
      << "At least one batch must be pushed to the generator to finalize a new checkpoint!";

  BATT_REQUIRE_OK(this->serialize_checkpoint());

  this->clear_old_roots();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  auto checkpoint_job = std::make_unique<CheckpointJob>();

  checkpoint_job->token_issuer = std::move(token_issuer);
  checkpoint_job->token.emplace(std::move(token));
  checkpoint_job->checkpoint_log = std::addressof(this->checkpoint_volume_);
  checkpoint_job->checkpoint = this->base_checkpoint_.clone();

  checkpoint_job->packed_checkpoint.emplace(
      llfs::PackAsVariant<CheckpointLogEvent, PackedCheckpoint>{
          PackedCheckpoint{
              .batch_upper_bound = this->base_checkpoint_.batch_upper_bound().int_value(),
              .new_tree_root = llfs::PackedPageId::from(this->base_checkpoint_.root_id()),
          },
      });

  // Package the job up with a PackedCheckpoint event record so we can append it to the Volume.
  //
  StatusOr<llfs::AppendableJob> appendable_job =
      llfs::make_appendable_job(std::move(this->job_),
                                llfs::PackableRef{*checkpoint_job->packed_checkpoint});

  BATT_REQUIRE_OK(appendable_job);

  // Reserve slot grant for the current checkpoint in checkpoint-log.
  //
  auto grant_size = appendable_job->calculate_grant_size();
  StatusOr<batt::Grant> checkpoint_grant = this->reserve_slot_grant_for_checkpoints(grant_size);

  BATT_REQUIRE_OK(checkpoint_grant);

  checkpoint_job->append_job_grant.emplace(std::move(*checkpoint_grant));
  checkpoint_job->appendable_job.emplace(std::move(*appendable_job));
  checkpoint_job->prepare_slot_sequencer.emplace(this->slot_sequencer_);

  this->base_job_ = checkpoint_job->appendable_job->job.finalized_job();
  this->slot_sequencer_ = this->slot_sequencer_.get_next();

  BATT_CHECK_EQ(this->job_, nullptr);

  VLOG(1) << "checkpoint finalized";

  return {std::move(checkpoint_job)};
}

}  // namespace turtle_kv
