#pragma once

#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_log_events.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/packed_checkpoint.hpp>

#include <turtle_kv/import/optional.hpp>

#include <llfs/slot.hpp>
#include <llfs/slot_sequencer.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/grant.hpp>

#include <memory>

namespace turtle_kv {

struct CheckpointJob {
  CheckpointJob() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  CheckpointJob(const CheckpointJob&) = delete;
  CheckpointJob& operator=(const CheckpointJob&) = delete;
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::PageCacheJob& job() const
  {
    BATT_CHECK(this->appendable_job);
    return this->appendable_job->get_const_job();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // This object retains a shared ownership reference to the token issuer to guarantee correct
  // teardown order.  IMPORTANT: this field must be before `token` so its scope will be wider.
  //
  std::shared_ptr<batt::Grant::Issuer> token_issuer;

  // A Grant of size 1, issued from `Tablet::checkpoint_tokens_`; used to rate-limit the
  // checkpoint generation process.
  //
  Optional<batt::Grant> token;

  llfs::Volume* checkpoint_log = nullptr;

  Optional<Checkpoint> checkpoint;

  Optional<llfs::PackAsVariant<CheckpointLogEvent, PackedCheckpoint>> packed_checkpoint;

  //----- --- -- -  -  -   -

  Optional<batt::Grant> append_job_grant;

  Optional<llfs::AppendableJob> appendable_job;

  Optional<llfs::SlotSequencer> prepare_slot_sequencer;

  batt::Promise<llfs::SlotRange> promise;

  DeltaBatchId batch_id_upper_bound = DeltaBatchId::min_value();

  usize batch_count = 0;
};

/** \brief Returns the least-ordered DeltaBatchId *not* included in the passed checkpoint.
 */
inline DeltaBatchId get_batch_upper_bound(const CheckpointJob& job)
{
  return job.batch_id_upper_bound;
}

}  // namespace turtle_kv
