#pragma once

#include <turtle_kv/tree/packed_tree_checkpoint.hpp>
#include <turtle_kv/tree/tree_checkpoint.hpp>
#include <turtle_kv/tree/tree_checkpoint_log_events.hpp>

#include <turtle_kv/import/optional.hpp>

#include <llfs/appendable_job.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_sequencer.hpp>
#include <llfs/volume.hpp>

#include <batteries/async/future.hpp>
#include <batteries/async/grant.hpp>

#include <memory>

namespace turtle_kv {

struct TreeCheckpointJob {
  TreeCheckpointJob() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  TreeCheckpointJob(const TreeCheckpointJob&) = delete;
  TreeCheckpointJob& operator=(const TreeCheckpointJob&) = delete;
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // This object retains a shared ownership reference to the token issuer to guarantee correct
  // teardown order.  IMPORTANT: this field must be before `token` so its scope will be wider.
  //
  std::shared_ptr<batt::Grant::Issuer> token_issuer;

  // A Grant of size 1, used to rate-limit the checkpoint generation pipeline.
  //
  Optional<batt::Grant> token;

  llfs::Volume* checkpoint_log = nullptr;

  Optional<TreeCheckpoint> checkpoint;

  Optional<llfs::PackAsVariant<TreeCheckpointLogEvent, PackedTreeCheckpoint>> packed_checkpoint;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<batt::Grant> append_job_grant;

  Optional<llfs::AppendableJob> appendable_job;

  Optional<llfs::SlotSequencer> prepare_slot_sequencer;

  batt::Promise<llfs::SlotRange> promise;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

}  // namespace turtle_kv
