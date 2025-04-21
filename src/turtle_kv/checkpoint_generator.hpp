#pragma once

#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/checkpoint_job.hpp>
#include <turtle_kv/checkpoint_lock.hpp>
#include <turtle_kv/delta_batch.hpp>

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/status.hpp>

#include <llfs/finalized_page_cache_job.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_sequencer.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/mutex.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/small_vec.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Accepts batches of deltas (edits) and applies them to the most recent checkpoint to
 * produce a series of new checkpoints.
 *
 * The usage pattern for this object is as follows:
 *
 * 1. Create instance: generator
 * 2. generator.push_batch(batch)
 * 3. generator.finalize_checkpoint(token, token_issuer)
 *
 * These steps can be repeated any number of times.  Steps 2 (push_batch) can happen in any order.
 * However, it will not be possible to call finalize_checkpoint until sufficient checkpoint grant(s)
 * have been pushed.
 *
 * There can be multiple calls to push_batch for a given call to
 * finalize_checkpoint.  This will create a "roll-up" checkpoint that includes multiple batches.
 * This may be desirable to amortize the fixed cost overhead of writing a checkpoint to durable
 * storage (YMMV).
 */
class CheckpointGenerator
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit CheckpointGenerator(batt::WorkerPool& worker_pool,    //
                               const TreeOptions& tree_options,  //
                               llfs::PageCache& cache,           //
                               Checkpoint&& base_checkpoint,     //
                               llfs::Volume& checkpoint_volume) noexcept;

  CheckpointGenerator(const CheckpointGenerator&) = delete;
  CheckpointGenerator& operator=(const CheckpointGenerator&) = delete;

  ~CheckpointGenerator() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Requests that this object shut down.
   */
  void halt() noexcept;

  /** \brief Blocks the caller until any background activity managed by this object has finished.
   */
  void join() noexcept;

  /** \brief Flush a batch to the current base checkpoint tree.
   *
   * If the slot range of the batch has already been covered by a previous checkpoint (which can
   * happen during/after recovery), this function returns 0 to indicate no batch was added to the
   * checkpoint.  Otherwise it tries to do a batch update against the current checkpoint tree, and
   * if that succeeds, 1 is returned.
   *
   * \return The number of batches accepted if successful; error Status otherwise.
   */
  StatusOr<usize> push_batch(std::unique_ptr<DeltaBatch>&& batch) noexcept;

  /** \brief Finalize the current checkpoint rollup and return a CheckpointJob that can be handed to
   * a checkpoint committer.
   *
   *`token` must be a grant of size 1 issued from a pool controlling the rate at which checkpoints
   * are created (this bounds the checkpoint pipeline and provides back pressure).
   *
   * `push_batch` MUST be called at least once after construction and before this call, and at least
   * once in between calls to `finalize_checkpoint` so that we have a new checkpoint to finalize.
   */
  StatusOr<std::unique_ptr<CheckpointJob>> finalize_checkpoint(
      batt::Grant&& token,
      std::shared_ptr<batt::Grant::Issuer>&& token_issuer) noexcept;

  llfs::PageCacheJob& page_cache_job() const
  {
    return *this->job_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Creates `this->job_` if not already created.  Sets dependency on `base_job_`.
   */
  void initialize_job();

  /** \brief Serializes any deferred pages in the current base_checkpoint_ tree.
   */
  Status serialize_checkpoint() noexcept;

  /** \brief Clears `this->roots_to_remove_` by deleting obsolete root pages from the current job's
   * root set.
   */
  void clear_old_roots() noexcept;

  /** \brief This function first uses a non-blocking call to Volume::reserve on the checkpoint
   * volume. If non-blocking reserve fails, it tracks some metrics and then goes into a blocking
   * reserve call.
   */
  StatusOr<batt::Grant> reserve_slot_grant_for_checkpoints(usize slot_grant_size);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Used to parallelize work during batch updates.
  //
  batt::WorkerPool& worker_pool_;

  // Tree config.
  //
  const TreeOptions tree_options_;

  // The cache that provides the backing store for checkpoints.
  //
  llfs::PageCache& cache_;

  // The current job.  This job is lazily created when `push_batch` is called, and is reset after
  // `finalize_checkpoint`.
  //
  std::unique_ptr<llfs::PageCacheJob> job_;

  // The latest checkpoint tree.
  //
  Checkpoint base_checkpoint_;

  // The number of times batch update has been called for the current checkpoint; reset to 0
  // whenever finalize_checkpoint is called.
  //
  usize current_batch_count_ = 0;

  // The previous PageCache job, which may contain pages referenced by `base_checkpoint_tree`, but
  // which are not yet written to storage.
  //
  llfs::FinalizedPageCacheJob base_job_;

  // Enforces the ordering of PrepareJob slots when the checkpoint is appended to the WAL. This is
  // not used directly by this class, but rather by the checkpoint committer.
  //
  llfs::SlotSequencer slot_sequencer_;

  // Roots to remove from `job_` when finalizing.
  //
  batt::SmallVec<llfs::PageId, 8> roots_to_remove_;

  // Set to `true` when halt is invoked.
  //
  batt::Watch<bool> stop_requested_;

  // Used to allocate grants directly from checkpoint_volume.
  //
  llfs::Volume& checkpoint_volume_;

  // Used to cancel pending checkpoint updates on halt().
  //
  batt::Mutex<batt::CancelToken> cancel_token_{batt::None};
};

}  // namespace turtle_kv
