//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_writer.hpp>
//

#include <turtle_kv/util/small_queue.hpp>

#include <chrono>
#include <cstdlib>
#include <random>

namespace turtle_kv {

constexpr usize kStaticQueueSize = 16;

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void release_blocks(SmallQueueBase<ChangeLogBlock*>& blocks) noexcept
{
  for (ChangeLogBlock* p_block : blocks) {
    p_block->remove_ref(1);
  }
  blocks.clear();
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ChangeLogWriter::CollectedBlocksState {
  /** \brief Blocks that have been collected from appender threads; these are waiting to be placed
   * in the change log file by transferring to PreparedBlocksState.
   */
  SmallQueue<BlockBuffer*, kStaticQueueSize> blocks;

  //----- --- -- -  -  -   -

  ~CollectedBlocksState() noexcept
  {
    release_blocks(this->blocks);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ChangeLogWriter::PreparedBlocksState {
  /** \brief The position in the file where the next write will take place.
   */
  FileOffset file_offset;

  /** \brief The unwritten portions of the prepared blocks.  There is exactly one element in
   * `block_chunks` for each element in `blocks` (below).
   */
  SmallQueue<ConstBuffer, kStaticQueueSize> block_chunks;

  /** \brief The blocks currently prepared for write.
   */
  SmallQueue<BlockBuffer*, kStaticQueueSize> blocks;

  //----- --- -- -  -  -   -

  ~PreparedBlocksState() noexcept
  {
    release_blocks(this->blocks);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ChangeLogWriter::WrittenBlocksState {
  /** \brief The (block-size-unit) offset within the file of the first block in the queue.
   */
  Optional<BlockIndex> block_index;

  /** \brief Blocks known to have been written, but not yet added to the active blocks state.
   */
  SmallQueue<BlockBuffer*, kStaticQueueSize> blocks;

  //----- --- -- -  -  -   -

  ~WrittenBlocksState() noexcept
  {
    release_blocks(this->blocks);
  }

  void check_invariants(const ChangeLogFile::Config& config) const noexcept
  {
    BATT_CHECK_IMPLIES(!this->blocks.empty(), this->block_index)
        << "If there are written blocks, then block_index must be set!";

    BATT_CHECK_LT(this->block_index.value_or(BlockIndex{0}), config.block_count)
        << "block_index must be strictly less than the block count!";
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ChangeLogWriter::ActiveBlocksState {
  /** \brief Aka the "trim position" -- this is the position in the file, not the logical offset.
   */
  BlockIndex active_lower_bound_block;

  /** \brief Aka the "flush position" -- this is the position in the file (not logical offset) one
   * past the *last* known written and untrimmed block.
   */
  BlockIndex active_upper_bound_block;

  /** \brief Aka the "trim offset" -- this is the logical value (not the position in the file).
   */
  EditOffset active_edit_offset_lower_bound;

  /** \brief For each block index in the file, the known upper bound edit offset of slots in that
   * block.
   */
  std::unique_ptr<i64[]> block_upper_bounds;

  /** \brief When blocks are transferred from `WrittenBlocksState` to `ActiveBlocksState`, we move
   * their grant form the BlockBuffer to this shared grant pool.
   */
  batt::Grant in_use_block_grant;

  //----- --- -- -  -  -   -

  explicit ActiveBlocksState(const ChangeLogFile::Config& config,
                             batt::Grant::Issuer& block_grant_pool,
                             const Interval<BlockIndex>& active_block_range,
                             const Slice<EditOffset>& active_blocks_upper_bounds) noexcept
      : active_lower_bound_block{active_block_range.lower_bound}
      , active_upper_bound_block{active_block_range.upper_bound}
      , active_edit_offset_lower_bound{0 /*TODO [tastolfi 2026-04-20] - pass this in*/}
      , block_upper_bounds{new i64[config.block_count]}
      , in_use_block_grant{
            BATT_OK_RESULT_OR_PANIC(block_grant_pool.issue_grant(0, batt::WaitForResource::kFalse))}
  {
    const usize block_count = BATT_CHECKED_CAST(usize, config.block_count);

    BATT_CHECK_LT(this->active_lower_bound_block, config.block_count);

    BATT_CHECK_LE(this->active_lower_bound_block, this->active_upper_bound_block);

    BATT_CHECK_EQ(
        active_blocks_upper_bounds.size(),
        BATT_CHECKED_CAST(usize, this->active_upper_bound_block - this->active_lower_bound_block));

    BATT_CHECK_LE(active_blocks_upper_bounds.size(), block_count);

    for (usize src_i = 0, dst_i = this->active_lower_bound_block;
         src_i < active_blocks_upper_bounds;
         ++src_i) {
      this->block_upper_bounds[dst_i] = active_blocks_upper_bounds[src_i].value();
      ++dst_i;
      if (dst_i == block_count) {
        dst_i = 0;
      }
    }

    this->in_use_block_grant.subsume(BATT_OK_RESULT_OR_PANIC(
        block_grant_pool.issue_grant(active_block_range.size(), batt::WaitForResource::kFalse)));
  }

  void check_invariants(const ChangeLogFile::Config& config) const noexcept
  {
    BATT_CHECK_LE(this->active_lower_bound_block, this->active_upper_bound_block)
        << "active_lower_bound_block and active_upper_bound_block must form a valid Interval";

    BATT_CHECK_LE(this->active_upper_bound_block - this->active_lower_bound_block,
                  config.block_count)
        << "The active block interval must not be larger than the maximum block count";

    BATT_CHECK_LT(this->active_lower_bound_block, config.block_count)
        << "active_lower_bound_block must always be a valid (physical) block index within the file";

    BATT_CHECK_EQ(
        BATT_CHECKED_CAST(u64, this->active_upper_bound_block - this->active_lower_bound_block),
        in_use_block_grant.size())
        << "in_use_block_grant must exactly cover the active block interval";
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void ChangeLogWriter::check_invariants(const ChangeLogFile::Config& config,
                                                  const WrittenBlocksState& written_blocks,
                                                  const ActiveBlocksState& active_blocks) noexcept
{
  if (written_blocks.block_index.has_value()) {
    const i64 block_i = *written_blocks.block_index;

    BATT_CHECK(block_i == active_blocks.active_upper_bound_block ||
               block_i + config.block_count == active_blocks.active_upper_bound_block)
        << "The WrittenBlocksState::block_index must agree with "
           "ActiveBlocksState::active_upper_bound_block!"
        << BATT_INSPECT(block_i) << BATT_INSPECT(active_blocks.active_upper_bound_block)
        << BATT_INSPECT(config.block_count);
  }
  x
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ChangeLogWriter::Context

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogWriter::Context::Context(ChangeLogWriter& writer) noexcept : writer_{writer}
{
  this->writer_.add_context(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::Context::~Context() noexcept
{
  this->writer_.remove_context(*this);

  BATT_CHECK_EQ(this->head_.load(), nullptr)
      << "ChangeLogWriter::remove_context should have consumed all buffers!";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::Context::consume_buffers() noexcept -> BlockBuffer*
{
  return this->head_.exchange(nullptr);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::Context::pop_buffer(BlockBuffer*& observed_head) noexcept -> BlockBuffer*
{
  BlockBuffer* buffer = this->head_.exchange(nullptr);
  BlockBuffer* old_next = buffer ? buffer->swap_next(nullptr) : nullptr;
  if (old_next != nullptr) {
    BATT_CHECK_OK(old_next->verify());
    this->head_.store(old_next);
  }
  observed_head = old_next;
  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::Context::push_buffer(BlockBuffer*& buffer,
                                           BlockBuffer*& observed_head) noexcept
{
  BATT_CHECK_OK(buffer->verify());
  for (;;) {
    buffer->set_next(observed_head);
    if (this->head_.compare_exchange_weak(observed_head, buffer)) {
      observed_head = buffer;
      buffer = nullptr;
      break;
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ChangeLogWriter

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<ChangeLogWriter>> ChangeLogWriter::open_or_create(
    const std::filesystem::path& path,        //
    const ChangeLogFile::Config& config,      //
    const ChangeLogWriter::Options& options,  //
    RemoveExisting remove_existing            //
    ) noexcept
{
  std::error_code ec;
  if (remove_existing || !std::filesystem::exists(path, ec) || ec) {
    BATT_REQUIRE_OK(ChangeLogFile::create(path, config, remove_existing));
  }
  BATT_REQUIRE_OK(ec);

  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

  return {std::make_unique<ChangeLogWriter>(std::move(log_file), options)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<ChangeLogWriter>> ChangeLogWriter::open(
    const std::filesystem::path& path,                //
    Optional<ChangeLogWriter::Options> maybe_options  //
    ) noexcept
{
  Options options = maybe_options.value_or(Options::with_default_values());

  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

  return {std::make_unique<ChangeLogWriter>(std::move(log_file), options)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogWriter::ChangeLogWriter(
    std::unique_ptr<ChangeLogFile>&& change_log,
    const Options& options,
    const Interval<BlockIndex>& active_block_range,
    const Slice<EditOffset>& active_blocks_upper_bounds) noexcept
    : change_log_{std::move(change_log)}
    , options_{options}
{
  {
    batt::ScopedLock<State> locked_state{this->state_};
    locked_state->active_blocks_state_ =
        std::make_unique<ActiveBlocksState>(this->config(),
                                            this->free_block_tokens_,
                                            active_block_range,
                                            active_blocks_upper_bounds);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::~ChangeLogWriter() noexcept
{
  this->halt();
  this->join();

  // Collect and release any remaining block buffers.
  {
    CollectedBlocksState collected;
    this->collect_blocks(collected).IgnoreError();
  }

  this->state_.with_lock([](State& state) {
    state.check_ready_to_shut_down();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::start(batt::Task::executor_type&& executor) noexcept
{
  BATT_CHECK(!this->task_);

  this->task_.emplace(
      std::move(executor),
      [this] {
        this->writer_task_main();
      },
      "ChangeLogWriter::writer_task_main");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::halt() noexcept
{
  this->halt_requested_.store(true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::join() noexcept
{
  if (this->task_) {
    this->task_->join();
    this->task_ = None;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::add_context(Context& context) noexcept
{
  batt::ScopedLock<State> locked_state{this->state_};
  locked_state->contexts_.emplace_back(std::addressof(context));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::remove_context(Context& context) noexcept
{
  {
    batt::ScopedLock<State> locked_state{this->state_};

    //----- --- -- -  -  -   -
    // Remove `context` from the State.
    //
    auto iter = std::find(locked_state->contexts_.begin(),
                          locked_state->contexts_.end(),
                          std::addressof(context));

    BATT_CHECK_NE(iter, locked_state->contexts_.end())
        << BATT_INSPECT_RANGE(locked_state->contexts_)
        << BATT_INSPECT((void*)std::addressof(context));

    std::swap(*iter, locked_state->contexts_.back());
    locked_state->contexts_.pop_back();

    //----- --- -- -  -  -   -
    // Claim ownership over any (unwritten) buffers this Context may be holding, so we can flush
    // them the next time the write task wakes up.
    //
    BlockBuffer* stack = context.consume_buffers();
    if (stack != nullptr) {
      locked_state->ready_to_write_.push_back(stack);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::allocate_buffer(EditOffset offset) noexcept -> StatusOr<BlockBuffer*>
{
  BATT_ASSIGN_OK_RESULT(
      batt::Grant buffer_grant,
      this->change_log_->reserve_blocks(BlockCount{1}, batt::WaitForResource::kTrue));

  return BlockBuffer::allocate(offset, std::move(buffer_grant), this->config().block_size);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::poll_updates() noexcept -> batt::SmallVec<BlockBuffer*, 8>
{
  this->metrics_.poll_count.add(1);

  batt::SmallVec<BlockBuffer*, 8> update_buffers;
  batt::SmallVec<BlockBuffer*, 8> buffer_stacks;
  {
    batt::ScopedLock<State> locked_state{this->state_};

    // First collect the stacks from Contexts which have gone out-of-scope, since they are more
    // likely to have produced older (EditOffset) data.
    //
    for (BlockBuffer* stack : locked_state->ready_to_write_) {
      BATT_CHECK_NOT_NULLPTR(stack);
      buffer_stacks.push_back(stack);
    }
    locked_state->ready_to_write_.clear();

    // Now collect stacks from all active Contexts.
    //
    for (Context* context : locked_state->contexts_) {
      BlockBuffer* stack = context->consume_buffers();
      if (stack) {
        buffer_stacks.push_back(stack);
      }
    }
  }

  for (BlockBuffer* head : buffer_stacks) {
    BATT_CHECK_NOT_NULLPTR(head)
        << "The null-check in the loop above should have taken care of this!";

    // Disassemble each stack, adding the BlockBuffer objects to `updates`.
    //
    for (BlockBuffer* current = head; current != nullptr;) {
      BlockBuffer* const next = current->swap_next(nullptr);
      update_buffers.emplace_back(current);
      current = next;
    }
  }

  return update_buffers;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::writer_task_main() noexcept
{
  VLOG(1) << "entered ChangeLogWriter::writer_task_main";

  Status status = [this]() -> Status {
    // Use an RNG to select delay (with jitter) for polling.
    //
    std::default_random_engine rng{std::random_device{}()};

    BATT_CHECK_LE(this->options_.min_delay_usec, this->options_.max_delay_usec);

    std::uniform_int_distribution<i64> pick_delay_usec{this->options_.min_delay_usec,
                                                       this->options_.max_delay_usec};

    // The number of consecutive times `poll_updates()` has been called without returning any data
    // to write.
    //
    usize inactive_count = 0;

    // Set after each write to force a longer polling delay when collected buffers don't contain
    // enough data (see ChangeLogWriter::kMinBlockDensityTargetPct).
    //
    bool force_sleep = false;

    /*
      Writer task main loop (new; not yet implemented):

      Pipeline:

      State -> action -> State -> action -> ...

      Init:
       - batch <- empty batch
       - file_offset <- recovery upper bound

      1. poll per-thread Contexts to gather any writable blocks
      2. if we didn't get any (and there's no other queued data), sleep and goto 1
      3. move as much data as we can from collected blocks to the "to-write" batch
         - batch is limited by: a. end of file and b. max batch size (IOV_MAX)
      4. write as much of the batch as possible (async_write_some) -> (could result in short write)
      5. for all full written blocks, update the "edit_offset_upper_bound" table
         - this will allow the trim position (i.e. the active lower bound) to advance
      6. update file_offset + batch
         - apply wrap-around if needed
      7. loop back around to 1
     */

    for (;;) {
      batt::SmallVec<BlockBuffer*, 8> update_buffers =
          !force_sleep ? this->poll_updates() : batt::SmallVec<BlockBuffer*, 8>{};

      // If there are no updates, then sleep before polling again.
      //
      if (update_buffers.empty()) {
        inactive_count += 1;
        this->metrics_.sleep_count.add(1);

        // If we get in here, then we have no indication that there is any data available for
        // appending; in this case, enter our timed polling loop.
        //
        const i64 delay_usec = pick_delay_usec(rng);
        batt::Task::sleep(std::chrono::microseconds(delay_usec));

        if (this->halt_requested_.load()) {
          return batt::OkStatus();
        }

        // After allowing other tasks to run, we should immediately poll updates again to see if we
        // have more data.
        //
        continue;
      }

      VLOG(2) << "writer_task awakes!" << BATT_INSPECT(inactive_count);
      inactive_count = 0;

      BATT_ASSIGN_OK_RESULT(WriteOpStats stats, this->write_buffers(update_buffers));

      // Force a sleep if the collected buffers weren't full enough to hit the target density.
      //
      force_sleep = (stats.user_bytes_written * 100 <
                     Self::kMinBlockDensityTargetPct * stats.total_bytes_written);

      VLOG(2) << "done writing!  polling for more";
    }
  }();

  // If we are exiting cleanly, poll one more time and flush any buffers we find.
  //
  if (status.ok()) {
    status = this->write_buffers(this->poll_updates()).status();
  }

  if (VLOG_IS_ON(1) || (!status.ok() && !this->halt_requested_.load())) {
    LOG(INFO) << "ChangeLogWriter::writer_task exiting with status=" << status;
  }
}

#if 0  // TODO [tastolfi 2026-04-20] delete once refactor is done
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::write_buffers(const batt::SmallVecBase<BlockBuffer*>& update_buffers) noexcept
    -> StatusOr<WriteOpStats>
{
  batt::Grant grant = BATT_OK_RESULT_OR_PANIC(
      this->change_log_->reserve_blocks(BlockCount{0}, batt::WaitForResource::kFalse));

  // Don't release any buffers until we have appended as much data as we can.
  //
  auto on_scope_exit = batt::finally([&] {
    // After `direct_append` (below); now it is OK to free buffers.
    //
    ChangeLogWriter::remove_buffer_refs(update_buffers);
  });

  // Add all to the grant.
  //
  WriteOpStats stats;

  batt::SmallVec<ConstBuffer, 32> to_append;
  for (BlockBuffer* buffer : update_buffers) {
    BATT_CHECK_NOT_NULLPTR(buffer);
    BATT_REQUIRE_OK(buffer->verify());

    stats.user_bytes_written += buffer->slots_total_size();
    stats.total_bytes_written += buffer->block_size();

    grant.subsume(buffer->consume_grant());
    to_append.emplace_back(buffer->prepare_to_flush());
  }

  this->metrics_.received_block_byte_count.add(stats.total_bytes_written);
  this->metrics_.received_user_byte_count.add(stats.user_bytes_written);

  VLOG(2) << "have " << to_append.size() << " buffers to write;"
          << BATT_INSPECT(stats.user_bytes_written) << BATT_INSPECT(stats.total_bytes_written)
          << BATT_INSPECT((double)stats.user_bytes_written / (double)stats.total_bytes_written);

  // If we have some data to append to the WAL Volume, do it now.
  //
  if (!to_append.empty()) {
    BATT_CHECK_EQ(grant.size(), to_append.size());

    StatusOr<ChangeLogFile::ReadLock> read_lock = this->change_log_->append(grant, to_append);
    BATT_REQUIRE_OK(read_lock);

    this->metrics_.write_count.add(1);
    this->metrics_.written_block_byte_count.add(stats.total_bytes_written);
    this->metrics_.written_user_byte_count.add(stats.user_bytes_written);

    usize i = 0;
    for (BlockBuffer* buffer : update_buffers) {
      buffer->set_read_lock(read_lock->lock_subrange(i, 1));
      ++i;
    }
  }

  return {stats};
}
#endif

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::activate_blocks(WrittenBlocksState& input,
                                        ActiveBlocksState& output) noexcept
{
  if (input.blocks.empty()) {
    return OkStatus();
  }

  // Sanity checks.
  //
  input.check_invariants(this->config());
  output.check_invariants(this->config());
  check_invariants(this->config(), input, output);

  auto on_scope_exit = batt::finally([&] {
    input.check_invariants(this->config());
    output.check_invariants(this->config());
    check_invariants(this->config(), input, output);
    BATT_CHECK(input.blocks.empty());
  });

  // If the input is non-empty, the next block index must be set.
  //
  i64 block_i = input.block_index.value_or_panic();

  // We must consume the entire input.
  //
  while (!input.blocks.empty()) {
    BlockBuffer* const next_block = input.blocks.front();

    // Remove the first element and release the block each time we reach the end of the loop body.
    //
    auto on_loop_body_exit = batt::finally([&] {
      input.blocks.pop_front();
      next_block->remove_ref(1);
    });

    // Update active blocks edit offset upper bound.
    //
    output.block_upper_bounds[block_i] = next_block->edit_offset_upper_bound().value();

    // Transfer grant ownership to the in_use_block_grant.
    //
    output.in_use_block_grant.subsume(next_block->consume_grant());

    // Advance to the next block index, with wrap-around.
    //
    ++block_i;
    if (block_i == this->config().block_count) {
      block_i = 0;
    }
  }

  // Synchronize input.block_index and output.active_upper_bound_block with block_i.
  //
  input.block_index = BlockIndex{block_i};
  output.active_upper_bound_block = *input.block_index;

  if (output.active_upper_bound_block < output.active_lower_bound_block) {
    output.active_upper_bound_block =
        BlockIndex{output.active_upper_bound_block + this->config().block_count};
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::trim(EditOffset new_active_lower_bound)
{
  // Release any grant count after we exit the critical section below, to avoid double-locking.
  //
  Optional<batt::Grant> released_grant;

  const BlockCount max_block_count{this->config().block_count};
  {
    batt::ScopedLock<State> locked_state{this->state_};
    ActiveBlocksState& s = *locked_state->active_blocks_state_;

    // Sanity checks.
    //
    s.check_invariants(this->config());
    auto on_scope_exit = batt::finally([&] {
      s.check_invariants(this->config());
    });

    // Do nothing if the new trim offset isn't greater than the current one.
    //
    if (new_active_lower_bound <= s.active_edit_offset_lower_bound) {
      return OkStatus();
    }
    s.active_edit_offset_lower_bound = new_active_lower_bound;

    // Keep track of how many blocks are newly trimmed.
    //
    u64 n_trimmed = 0;

    // Step through the active blocks until we reach the end or find one whose upper bound is above
    // the trim offset.
    //
    while (s.active_lower_bound_block < s.active_upper_bound_block) {
      // Stop at the first block whose upper bound is after the trim offset.x
      //
      if (EditOffset{s.block_upper_bounds[s.active_lower_bound_block]} > new_active_lower_bound) {
        break;
      }

      ++n_trimmed;

      // Advance the active lower bound, with wrap-around.  We maintain the invariants:
      //
      //  - s.active_lower_bound_block < s.max_block_count
      //  - s.active_lower_bound_block <= s.active_upper_bound_block
      //
      s.active_lower_bound_block = BlockIndex{s.active_lower_bound_block + 1};
      if (s.active_lower_bound_block == max_block_count) {
        s.active_lower_bound_block = BlockIndex{0};
        s.active_upper_bound_block = BlockIndex{s.active_upper_bound_block - max_block_count};
      }
    }

    // Release any trimmed blocks.
    //
    if (n_trimmed != 0) {
      released_grant.emplace(BATT_OK_RESULT_OR_PANIC(s.in_use_block_grant.spend(n_trimmed)));
    }
  }
  return OkStatus();
}

}  // namespace turtle_kv
