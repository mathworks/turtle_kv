//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_writer.hpp>
//

#include <turtle_kv/change_log/change_log_reader.hpp>

#include <turtle_kv/util/small_queue.hpp>

#include <batteries/do_nothing.hpp>

#include <chrono>
#include <cstdlib>
#include <random>

namespace turtle_kv {

constexpr usize kStaticQueueSize = 16;

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void release_blocks(SmallQueueBase<ChangeLogBlock*>* blocks) noexcept
{
  for (ChangeLogBlock* p_block : *blocks) {
    p_block->remove_ref(1);
  }
  blocks->clear();
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
    release_blocks(&this->blocks);
  }

  bool empty() const noexcept
  {
    return this->blocks.empty();
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

  explicit PreparedBlocksState(const ChangeLogFile::Config& config,
                               const ActiveBlocksState& active_blocks) noexcept;

  ~PreparedBlocksState() noexcept
  {
    release_blocks(&this->blocks);
  }

  bool empty() const noexcept
  {
    BATT_CHECK_EQ(this->block_chunks.empty(), this->blocks.empty());
    return this->block_chunks.empty();
  }

  void check_invariants(const ChangeLogFile::Config& config) const noexcept
  {
    BATT_CHECK_LT(this->file_offset, config.last_block_end_offset())
        << "file_offset must be before the end of the last block!";

    BATT_CHECK_EQ(this->block_chunks.size(), this->blocks.size())
        << "The elements of block_chunks and blocks must line up!";

    if (!this->block_chunks.empty()) {
      BATT_CHECK_GT(this->block_chunks.front().size(), 0)
          << "Empty chunks must be removed from the front of the queue!";
    }
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
    release_blocks(&this->blocks);
  }

  bool empty() const noexcept
  {
    return this->blocks.empty();
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
struct ChangeLogWriter::AdvanceSyncState {
  /** \brief Visits all slots in all flushed blocks, to track the current durable 'sync' upper
   * bound EditOffset.
   */
  ChangeLogBlocksVisitor visitor;

  //----- --- -- -  -  -   -

  AdvanceSyncState() = delete;

  explicit AdvanceSyncState(EditOffset recovered_upper_bound) noexcept
      : visitor{recovered_upper_bound}
  {
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ChangeLogWriter::ActiveBlocksState : ChangeLogFile::MetaState {
  using Super = ChangeLogFile::MetaState;

  //----- --- -- -  -  -   -

  /** \brief For each block index in the file, the known upper bound edit offset of slots in that
   * block.
   */
  std::unique_ptr<i64[]> block_upper_bounds;

  /** \brief When blocks are transferred from `WrittenBlocksState` to `ActiveBlocksState`, we move
   * their grant form the BlockBuffer to this shared grant pool.
   */
  batt::Grant in_use_block_grant;

  //----- --- -- -  -  -   -

  /** \brief Construct the ActiveBlockState from recovered log state.
   */
  explicit ActiveBlocksState(const ChangeLogFile::Config& config,
                             batt::Grant::Issuer& block_grant_pool,
                             const RecoveredChangeLogState& recovered_state) noexcept
      : Super{static_cast<const ChangeLogMetaState&>(recovered_state)}
      , block_upper_bounds{new i64[config.block_count]}
      , in_use_block_grant{
            BATT_OK_RESULT_OR_PANIC(block_grant_pool.issue_grant(0, batt::WaitForResource::kFalse))}
  {
    BATT_CHECK_EQ(config.block_count, BATT_CHECKED_CAST(i64, block_grant_pool.available()));

    config.check_invariants(this->block_range);

    BATT_CHECK_EQ(recovered_state.active_blocks_upper_bounds.size(),
                  (usize)this->block_range.size());

    Interval<BlockIndex> uninitialized = this->block_range;

    for (const EditOffset& block_upper_bound : recovered_state.active_blocks_upper_bounds) {
      this->block_upper_bounds[uninitialized.lower_bound] = block_upper_bound.value();
      config.increment_lower_bound(uninitialized);
    }

    this->in_use_block_grant.subsume(
        BATT_OK_RESULT_OR_PANIC(block_grant_pool.issue_grant(recovered_state.block_range.size(),
                                                             batt::WaitForResource::kFalse)));

    {
      BATT_DEBUG_INFO(
          "RecoveredChangeLogState::trim_edit_offset should be consistent with the active block "
          "range when passed to ActiveBlocksState::ActiveBlocksState");

      const auto trim_before = this->trim_edit_offset;
      const auto block_range_before = this->block_range;

      Optional<batt::Grant> released_grant =
          this->apply_trim(recovered_state.trim_edit_offset, config);

      BATT_CHECK_EQ(released_grant, None);
      BATT_CHECK_EQ(this->trim_edit_offset, trim_before);
      BATT_CHECK_EQ(this->block_range, block_range_before);
    }
  }

  /** \brief Panics if the current state of `this` violates any invariants with respect to the
   * passed Config.
   */
  void check_invariants(const ChangeLogFile::Config& config) const noexcept
  {
    config.check_invariants(this->block_range);
    BATT_CHECK_EQ((usize)this->block_range.size(), in_use_block_grant.size())
        << "in_use_block_grant must exactly cover the active block interval";
  }

  /** \brief Updates this->trim_edit_offset, this->block_range, and the in_use_block_grant.
   *
   * \return Grant for any newly trimmable blocks
   */
  Optional<batt::Grant> apply_trim(EditOffset new_trim_edit_offset,
                                   const ChangeLogFile::Config& config) noexcept;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogWriter::PreparedBlocksState::PreparedBlocksState(
    const ChangeLogFile::Config& config,
    const ActiveBlocksState& active_blocks) noexcept
{
  BlockIndex next_block_to_write = config.wrapped_upper_bound(active_blocks.block_range);
  this->file_offset = config.block_offset_from_index(next_block_to_write);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void ChangeLogWriter::check_invariants(const ChangeLogFile::Config& config,
                                                  const WrittenBlocksState& written_blocks,
                                                  const ActiveBlocksState& active_blocks) noexcept
{
  if (written_blocks.block_index.has_value()) {
    const i64 block_i = *written_blocks.block_index;

    BATT_CHECK(block_i == active_blocks.block_range.upper_bound ||
               block_i + config.block_count == active_blocks.block_range.upper_bound)
        << "The WrittenBlocksState::block_index must agree with "
           "ActiveBlocksState::active_upper_bound_block!"
        << BATT_INSPECT(block_i) << BATT_INSPECT(active_blocks.block_range.upper_bound)
        << BATT_INSPECT(config.block_count);
  }
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
    const std::filesystem::path& path,
    const ChangeLogFile::Config& config,
    const ChangeLogWriter::Options& options,
    RemoveExisting remove_existing,
    const Optional<RecoveredChangeLogState>& recovered_state) noexcept
{
  std::error_code ec;
  if (remove_existing || !std::filesystem::exists(path, ec) || ec) {
    BATT_REQUIRE_OK(ChangeLogFile::create(path, config, remove_existing));
  }
  BATT_REQUIRE_OK(ec);

  return ChangeLogWriter::open(path, options, recovered_state);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<ChangeLogWriter>> ChangeLogWriter::open(
    const std::filesystem::path& path,
    Optional<ChangeLogWriter::Options> options,
    Optional<RecoveredChangeLogState> recovered_state) noexcept
{
  if (!options) {
    options = Options::with_default_values();
  }

  BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

  if (!recovered_state) {
    // Create a temporary, non-owning reader to recover the state.
    //
    ChangeLogReader reader{log_file.get()};
    BATT_ASSIGN_OK_RESULT(recovered_state, reader.recover_state());
  }

  return {std::make_unique<ChangeLogWriter>(std::move(log_file), *options, *recovered_state)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogWriter::ChangeLogWriter(
    std::unique_ptr<ChangeLogFile>&& change_log,
    const Options& options,
    const RecoveredChangeLogState& recovered_state) noexcept
    : change_log_{std::move(change_log)}
    , options_{options}
    , free_block_tokens_{BATT_CHECKED_CAST(u64, this->change_log_->config().block_count.value())}
    , metrics_{}
    , next_edit_offset_{recovered_state.next_edit_offset.value()}
    , sync_upper_bound_{recovered_state.next_edit_offset.value()}
{
  // Initialize the meta-block buffer to reflect the on-disk state.
  //
  std::memset(&this->meta_block_buffer_, 0, sizeof(ChangeLogFile::PackedMetaBlock));
  this->config().pack_to(&this->meta_block_buffer_.config);
  recovered_state.ChangeLogMetaState::pack_to(&this->meta_block_buffer_.meta_state);

  {
    batt::ScopedLock<State> locked_state{this->state_};
    locked_state->active_blocks_state_ =
        std::make_unique<ActiveBlocksState>(this->config(),
                                            this->free_block_tokens_,
                                            recovered_state);
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
  this->sync_upper_bound_.close();
  this->halt_requested_.store(true);
  this->free_block_tokens_.close();
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
auto ChangeLogWriter::allocate_buffer(batt::WaitForResource wait_for_resource) noexcept
    -> StatusOr<BlockBuffer*>
{
  BATT_ASSIGN_OK_RESULT(batt::Grant buffer_grant,
                        this->free_block_tokens_.issue_grant(1, wait_for_resource));

  this->change_log_->metrics().reserved_blocks_count.add(buffer_grant.size());

  return BlockBuffer::allocate(std::move(buffer_grant), this->config().block_size);
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

    CollectedBlocksState collected;
    PreparedBlocksState prepared{this->config(), *this->state_.lock()->active_blocks_state_};
    WrittenBlocksState written;
    AdvanceSyncState synced{EditOffset{this->sync_upper_bound_.get_value()}};

    for (;;) {
      // Collect BlockBuffers from writer contexts.
      //
      if (!force_sleep) {
        BATT_REQUIRE_OK(this->collect_blocks(collected));
      }

      BATT_ASSIGN_OK_RESULT(BlockBufferStats prepare_stats,
                            this->prepare_blocks(collected, prepared));

      // If there are no updates, then sleep before polling again (unless halt requested or
      // there is pending urgent work).
      //
      if ((force_sleep || prepared.empty()) && this->halt_requested_.load() == false &&
          !this->has_pending_urgent_sync_work()) {
        force_sleep = false;
        inactive_count += 1;
        this->metrics_.sleep_count.add(1);

        // If we get in here, then we have no indication that there is any data available for
        // appending; in this case, enter our timed polling loop.
        //
        const i64 delay_usec = pick_delay_usec(rng);
        [[maybe_unused]] auto ec = batt::Task::sleep(std::chrono::microseconds(delay_usec));

        // After allowing other tasks to run, we should immediately poll updates again to see if we
        // have more data.
        //
        continue;
      }

      VLOG(2) << "writer_task awakes!" << BATT_INSPECT(inactive_count);
      inactive_count = 0;

      BATT_ASSIGN_OK_RESULT(BlockBufferStats write_stats, this->write_blocks(prepared, written));

      const BlockBufferStats block_stats = prepare_stats + write_stats;

      // "Activate" the written blocks by adding them to the active blocks state; this allows
      // accurate trimming and reclamation of storage resources.
      //
      batt::SmallVec<boost::intrusive_ptr<ChangeLogBlock>, kStaticQueueSize> newly_activated;
      {
        batt::ScopedLock<State> locked_state{this->state_};
        BATT_REQUIRE_OK(
            this->activate_blocks(written, *locked_state->active_blocks_state_, newly_activated));
      }

      // Advance the durable upper bound with the newly activated blocks.
      //
      this->advance_sync_upper_bound(newly_activated, synced);

      // Force a sleep if the collected buffers weren't full enough to hit the target density,
      // unless there is pending urgent work.
      //
      force_sleep = block_stats.is_under_target() && !this->has_pending_urgent_sync_work();

      // If halt is requested and we don't appear to be making any progress, then return.
      //
      if (this->halt_requested_.load()     //
          && block_stats.total_bytes == 0  //
          && collected.empty()             //
          && prepared.empty()) {
        return OkStatus();
      }

      VLOG(2) << "done writing!  polling for more";
    }
  }();

  if (VLOG_IS_ON(1) || (!status.ok() && !this->halt_requested_.load())) {
    LOG(INFO) << "ChangeLogWriter::writer_task exiting with status=" << status;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::collect_blocks(CollectedBlocksState& output) noexcept
{
  this->metrics_.poll_count.add(1);

  batt::SmallVec<BlockBuffer*, kStaticQueueSize> buffer_stacks;
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
      output.blocks.push_back(current);
      current = next;
    }
  }

  // TODO [tastolfi 2026-05-05] reverse each thread's stack, then collect in round-robin order
  //  std::reverse(output.blocks.begin(), output.blocks.end());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::prepare_blocks(CollectedBlocksState& input,
                                     PreparedBlocksState& output) noexcept
    -> StatusOr<BlockBufferStats>
{
  BlockBufferStats stats;

  if (input.blocks.empty()) {
    return {stats};
  }

  // Sanity checks.
  //
  output.check_invariants(this->config());

  auto on_scope_exit = batt::finally([&] {
    output.check_invariants(this->config());
  });

  // Keep track of how much space there is at the current write offset (output.file_offset), so we
  // don't run past the end of the last block.
  //
  i64 space_in_file = this->config().last_block_end_offset() - output.file_offset;

  // Add as many blocks as we can.
  //
  while (!input.blocks.empty() && space_in_file >= this->config().block_size &&
         output.block_chunks.size() < this->config().max_write_batch_size()) {
    //----- --- -- -  -  -   -
    BlockBuffer* const next_block = input.blocks.front();

    BATT_CHECK_EQ(next_block->block_size(), (usize)this->config().block_size);

    space_in_file -= this->config().block_size;

    stats.user_bytes += next_block->slots_total_size();
    stats.total_bytes += next_block->block_size();

    output.blocks.push_back(next_block);
    output.block_chunks.push_back(next_block->prepare_to_flush());
    input.blocks.pop_front();
  }

  this->metrics_.received_user_byte_count.add(stats.user_bytes);
  this->metrics_.received_block_byte_count.add(stats.total_bytes);

  return {stats};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogWriter::write_blocks(PreparedBlocksState& input, WrittenBlocksState& output) noexcept
    -> StatusOr<BlockBufferStats>
{
  // Sanity checks.
  //
  input.check_invariants(this->config());
  output.check_invariants(this->config());

  auto on_scope_exit = batt::finally([&] {
    input.check_invariants(this->config());
    output.check_invariants(this->config());
  });

  BATT_CHECK_LE(input.block_chunks.size(), this->config().max_write_batch_size())
      << "Too many prepared blocks!";

  // Write!
  //
  this->metrics_.write_count.add(1);
  BATT_ASSIGN_OK_RESULT(  //
      i32 n_written,
      batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
        this->change_log_->file().async_write_some(input.file_offset,
                                                   batt::as_slice(input.block_chunks),
                                                   BATT_FORWARD(handler));
      }));

  // Collect stats on the blocks that were written.
  //
  BlockBufferStats stats;

  // Consume however many bytes were written.
  //
  while (n_written > 0) {
    BATT_CHECK(!input.block_chunks.empty())
        << "More bytes written than were passed to async_write_some!";

    ConstBuffer& next_chunk = input.block_chunks.front();
    const usize n_to_consume = std::min<usize>(next_chunk.size(), n_written);

    next_chunk += n_to_consume;
    n_written -= n_to_consume;
    input.file_offset = FileOffset{input.file_offset + BATT_CHECKED_CAST(i64, n_to_consume)};

    BATT_CHECK_LE(input.file_offset, this->config().last_block_end_offset())
        << "Wrote past the end of the last block!";

    // Wrap-around at the end of the file.
    //
    if (input.file_offset == this->config().last_block_end_offset()) {
      input.file_offset = this->config().block0_offset;

      BATT_CHECK_EQ(n_written, 0)
          << "write_blocks should never write data that wraps around to the file start!";
    }

    // When the prepared chunk at the front of the input is fully consumed, remove it and transfer
    // its BlockBuffer to the output state.
    //
    if (next_chunk.size() == 0) {
      BlockBuffer* const block = input.blocks.front();

      stats.user_bytes += block->slots_total_size();
      stats.total_bytes += block->block_size();

      if (output.blocks.empty()) {
        const BlockIndex written_block_index =
            this->config().block_index_from_end_offset(input.file_offset);

        // Update or verify the output block index.
        //
        if (output.block_index.has_value()) {
          BATT_CHECK_EQ(*output.block_index, written_block_index)
              << BATT_INSPECT(input.file_offset);
        } else {
          output.block_index = written_block_index;
        }
      }

      // No need to touch the block's ref count; just push, pop and keep going.
      //
      output.blocks.push_back(block);
      input.blocks.pop_front();
      input.block_chunks.pop_front();
    }
  }

  this->metrics_.written_user_byte_count.add(stats.user_bytes);
  this->metrics_.written_block_byte_count.add(stats.total_bytes);

  return {stats};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::activate_blocks(
    WrittenBlocksState& input,
    ActiveBlocksState& output,
    batt::SmallVecBase<boost::intrusive_ptr<ChangeLogBlock>>& newly_activated) noexcept
{
  if (input.blocks.empty()) {
    return OkStatus();
  }

  const ChangeLogFile::Config& cfg = this->config();

  // Sanity checks.
  //
  input.check_invariants(cfg);
  output.check_invariants(cfg);
  check_invariants(cfg, input, output);

  VLOG(1) << "ChangeLogWriter::activate_blocks entered --"
          << BATT_INSPECT(output.in_use_block_grant.size());

  auto on_scope_exit = batt::finally([&] {
    input.check_invariants(cfg);
    output.check_invariants(cfg);
    check_invariants(cfg, input, output);
    BATT_CHECK(input.blocks.empty());

    VLOG(1) << "ChangeLogWriter::activate_blocks returned--"
            << BATT_INSPECT(output.in_use_block_grant.size());
  });

  // We must consume the entire input.
  //
  while (!input.blocks.empty()) {
    BlockBuffer* const next_block = input.blocks.front();

    auto on_loop_body_exit = batt::finally([&] {
      // Remove the first element and release the block each time we reach the end of the loop body.
      //
      input.blocks.front() = nullptr;
      input.blocks.pop_front();
      next_block->remove_ref(1);

      // Advance to the next block index, with wrap-around.
      //
      cfg.increment_with_wrap(*input.block_index);
    });

    // IMPORTANT: consume the block grant before doing anything else, so we don't leak the block.
    //
    batt::Grant block_grant = next_block->consume_grant(*input.block_index);
    BATT_CHECK_EQ(block_grant.size(), 1);

    VLOG(1) << BATT_INSPECT(output.block_range) << BATT_INSPECT(output.trim_edit_offset)
            << BATT_INSPECT(next_block->edit_offset_lower_bound())
            << BATT_INSPECT(next_block->edit_offset_upper_bound());

    // If there are no active blocks and the trim point is already past the next block, just
    // increment the block range and keep going.
    //
    if (output.block_range.empty() &&
        output.trim_edit_offset >= next_block->edit_offset_upper_bound()) {
      VLOG(1) << "discarding already-trimmed block that was just written;"
              << BATT_INSPECT(output.trim_edit_offset) << BATT_INSPECT(output.block_range)
              << BATT_INSPECT(next_block->edit_offset_lower_bound())
              << BATT_INSPECT(next_block->edit_offset_upper_bound());
      cfg.increment_block_range(output.block_range);
      BATT_CHECK(output.block_range.empty()) << BATT_INSPECT(output.block_range);
      continue;
    }

    // Collect blocks with slots for advancing the durable upper bound.
    //
    if (next_block->slot_count() > 0) {
      newly_activated.emplace_back(next_block);
    }

    // Update active blocks edit offset upper bound.
    //
    output.block_upper_bounds[*input.block_index] = next_block->edit_offset_upper_bound().value();

    // Transfer grant ownership to the in_use_block_grant.
    //
    output.in_use_block_grant.subsume(std::move(block_grant));

    // No wrap-around for active_upper_bound_block, because it must stay >= the lower bound.
    //
    cfg.increment_upper_bound(output.block_range);
  }

  BATT_REQUIRE_OK(this->refresh_meta_block(output));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::trim(EditOffset new_trim_edit_offset)
{
  VLOG(1) << "ChangeLogWriter::trim(" << new_trim_edit_offset << ")";

  // Release any grant count after we exit the critical section below, to avoid double-locking.
  //
  Optional<batt::Grant> released_grant;

  const ChangeLogFile::Config& cfg = this->config();
  {
    batt::ScopedLock<State> locked_state{this->state_};
    ActiveBlocksState& active_blocks = *locked_state->active_blocks_state_;

    released_grant = active_blocks.apply_trim(new_trim_edit_offset, cfg);

    // Refresh the meta-block.
    //
    BATT_REQUIRE_OK(this->refresh_meta_block(active_blocks));
    //
    // VERY IMPORTANT: the meta-block must be written with the new trim value before we release
    // grant, allowing writers to overwrite trimmed blocks.
  }
  return OkStatus();
  //
  // release_grant is destructed, releasing block tokens for re-use.
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<batt::Grant> ChangeLogWriter::ActiveBlocksState::apply_trim(
    EditOffset new_trim_edit_offset,
    const ChangeLogFile::Config& cfg) noexcept
{
  VLOG(1) << "trim(" << new_trim_edit_offset << ")";

  // Sanity checks.
  //
  this->check_invariants(cfg);
  auto on_scope_exit = batt::finally([&] {
    this->check_invariants(cfg);
  });

  this->trim_edit_offset = std::max(this->trim_edit_offset, new_trim_edit_offset);

  // Keep track of how many blocks are newly trimmed.
  //
  u64 n_trimmed = 0;

  // Step through the active blocks until we reach the end or find one whose upper bound is above
  // the trim offset.
  //
  while (!this->block_range.empty()) {
    // Stop at the first block whose upper bound is after the trim offset.
    //
    if (EditOffset{this->block_upper_bounds[this->block_range.lower_bound]} >
        this->trim_edit_offset) {
      break;
    }

    ++n_trimmed;

    VLOG(1) << "Trimmed block at " << this->block_range.lower_bound << "; edit_offset_upper_bound="
            << this->block_upper_bounds[this->block_range.lower_bound];

    // Advance the active lower bound, with wrap-around.
    //
    cfg.increment_lower_bound(this->block_range);
  }

  VLOG(1) << BATT_INSPECT(n_trimmed);

  // Release any trimmed blocks.  IMPORTANT: we release the in use grant all at once rather than
  // one block at a time, so that any clients blocked waiting for space won't immediately run out
  // of space.
  //
  Optional<batt::Grant> released_grant;
  if (n_trimmed != 0) {
    released_grant.emplace(BATT_OK_RESULT_OR_PANIC(this->in_use_block_grant.spend(n_trimmed)));
  }

  VLOG(1) << BATT_INSPECT(this->in_use_block_grant.size());

  return released_grant;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::sync(EditOffset upper_bound, bool urgent) noexcept
{
  // Return early if upper bound is already synced.
  //
  if (this->sync_upper_bound_.get_value() >= upper_bound.value()) {
    return OkStatus();
  }

  if (urgent) {
    this->urgent_sync_counter_.fetch_add(1);
  }

  auto on_exit = batt::finally([&] {
    if (urgent) {
      this->urgent_sync_counter_.fetch_sub(1);
    }
  });

  if (this->task_) {
    this->task_->wake();
  }

  // Block until the writer main task advances sync_upper_bound_ past our target.
  //
  BATT_ASSIGN_OK_RESULT([[maybe_unused]] const i64 observed,
                        this->sync_upper_bound_.await_true([upper_bound](i64 observed) {
                          return observed >= upper_bound.value();
                        }));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 ChangeLogWriter::get_unflushed_byte_count() const noexcept
{
  const i64 count =
      std::max<i64>(0, this->next_edit_offset_.load() - this->sync_upper_bound_.get_value());
  return count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool ChangeLogWriter::has_pending_urgent_sync_work() const noexcept
{
  return this->urgent_sync_counter_.load() > 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogWriter::advance_sync_upper_bound(
    batt::SmallVecBase<boost::intrusive_ptr<ChangeLogBlock>>& newly_activated,
    AdvanceSyncState& sync_state) noexcept
{
  LatencyTimer timer{Every2ToTheConst<0>{}, this->metrics_.advance_sync_upper_bound_latency};

  BATT_CHECK_EQ(sync_state.visitor.visited_upper_bound(),
                EditOffset{this->sync_upper_bound_.get_value()})
      << "The ChangeLogWriter::write_task_main thread must be the only modifier of "
         "sync_upper_bound_!";

  for (auto& block_ptr : newly_activated) {
    sync_state.visitor.add_block(std::move(block_ptr));
  }

  sync_state.visitor.visit_change_log_blocks(batt::DoNothing{});
  //
  // Nothing to do with the visited blocks; we just care about the new visited upper bound.

  this->sync_upper_bound_.set_value(sync_state.visitor.visited_upper_bound().value());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogWriter::refresh_meta_block(ActiveBlocksState& active_blocks) noexcept
{
  active_blocks.ChangeLogMetaState::pack_to(&this->meta_block_buffer_.meta_state);
  BATT_REQUIRE_OK(this->change_log_file().write_meta_block(this->meta_block_buffer_));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::BlockStats ChangeLogWriter::get_block_stats() noexcept
{
  batt::ScopedLock<State> locked_state{this->state_};

  BlockStats stats;

  stats.total_count = this->config().block_count;
  stats.active_count = locked_state->active_blocks_state_->in_use_block_grant.size();
  stats.free_count = this->free_block_tokens_.available();
  stats.reserved_count = stats.total_count - (stats.active_count + stats.free_count);

  return stats;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::State::~State() noexcept
{
  this->check_ready_to_shut_down();
}

}  // namespace turtle_kv
