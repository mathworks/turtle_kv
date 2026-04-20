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
    const usize block_count = BATT_CHECKED_CAST(usize, config.block_count.value());

    BATT_CHECK_LT(this->active_lower_bound_block, config.block_count);

    BATT_CHECK_LE(this->active_lower_bound_block, this->active_upper_bound_block);

    BATT_CHECK_EQ(
        active_blocks_upper_bounds.size(),
        BATT_CHECKED_CAST(usize, this->active_upper_bound_block - this->active_lower_bound_block));

    BATT_CHECK_LE(active_blocks_upper_bounds.size(), block_count);

    for (usize src_i = 0, dst_i = this->active_lower_bound_block;
         src_i < active_blocks_upper_bounds.size();
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
/*explicit*/ ChangeLogWriter::PreparedBlocksState::PreparedBlocksState(
    const ChangeLogFile::Config& config,
    const ActiveBlocksState& active_blocks) noexcept
{
  BlockIndex next_block_to_write = active_blocks.active_upper_bound_block;
  if (next_block_to_write >= config.block_count) {
    next_block_to_write = BlockIndex{next_block_to_write - config.block_count};
  }
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

    BATT_CHECK(block_i == active_blocks.active_upper_bound_block ||
               block_i + config.block_count == active_blocks.active_upper_bound_block)
        << "The WrittenBlocksState::block_index must agree with "
           "ActiveBlocksState::active_upper_bound_block!"
        << BATT_INSPECT(block_i) << BATT_INSPECT(active_blocks.active_upper_bound_block)
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

  // TODO [tastolfi 2026-04-20] pass real values for active blocks params!
  //
  return {std::make_unique<ChangeLogWriter>(std::move(log_file),
                                            options,
                                            make_interval(BlockIndex{0}, BlockIndex{0}),
                                            Slice<EditOffset>{})};
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

  // TODO [tastolfi 2026-04-20] pass real values for active blocks params!
  //
  return {std::make_unique<ChangeLogWriter>(std::move(log_file),
                                            options,
                                            make_interval(BlockIndex{0}, BlockIndex{0}),
                                            Slice<EditOffset>{})};
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
auto ChangeLogWriter::allocate_buffer(EditOffset offset) noexcept -> StatusOr<BlockBuffer*>
{
  BATT_ASSIGN_OK_RESULT(batt::Grant buffer_grant,
                        this->free_block_tokens_.issue_grant(1, batt::WaitForResource::kTrue));

  this->change_log_->metrics().reserved_blocks_count.add(buffer_grant.size());

  return BlockBuffer::allocate(offset, std::move(buffer_grant), this->config().block_size);
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

    for (;;) {
      // Collect BlockBuffers from writer contexts.
      //
      if (!force_sleep) {
        BATT_REQUIRE_OK(this->collect_blocks(collected));
      }

      BATT_ASSIGN_OK_RESULT(BlockBufferStats prepare_stats,
                            this->prepare_blocks(collected, prepared));

      // If there are no updates, then sleep before polling again (unless halt requested).
      //
      if ((force_sleep || prepared.empty()) && this->halt_requested_.load() == false) {
        force_sleep = false;
        inactive_count += 1;
        this->metrics_.sleep_count.add(1);

        // If we get in here, then we have no indication that there is any data available for
        // appending; in this case, enter our timed polling loop.
        //
        const i64 delay_usec = pick_delay_usec(rng);
        batt::Task::sleep(std::chrono::microseconds(delay_usec));

        // After allowing other tasks to run, we should immediately poll updates again to see if we
        // have more data.
        //
        continue;
      }

      VLOG(2) << "writer_task awakes!" << BATT_INSPECT(inactive_count);
      inactive_count = 0;

      BATT_ASSIGN_OK_RESULT(BlockBufferStats write_stats, this->write_blocks(prepared, written));

      const BlockBufferStats block_stats = prepare_stats + write_stats;

      // Force a sleep if the collected buffers weren't full enough to hit the target density.
      //
      force_sleep = block_stats.is_under_target();

      // "Activate" the written blocks by adding them to the active blocks state; this allows
      // accurate trimming and reclamation of storage resources.
      {
        batt::ScopedLock<State> locked_state{this->state_};
        BATT_REQUIRE_OK(this->activate_blocks(written, *locked_state->active_blocks_state_));
      }

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
         output.block_chunks.size() < this->max_batch_size_) {
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

  BATT_CHECK_LT(input.block_chunks.size(), this->max_batch_size_) << "Too many prepared blocks!";

  // Write!
  //
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
          BATT_CHECK_EQ(*output.block_index, written_block_index) << "";
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
      // Stop at the first block whose upper bound is after the trim offset.
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::State::~State() noexcept
{
  this->check_ready_to_shut_down();
}

}  // namespace turtle_kv
