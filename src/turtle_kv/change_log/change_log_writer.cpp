//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_writer.hpp>
//

#include <chrono>
#include <cstdlib>
#include <random>

namespace turtle_kv {

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
/*explicit*/ ChangeLogWriter::ChangeLogWriter(std::unique_ptr<ChangeLogFile>&& change_log,
                                              const Options& options) noexcept
    : change_log_{std::move(change_log)}
    , options_{options}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogWriter::~ChangeLogWriter() noexcept
{
  this->halt();
  this->join();

  ChangeLogWriter::remove_buffer_refs(this->poll_updates());

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

  return BlockBuffer::allocate(offset,
                               std::move(buffer_grant),
                               this->change_log_->config().block_size);
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

}  // namespace turtle_kv
