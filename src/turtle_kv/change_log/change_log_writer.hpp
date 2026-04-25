//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_WRITER_HPP

#include <turtle_kv/change_log/api_types.hpp>
#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/change_log_file.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/util/small_queue.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/async/future.hpp>
#include <batteries/async/grant.hpp>
#include <batteries/async/latch.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/interval.hpp>

#include <concepts>
#include <ranges>

namespace turtle_kv {

class ChangeLogWriter
{
 public:
  /** \brief The default minimum delay (in microseconds) for the background task.
   */
  static constexpr i64 kDefaultMinDelayUsec = 1500;

  /** \brief The default maximum delay (in microseconds) for the background task.
   */
  static constexpr i64 kDefaultMaxDelayUsec = 2250;

  /** \brief The log2 of the number of bytes (in EditOffset) between block cluster breaks.
   */
  static constexpr i32 kBlockClusterLimitBits = 24;  // 2^24 bytes == 16MB

  /** \brief When the writer thread wakes, if it collects blocks which are *less* full than this
   * percentage, it will go back to sleep after writing them; otherwise, it will poll immediately
   * with no sleep.
   */
  static constexpr usize kMinBlockDensityTargetPct = 75;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Self = ChangeLogWriter;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using BlockBuffer = ChangeLogBlock;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief Run-time options to configure a ChangeLogWriter.
   */
  struct Options {
    /** \brief The minimum number of seconds to wait in the background task, if there are no buffers
     * ready to be appended. The actual delay time is (pseudo-)randomly chosen between this and
     * `max_delay_usec`; this jitter is used to prevent sleep/wake resonance and thundering-hurd
     * problems.
     */
    i64 min_delay_usec;

    /** \brief The maximum number of seconds to wait in the background task, if there are no buffers
     * ready to be appended. (see min_delay_usec)
     */
    i64 max_delay_usec;

    //----- --- -- -  -  -   -

    /** \brief Creates and returns an Options struct with default values.
     */
    static Options with_default_values() noexcept
    {
      return {
          ChangeLogWriter::kDefaultMinDelayUsec,
          ChangeLogWriter::kDefaultMaxDelayUsec,
      };
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Metrics {
    FastCountMetric<u64> received_block_byte_count{0};
    FastCountMetric<u64> received_user_byte_count{0};
    FastCountMetric<u64> written_block_byte_count{0};
    FastCountMetric<u64> written_user_byte_count{0};
    FastCountMetric<u64> poll_count{0};
    FastCountMetric<u64> sleep_count{0};
    FastCountMetric<u64> write_count{0};
    FastCountMetric<u64> block_rebase_count{0};
    DerivedMetric<double> block_utilization_rate{[this] {
      return (double)this->received_user_byte_count.load() /
             ((double)this->received_block_byte_count.load() + 1e-6);
    }};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief A per-thread/task context that can be used to write to the Volume.
   */
  class Context
  {
   public:
    using BlockBuffer = ChangeLogBlock;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Constructs a Context and adds it to the given Writer; this means the writer will
     * periodically poll the Context (lock-free) to try to "steal" stacks of buffers to append
     * to the Volume root log.
     */
    explicit Context(ChangeLogWriter& writer) noexcept;

    /** \brief Context is not copy/move-constructible.
     */
    Context(const Context&) = delete;

    /** \brief Context is not copy/move-assignable.
     */
    Context& operator=(const Context&) = delete;

    /** \brief Deconstructs the Context, removing it from its associated ChangeLogWriter.
     *
     * All Context objects MUST be destroyed before their ChangeLogWriter goes out of scope, or
     * behavior is undefined.
     */
    ~Context() noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief Returns a reference to the ChangeLogWriter associated with this Context.
     */
    ChangeLogWriter& writer() noexcept
    {
      return this->writer_;
    }

    /** \brief Remove the entire stack of BlockBuffer objects from this context.
     *
     * This function is thread-safe and lock-free.
     *
     * The returned BlockBuffer stack will be in FILO order.
     */
    BlockBuffer* consume_buffers() noexcept;

    /** \brief Appends the passed payload value as a new slot within some BlockBuffer owned by
     * this Context. \return the sequence number (index) of the newly formatted slot.
     */
    template <typename SerializeFn>
      requires std::
          invocable<SerializeFn&&, FirstVisitToBlock, BlockBuffer*, MutableBuffer, EditOffset>
        Status append_slot(EditOffset min_edit_offset_lower_bound,
                           usize byte_size,
                           batt::WaitForResource wait_for_resource,
                           SerializeFn&& fn) noexcept;

    /** \brief Appends the passed payload value as a new slot within some BlockBuffer owned by
     * this Context. \return the sequence number (index) of the newly formatted slot.
     */
    template <typename SerializeFn>
      requires std::
          invocable<SerializeFn&&, FirstVisitToBlock, BlockBuffer*, MutableBuffer, EditOffset>
        Status append_slot(EditOffset min_edit_offset_lower_bound,
                           usize byte_size,
                           SerializeFn&& fn) noexcept
    {
      return this->append_slot(min_edit_offset_lower_bound,
                               byte_size,
                               batt::WaitForResource::kTrue,
                               BATT_FORWARD(fn));
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    /** \brief Removes the top buffer on the stack; this function is thread-safe and lock-free.
     *
     * The observed `next` pointer of the returned buffer is stored in `observed_head`; this
     * pointer will be needed to call `push_buffer` when the caller is done modifying the popped
     * buffer.
     */
    BlockBuffer* pop_buffer(BlockBuffer*& observed_head) noexcept;

    /** \brief Places `buffer` on the top of the stack; this function is thread-safe and
     * lock-free.
     *
     * Upon success, buffer is set to nullptr and observed_head is updated (if necessary) to
     * point to the new head-of-stack, i.e., the passed-in value of buffer.
     */
    void push_buffer(BlockBuffer*& buffer, BlockBuffer*& observed_head) noexcept;

    //----- --- -- -  -  -   -

    /** \brief The writer associated with this Context.
     */
    ChangeLogWriter& writer_;

    /** \brief The top BlockBuffer on the stack.
     */
    std::atomic<BlockBuffer*> head_{nullptr};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<std::unique_ptr<ChangeLogWriter>> open_or_create(
      const std::filesystem::path& path,                      //
      const ChangeLogFile::Config& config,                    //
      const ChangeLogWriter::Options& options,                //
      RemoveExisting remove_existing = RemoveExisting{false}  //
      ) noexcept;

  static StatusOr<std::unique_ptr<ChangeLogWriter>> open(
      const std::filesystem::path& path,                       //
      Optional<ChangeLogWriter::Options> maybe_options = None  //
      ) noexcept;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new ChangeLogWriter.
   *
   * The ChangeLogWriter must be started by calling ChangeLogWriter::start().
   *
   * `active_block_range` and `active_blocks_upper_bounds` are derived from running recovery on the
   * ChangeLogFile.
   */
  explicit ChangeLogWriter(std::unique_ptr<ChangeLogFile>&& change_log,
                           const Options& options,
                           const Interval<BlockIndex>& active_block_range,
                           const Slice<EditOffset>& active_blocks_upper_bounds) noexcept;

  /** \brief Destructs a ChangeLogWriter.  All ChangeLogWriter::Context objects must be
   * destructed before the ChangeLogWriter is allowed to go out of scope.
   */
  ~ChangeLogWriter() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Metrics& metrics() noexcept
  {
    return this->metrics_;
  }

  /** \brief Returns the Config of the ChangeLogFile.
   */
  const ChangeLogFile::Config& config() const noexcept
  {
    return this->change_log_->config();
  }

  /** \brief Returns the options passed in at construction time.
   */
  const Options& options() const noexcept
  {
    return this->options_;
  }

  /** \brief Returns a reference to the ChangeLogFile.
   */
  ChangeLogFile& change_log_file() noexcept
  {
    return *this->change_log_;
  }

  /** \brief Spawns a background task to poll for new updates and write them to the Volume's
   * root log.  MUST only be called once!
   */
  void start(batt::Task::executor_type&& executor) noexcept;

  /** \brief Request shutdown of the ChangeLogWriter.  Safe to call multiple times; only has an
   * effect the first time.
   */
  void halt() noexcept;

  /** \brief Blocks the caller until the background task has finished.  Does NOT initiate
   * shutdown (see ChangeLogWriter::halt()).
   */
  void join() noexcept;

  /** \brief Returns the next available EditOffset (logical time stamp) for this log.
   */
  EditOffset next_edit_offset() const noexcept
  {
    return EditOffset{this->next_edit_offset_.load()};
  }

  /** \brief Advances the trim position (file/block offset), possibly moving Grant count from in-use
   * to available (which will unblock appenders).
   */
  Status trim(EditOffset new_active_lower_bound);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Writer Task pipeline stage states.
  //
  // "Active blocks" have been written and are not yet trimmed.  They contain the data that we must
  // *not* overwrite.

  // collect_blocks() -> CollectedBlocksState -> prepare_blocks()
  //
  struct CollectedBlocksState;

  // prepare_blocks() -> PreparedBlocksState -> write_blocks()
  //
  struct PreparedBlocksState;

  // write_blocks() -> WrittenBlocksState -> activate_blocks()
  //
  struct WrittenBlocksState;

  // activate_blocks() ->  ActiveBlocksState -> trim()

  struct ActiveBlocksState;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct State {
    std::vector<Context*> contexts_;
    std::vector<BlockBuffer*> ready_to_write_;

    /** \brief Both the background writer task and the public `trim` function need access to this.
     */
    std::unique_ptr<ActiveBlocksState> active_blocks_state_;

    //----- --- -- -  -  -   -

    void check_ready_to_shut_down() noexcept
    {
      BATT_CHECK(this->contexts_.empty()) << "All Context objects associated with a "
                                             "ChangeLogWriter MUST be destroyed before the "
                                             "ChangeLogWriter goes out of scope!";

      BATT_CHECK_EQ(this->ready_to_write_.size(), 0) << "BlockBuffer stacks must be released!";
    }

    ~State() noexcept;
  };

  struct BlockBufferStats {
    u64 user_bytes = 0;
    u64 total_bytes = 0;

    //----- --- -- -  -  -   -

    bool is_under_target() const noexcept
    {
      return this->user_bytes * 100 <
             this->total_bytes * ChangeLogWriter::kMinBlockDensityTargetPct;
    }

    BlockBufferStats operator+(const BlockBufferStats& other) const noexcept
    {
      return BlockBufferStats{
          .user_bytes = this->user_bytes + other.user_bytes,
          .total_bytes = this->total_bytes + other.total_bytes,
      };
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Removes the same count of references (`delta`) from each of the BlockBuffers in the
   * passed range (`range`).
   */
  template <typename BufferRange>
    requires std::ranges::range<BufferRange> &&
             std::assignable_from<BlockBuffer*&, std::ranges::range_value_t<BufferRange>>
  static void remove_buffer_refs(const BufferRange& range, i32 delta = 1) noexcept
  {
    for (BlockBuffer* buffer : range) {
      buffer->remove_ref(delta);
    }
  }

  /** \brief Panics if invariants do not hold between written_blocks and active_blocks.
   */
  static void check_invariants(const ChangeLogFile::Config& config,
                               const WrittenBlocksState& written_blocks,
                               const ActiveBlocksState& active_blocks) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief (Thread-safe) Adds an associated Context object.
   */
  void add_context(Context& context) noexcept;

  /** \brief (Thread-safe) Removes an associated Context object.
   */
  void remove_context(Context& context) noexcept;

  /** \brief Allocates and returns a new BlockBuffer of the configured size.  This function may
   * block waiting to acquire Grant from the Volume (i.e. Volume::reserve).
   */
  auto allocate_buffer(EditOffset edit_offset_lower_bound,
                       batt::WaitForResource wait_for_resource) noexcept -> StatusOr<BlockBuffer*>;

  /** \brief The background writer task; continuously polls all associated Contexts for new
   * data. When new data is found, it is merged in index-order and written in batches (as large
   * as possible) to the Volume, to optimize throughput.
   */
  void writer_task_main() noexcept;

  /** \brief Does a non-blocking check of all associated Contexts for any BlockBuffers that
   * might contain committed slot data.
   */
  Status collect_blocks(CollectedBlocksState& output) noexcept;

  /** \brief Sets new edit offset upper bounds for the passed blocks, transferring as many as
   * possible to this->write_buffer_.
   */
  StatusOr<BlockBufferStats> prepare_blocks(CollectedBlocksState& input,
                                            PreparedBlocksState& output) noexcept;

  /** \brief Writes a contiguous series of BlockBuffer data chunks to the file, then (based on the
   * number of bytes actually written by `IoRing::File::async_write_some`), transfers blocks that
   * have been *entirely* written to `output`.
   */
  StatusOr<BlockBufferStats> write_blocks(PreparedBlocksState& input,
                                          WrittenBlocksState& output) noexcept;

  /** \brief Takes blocks known to have been written and updates the active blocks state
   * accordingly. This means advancing `ActiveBlocksState::active_upper_bound_index`, copying each
   * block's edit_offset_upper_bound() to `ActiveBlocksState::block_upper_bounds`, and transferring
   * ownership of each block's Grant to `in_use_block_grant`.
   *
   * ChangeLogBlock (BlockBuffer) objects removed from `input.blocks` are released by decrementing
   * their ref count via `remove_ref`.
   */
  Status activate_blocks(WrittenBlocksState& input, ActiveBlocksState& output) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The state of the log file.
   */
  std::unique_ptr<ChangeLogFile> change_log_;

  /** \brief The configuration options passed in at construction time.
   */
  Options options_;

  batt::Grant::Issuer free_block_tokens_{
      BATT_CHECKED_CAST(u64, this->change_log_->config().block_count.value())};

  Metrics metrics_;

  /** \brief The next unassigned EditOffset.
   */
  std::atomic<i64> next_edit_offset_{
      0};  // TODO [tastolfi 2026-03-23] this must be set correctly in recovery!

  /** \brief Mutex-protected state for this object.
   */
  batt::Mutex<State> state_;

  const usize max_batch_size_ =
#if BATT_PLATFORM_IS_LINUX
      IOV_MAX;
#else
      2 * kMiB / this->config().block_size;
#endif

  /** \brief Set to true once-and-only-once when halt() is called the first time.
   */
  std::atomic<bool> halt_requested_{false};

  /** \brief The background writer task.
   */
  Optional<batt::Task> task_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

template <typename SerializeFn>
  requires std::
      invocable<SerializeFn&&, FirstVisitToBlock, ChangeLogBlock*, MutableBuffer, EditOffset>
    inline Status ChangeLogWriter::Context::append_slot(EditOffset min_edit_offset_lower_bound,
                                                        usize byte_size,
                                                        batt::WaitForResource wait_for_resource,
                                                        SerializeFn&& serialize_fn) noexcept
{
  Context& context = *this;
  ChangeLogWriter& writer = this->writer_;
  auto first_visit_to_block = FirstVisitToBlock{false};

  static constexpr i64 kBlockClusterMask =
      ~((i64{1} << ChangeLogWriter::kBlockClusterLimitBits) - 1);

  // Assign the EditOffset of the new slot.
  //
  const EditOffset slot_edit_offset{writer.next_edit_offset_.fetch_add(byte_size)};

  // Make sure there is a clean break between blocks every 2^kBlockClusterLimitBits.
  //
  min_edit_offset_lower_bound = EditOffset{
      std::max(min_edit_offset_lower_bound.value(),  //
               slot_edit_offset.value() & kBlockClusterMask),
  };

  // Grab a private buffer.
  //
  BlockBuffer* observed_head = nullptr;
  BlockBuffer* block_buffer = context.pop_buffer(observed_head);
  for (;;) {
    // Enforce the constraint that the Block buffer we pass to serialize_fn *must* have an
    // edit_offset_lower_bound at least as large as min_edit_offset_lower_bound.
    //
    if (block_buffer && block_buffer->edit_offset_lower_bound() < min_edit_offset_lower_bound) {
      context.push_buffer(block_buffer, observed_head);
      block_buffer = nullptr;
      writer.metrics_.block_rebase_count.add(1);
    }

    const bool no_buffer = (block_buffer == nullptr);

    // No buffer, no retry; there is no point attempting again if we had a fresh, empty buffer
    // to begin with.
    //
    const bool no_retry = no_buffer;

    // If no buffer, allocate one.
    //
    if (no_buffer) {
      VLOG(1) << "ChangeLogWriter::append_slot - allocating new block buffer...";
      BATT_CHECK_GE(slot_edit_offset, min_edit_offset_lower_bound);
      BATT_ASSIGN_OK_RESULT(block_buffer,
                            writer.allocate_buffer(slot_edit_offset, wait_for_resource));
      first_visit_to_block = FirstVisitToBlock{true};
      VLOG(1) << "ChangeLogWriter::append_slot - have new buffer!";
    }
    BATT_CHECK_NOT_NULLPTR(block_buffer);
    BATT_CHECK_GE(block_buffer->edit_offset_lower_bound(), min_edit_offset_lower_bound);
    BATT_CHECK_LE(block_buffer->edit_offset_lower_bound(), slot_edit_offset);

    // Serialize the payload.
    //
    const usize space_available = block_buffer->space();
    const usize space_needed = byte_size + sizeof(PackedEditOffsetDelta);

    Status status;
    usize bytes_to_commit = 0;

    if (space_needed <= space_available) {
      // Serialize the slot's edit offset delta at the beginning.
      //
      MutableBuffer slot_buffer = block_buffer->output_buffer(space_needed);

      BATT_REQUIRE_OK(BlockBuffer::write_slot_edit_offset_delta(
          slot_buffer,
          (slot_edit_offset - block_buffer->edit_offset_lower_bound()).to_slot_delta()));

      //----- --- -- -  -  -   -
      BATT_FORWARD(serialize_fn)(first_visit_to_block, block_buffer, slot_buffer, slot_edit_offset);
      //----- --- -- -  -  -   -

      bytes_to_commit = space_needed;
      status = OkStatus();

    } else {
      status = batt::StatusCode::kResourceExhausted;
    }

    {
      // When we leave this scope block, give the BlockBuffer back to the Context so it can be
      // (possibly) appended in the background.
      //
      auto on_scope_exit = batt::finally([&] {
        context.push_buffer(block_buffer, observed_head);
      });

      // If there was room in the buffer, then `format_slot` will succeed; assign this slot an
      // "index" (sequence number or logical time-stamp) and commit the newly copied data.
      // (Remember, the scope guard above will take care of giving the BlockBuffer back to the
      // Context)
      //
      if (status.ok()) {
        block_buffer->commit_slot(/*n_bytes=*/bytes_to_commit);
        return OkStatus();

      } else {
        VLOG(1) << "format_slot failed: " << status << BATT_INSPECT(no_buffer)
                << BATT_INSPECT(no_retry) << BATT_INSPECT(space_available)
                << BATT_INSPECT(byte_size) << BATT_INSPECT(space_needed);
      }
    }

    if (no_retry) {
      return status;
    }

    // Volume::format_slot only fails if there wasn't enough space; reset the buffer pointer and
    // retry (we will allocate a new buffer at the top of loop).
    //
    block_buffer = nullptr;
  }
}

}  // namespace turtle_kv
