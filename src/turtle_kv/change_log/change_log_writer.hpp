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
#include <turtle_kv/change_log/change_log_blocks_visitor.hpp>
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

#include <chrono>
#include <concepts>
#include <ranges>
#include <thread>

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
    LatencyMetric advance_sync_upper_bound_latency;
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
     * this Context.
     *
     * \param min_edit_offset_lower_bound Constrains the EditOffset range of the ChangeLogBlock to
     *    which the new slot is written so that:
     *      block->edit_offset_lower_bound() >= min_edit_offset_lower_bound
     * \param byte_size The size of the slot to be appended
     * \param wait_for_resource Controls whether this call should wait until there is space in
     *    the log to satisfy the request, or just fail immediately (with
     *     batt::StatusCode::kGrantUnavailable).
     * \param fn Invoked with the block buffer, the slot MutableBuffer, and the EditOffset; must
     *    write slot payload data to the MutableBuffer it receives.
     *
     * \return OkStatus() on success or batt::StatusCode::kGrantUnavailable if the log is full and
     *    wait_for_resource is `batt::WaitForResource::kFalse`
     */
    template <typename SerializeFn>
      requires std::
          invocable<SerializeFn&&, FirstVisitToBlock, BlockBuffer*, MutableBuffer, EditOffset>
        Status append_slot(EditOffset min_edit_offset_lower_bound,
                           usize byte_size,
                           batt::WaitForResource wait_for_resource,
                           SerializeFn&& fn) noexcept;

    /** \brief Calls `this->append_slot` with `wait_for_resource=batt::WaitForResource::kTrue`.
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
  //
  struct BlockStats {
    i64 total_count;
    i64 free_count;
    i64 reserved_count;
    i64 active_count;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static StatusOr<std::unique_ptr<ChangeLogWriter>> open_or_create(
      const std::filesystem::path& path,
      const ChangeLogFile::Config& config,
      const ChangeLogWriter::Options& options,
      RemoveExisting remove_existing = RemoveExisting{false},
      const Optional<RecoveredChangeLogState>& recovered_state = None) noexcept;

  static StatusOr<std::unique_ptr<ChangeLogWriter>> open(
      const std::filesystem::path& path,
      Optional<ChangeLogWriter::Options> options = None,
      Optional<RecoveredChangeLogState> recovered_state = None) noexcept;

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
                           const RecoveredChangeLogState& recovered_state) noexcept;

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

  /** \brief Returns the number of blocks in each state: free, reserved, active.
   */
  BlockStats get_block_stats() noexcept;

  [[nodiscard]] bool wait_for_flush(
      std::chrono::milliseconds poll_interval_ms = std::chrono::milliseconds(1),
      usize max_poll_cycles = 3000) noexcept
  {
    for (usize cycle_i = 0; cycle_i < max_poll_cycles; ++cycle_i) {
      BlockStats stats = this->get_block_stats();

      if (stats.reserved_count == 0) {
        return true;
      }

      if (batt::Task::sleep(poll_interval_ms)) {
        break;
      }
    }
    return false;
  }

  Status sync(EditOffset upper_bound, bool urgent = false) noexcept;

  Status sync_latest(bool urgent = false) noexcept
  {
    return this->sync(this->next_edit_offset(), urgent);
  }

  EditOffset durable_upper_bound() const noexcept
  {
    return EditOffset{this->sync_upper_bound_.get_value()};
  }

  /** \brief Returns the number of bytes between the sync upper bound and the next edit offset.
   * Updates the unflushed_byte_count metric.
   */
  i64 get_unflushed_byte_count() const noexcept;

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

  // activate_blocks() -> AdvanceSyncState -> advance_sync_upper_bound()
  //
  struct AdvanceSyncState;

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

  /** \brief Returned by various block writer pipeline functions to report on the total size and
   * overhead percentage of blocks.
   */
  struct BlockBufferStats {
    u64 user_bytes = 0;
    u64 total_bytes = 0;

    //----- --- -- -  -  -   -

    /** \brief Returns true when the percentage of this->user_bytes relative to this->total_bytes is
     * under the given target precentage.
     */
    bool is_under_target(
        usize target_pct = ChangeLogWriter::kMinBlockDensityTargetPct) const noexcept
    {
      return this->user_bytes * 100 < this->total_bytes * target_pct;
    }

    /** \brief Returns the field-wise sum of this with other; does not modify either input object!
     */
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
  auto allocate_buffer(batt::WaitForResource wait_for_resource) noexcept -> StatusOr<BlockBuffer*>;

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
   *
   * Blocks with slots are appended to `newly_activated` for post-activation advancing of the
   * durable upper bound.
   */
  Status activate_blocks(
      WrittenBlocksState& input,
      ActiveBlocksState& output,
      batt::SmallVecBase<boost::intrusive_ptr<ChangeLogBlock>>& newly_activated) noexcept;

  /** \brief Refreshes the meta-block in the change log file.
   */
  Status refresh_meta_block(ActiveBlocksState& active_blocks) noexcept;

  /** \brief Inserts newly activated blocks into the pending map and advances sync_upper_bound_ by
   * walking slots from the current upper bound. Called after activate_blocks, outside the
   * state mutex.
   */
  void advance_sync_upper_bound(
      batt::SmallVecBase<boost::intrusive_ptr<ChangeLogBlock>>& newly_activated,
      AdvanceSyncState& sync_state) noexcept;

  /** \brief Returns true when the writer task should stay awake: there are pending urgent syncs and
   * unflushed bytes.
   */
  bool has_pending_urgent_sync_work() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The state of the log file.
   */
  std::unique_ptr<ChangeLogFile> change_log_;

  /** \brief The configuration options passed in at construction time.
   */
  Options options_;

  /** \brief Pool from which block tokens are granted; this manages the space in the log, providing
   * back-pressure when it fills up.
   */
  batt::Grant::Issuer free_block_tokens_;

  /** \brief Observability metrics for the log writer.
   */
  Metrics metrics_;

  /** \brief The next unassigned EditOffset.
   */
  std::atomic<i64> next_edit_offset_;

  /** \brief Mutex-protected state for this object.
   */
  batt::Mutex<State> state_;

  /** \brief Buffer for updating the log meta-state while running.
   */
  ChangeLogFile::PackedMetaBlock meta_block_buffer_;

  /** \brief Set to true once-and-only-once when halt() is called the first time.
   */
  std::atomic<bool> halt_requested_{false};

  /** \brief The background writer task.
   */
  Optional<batt::Task> task_;

  /** \brief The confirmed durable EditOffset upper bound.
   */
  batt::Watch<i64> sync_upper_bound_;

  /** \brief The number of pending sync callers that have an urgent priority.
   */
  std::atomic<usize> urgent_sync_counter_{0};
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

template <typename SerializeFn>
  requires std::
      invocable<SerializeFn&&, FirstVisitToBlock, ChangeLogBlock*, MutableBuffer, EditOffset>
    inline auto ChangeLogWriter::Context::append_slot(EditOffset min_edit_offset_lower_bound,
                                                      usize byte_size,
                                                      batt::WaitForResource wait_for_resource,
                                                      SerializeFn&& serialize_fn) noexcept -> Status
{
  static constexpr i64 kBlockClusterMask =
      ~((i64{1} << ChangeLogWriter::kBlockClusterLimitBits) - 1);

  //----- --- -- -  -  -   -

  Context& context = *this;
  ChangeLogWriter& writer = this->writer_;
  auto first_visit_to_block = FirstVisitToBlock{false};

  i64 slot_size = (i64)byte_size;

  // Make sure there is a clean break between blocks every 2^kBlockClusterLimitBits.
  //
  min_edit_offset_lower_bound = EditOffset{
      std::max(min_edit_offset_lower_bound.value(),  //
               (writer.next_edit_offset_.load() + slot_size) & kBlockClusterMask),
  };

  const usize space_needed = byte_size + sizeof(PackedEditOffsetDelta);

  // Grab a private buffer.
  //
  BlockBuffer* observed_head = nullptr;
  BlockBuffer* block_buffer = context.pop_buffer(observed_head);
  for (;;) {
    // If no buffer, allocate one.
    //
    if (block_buffer == nullptr) {
      VLOG(1) << "ChangeLogWriter::append_slot - allocating new block buffer...";

      BATT_ASSIGN_OK_RESULT(block_buffer, writer.allocate_buffer(wait_for_resource));

      // If the slot data is too big to fit in an empty block buffer, this is a fatal error.
      //
      if (block_buffer->space() < space_needed) {
        context.push_buffer(block_buffer, observed_head);
        return batt::StatusCode::kOutOfRange;
      }
      first_visit_to_block = FirstVisitToBlock{true};

      VLOG(1) << "ChangeLogWriter::append_slot - have new buffer!";
    } else {
      const bool buffer_edit_range_too_low =
          (block_buffer->edit_offset_lower_bound() < min_edit_offset_lower_bound);

      const bool buffer_too_full = (block_buffer->space() < space_needed);

      // Try again once more with a fresh buffer.
      //
      if (buffer_edit_range_too_low || buffer_too_full) {
        context.push_buffer(block_buffer, observed_head);
        block_buffer = nullptr;
        continue;
      }
    }
    BATT_ASSERT_NOT_NULLPTR(block_buffer);
    BATT_ASSERT_GE(block_buffer->space(), space_needed);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Once we *know* we will succeed, and not before, we assign the slot edit offset.
    //
    const EditOffset slot_edit_offset{writer.next_edit_offset_.fetch_add(slot_size)};

    if (first_visit_to_block) {
      block_buffer->init_edit_offset_lower_bound(slot_edit_offset);
    } else {
      BATT_ASSERT_LE(block_buffer->edit_offset_lower_bound(), slot_edit_offset);
    }

    // Serialize the payload.
    //
    MutableBuffer slot_buffer = block_buffer->output_buffer(space_needed);

    // Serialize the slot's edit offset delta at the beginning.
    //
    BATT_CHECK_OK(BlockBuffer::write_slot_edit_offset_delta(
        slot_buffer,
        (slot_edit_offset - block_buffer->edit_offset_lower_bound()).to_slot_delta()));

    //----- --- -- -  -  -   -
    BATT_FORWARD(serialize_fn)(first_visit_to_block, block_buffer, slot_buffer, slot_edit_offset);
    //----- --- -- -  -  -   -

    block_buffer->commit_slot(/*n_bytes=*/space_needed);
    context.push_buffer(block_buffer, observed_head);

    return OkStatus();
  }
}

}  // namespace turtle_kv
