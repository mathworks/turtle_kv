#pragma once

#include <turtle_kv/change_log_block.hpp>
#include <turtle_kv/change_log_file.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <batteries/async/future.hpp>
#include <batteries/async/grant.hpp>
#include <batteries/async/latch.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/interval.hpp>

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Self = ChangeLogWriter;

  /** \brief A logical sequence number assigned to each slot appended through a ChangeLogWriter.
   */
  using Index = i64;

  /** \brief A closed (inclusive-bounds) interval of index values.
   */
  using IndexRange = CInterval<Index>;

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
    CountMetric<u64> received_block_byte_count{0};
    CountMetric<u64> received_user_byte_count{0};
    CountMetric<u64> written_block_byte_count{0};
    CountMetric<u64> written_user_byte_count{0};
    CountMetric<u64> sleep_count{0};
    CountMetric<u64> write_count{0};
    CountMetric<u64> block_alloc_count{0};
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
    template <typename SerializeFn = void(BlockBuffer*, MutableBuffer buffer)>
    StatusOr<Index> append_slot(u64 owner_id,
                                usize byte_size,
                                const SerializeFn& serialize_fn) noexcept;

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
   */
  explicit ChangeLogWriter(std::unique_ptr<ChangeLogFile>&& change_log,
                           const Options& options) noexcept;

  /** \brief Destructs a ChangeLogWriter.  All ChangeLogWriter::Context objects must be
   * destructed before the ChangeLogWriter is allowed to go out of scope.
   */
  ~ChangeLogWriter() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Metrics& metrics() noexcept
  {
    return this->metrics_;
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct State {
    std::vector<Context*> contexts_;

    ~State() noexcept
    {
      BATT_CHECK(this->contexts_.empty()) << "All Context objects associated with a "
                                             "ChangeLogWriter MUST be destroyed before the "
                                             "ChangeLogWriter goes out of scope!";
    }
  };

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
  auto allocate_buffer(u64 owner_id) noexcept -> StatusOr<BlockBuffer*>;

  /** \brief The background writer task; continuously polls all associated Contexts for new
   * data. When new data is found, it is merged in index-order and written in batches (as large
   * as possible) to the Volume, to optimize throughput.
   */
  void writer_task_main() noexcept;

  /** \brief Does a non-blocking check of all associated Contexts for any BlockBuffers that
   * might contain committed slot data.
   */
  batt::SmallVec<BlockBuffer*, 8> poll_updates() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Metrics metrics_;

  /** \brief The next unassigned logical slot sequence number.
   */
  std::atomic<i64> next_index_{0};

  /** \brief Mutex-protected state for this object.
   */
  batt::Mutex<State> state_;

  /** \brief The state of the log file.
   */
  std::unique_ptr<ChangeLogFile> change_log_;

  /** \brief The configuration options passed in at construction time.
   */
  Options options_;

  /** \brief Set to true once-and-only-once when halt() is called the first time.
   */
  std::atomic<bool> halt_requested_{false};

  /** \brief The background writer task.
   */
  Optional<batt::Task> task_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

template <typename SerializeFn>
inline auto ChangeLogWriter::Context::append_slot(u64 owner_id,
                                                  usize byte_size,
                                                  const SerializeFn& serialize_fn) noexcept
    -> StatusOr<Index>
{
  Context& context = *this;
  ChangeLogWriter& writer = this->writer_;

  // Grab a private buffer.
  //
  BlockBuffer* observed_head = nullptr;
  BlockBuffer* buffer = context.pop_buffer(observed_head);
  for (;;) {
    const bool no_buffer = (buffer == nullptr);

    // No buffer, no retry; there is no point attempting again if we had a fresh, empty buffer
    // to begin with.
    //
    const bool no_retry = no_buffer;

    // If no buffer, allocate one.
    //
    if (no_buffer) {
      BATT_ASSIGN_OK_RESULT(buffer, writer.allocate_buffer(owner_id));
      writer.metrics_.block_alloc_count.add(1);
    }
    BATT_CHECK_NOT_NULLPTR(buffer);

    // Serialize the payload.
    //
    const usize space = buffer->space();

    StatusOr<usize> result;
    if (byte_size <= space) {
      serialize_fn(buffer, buffer->output_buffer());
      result = byte_size;
    } else {
      result = Status{batt::StatusCode::kResourceExhausted};
    }

    {
      // When we leave this scope block, give the BlockBuffer back to the Context so it can be
      // (possibly) appended in the background.
      //
      auto on_scope_exit = batt::finally([&] {
        context.push_buffer(buffer, observed_head);
      });

      // If there was room in the buffer, then `format_slot` will succeed; assign this slot an
      // "index" (sequence number or logical time-stamp) and commit the newly copied data.
      // (Remember, the scope guard above will take care of giving the BlockBuffer back to the
      // Context)
      //
      if (result.ok()) {
        const i64 slot_index = writer.next_index_.fetch_add(1);
        buffer->commit_slot(/*n_bytes=*/*result);
        return slot_index;

      } else {
        VLOG(1) << "format_slot failed: " << result.status() << BATT_INSPECT(no_buffer)
                << BATT_INSPECT(no_retry) << BATT_INSPECT(space) << BATT_INSPECT(byte_size);
      }
    }

    if (no_retry) {
      return result.status();
    }

    // Volume::format_slot only fails if there wasn't enough space; reset the buffer pointer and
    // retry (we will allocate a new buffer at the top of loop).
    //
    buffer = nullptr;
  }
}

}  // namespace turtle_kv
