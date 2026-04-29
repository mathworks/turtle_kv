//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/execution_strategy.hpp>
//

#include <turtle_kv/script/script_context.hpp>

#include <batteries/checked_cast.hpp>

#include <chrono>

namespace turtle_kv {
namespace script {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ExecuteImmediately

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ExecuteImmediately::ExecuteImmediately(ScriptContext& context) noexcept
    : context_{context}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteImmediately::schedule(std::vector<Operation>&& ops) /*override*/
{
  using namespace script;

  for (Operation& op : ops) {
    BATT_REQUIRE_OK(execute_op(this->context_, op));
  }

  return {ops.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteImmediately::step() /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteImmediately::retire(ExecutionStrategy* parent [[maybe_unused]]) /*override*/
{
  return {0};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ExecutionTimer

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ExecutionTimer::ExecutionTimer(ScriptContext& context,
                                            ExecutionStrategy& base) noexcept
    : context_{context}
    , base_{base}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::invocable<> Fn>
  requires std::is_same_v<std::invoke_result_t<Fn>, StatusOr<usize>>
StatusOr<usize> ExecutionTimer::invoke_with_timer(Fn&& fn)
{
  auto start_time = std::chrono::steady_clock::now();

  BATT_ASSIGN_OK_RESULT(const usize icount, BATT_FORWARD(fn)());

  if (icount != 0) {
    auto duration = std::chrono::steady_clock::now() - start_time;

    double elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    double op_count = icount;

    LOG(INFO) << icount << " ops completed | elapsed: " << elapsed_ns / 1e9 << "s, "
              << op_count * 1e6 / elapsed_ns << " kops/sec";
  }

  return {icount};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecutionTimer::schedule(std::vector<Operation>&& ops) /*override*/
{
  return this->invoke_with_timer([&]() -> StatusOr<usize> {
    return this->base_.schedule(std::move(ops));
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecutionTimer::step() /*override*/
{
  return this->invoke_with_timer([&]() -> StatusOr<usize> {
    return this->base_.step();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecutionTimer::retire(ExecutionStrategy* parent) /*override*/
{
  return this->invoke_with_timer([&]() -> StatusOr<usize> {
    return this->base_.retire(parent);
  });
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ExecuteAtStep

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ExecuteAtStep::ExecuteAtStep(ExecutionStrategy& base) noexcept : base_{base}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteAtStep::schedule(std::vector<Operation>&& ops) /*override*/
{
  this->buffer_.emplace_back(std::move(ops));
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteAtStep::step() /*override*/
{
  usize total = 0;

  for (std::vector<Operation>& ops : this->buffer_) {
    BATT_ASSIGN_OK_RESULT(const usize count, this->base_.schedule(std::move(ops)));
    total += count;
  }
  this->buffer_.clear();

  return {total};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> ExecuteAtStep::retire(ExecutionStrategy* parent [[maybe_unused]]) /*override*/
{
  return {0};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class Interleave

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interleave::Interleave() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::schedule(std::vector<Operation>&& ops) /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::step() /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::retire(ExecutionStrategy* parent) /*override*/
{
  return {0};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class Parallel

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Parallel::Parallel(i32 n_threads) noexcept
    : n_threads_{n_threads}
    , barrier_{n_threads + 1, batt::DoNothing{}}
    , threads_{}
    , thread_progress_{new batt::CpuCacheLineIsolated<std::atomic<i64>>[n_threads]}
    , done_{false}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Parallel::~Parallel() noexcept
{
  BATT_CHECK(this->threads_.empty());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::activate(ExecutionStrategy*) /*override*/
{
  BATT_CHECK(this->threads_.empty());

  this->done_.store(false);

  for (i32 thread_i = 0; thread_i < this->n_threads_; ++thread_i) {
    this->threads_.emplace_back([thread_i, this] {
      for (;;) {
        // Wait for the stage to start.
        //
        this->barrier_.arrive_and_wait();

        auto on_scope_exit = batt::finally([&] {
          // Wait for the stage to finish.
          //
          this->barrier_.arrive_and_wait();
        });

        // If no more stages, exit.
        //
        if (this->done_.load()) {
          break;
        }

        Status status;

        // Do work.
        //
        const i64 kFetchCount = 16;
        const i64 kStepSize = kFetchCount * this->n_threads_;
        const i64 op_count = BATT_CHECKED_CAST(i64, this->stage_ops_.size());
        std::atomic<i64>& progress = *this->thread_progress_[thread_i];
        for (;;) {
          i64 next_op_i = progress.fetch_add(kStepSize);
          if (next_op_i >= op_count) {
            // Finished with the work for this thread!
            //
            break;
          }

          const i64 last_op_i = std::min(op_count, next_op_i + kStepSize);
          for (; next_op_i != last_op_i; next_op_i += this->n_threads_) {
            status.Update(execute_op(this->context_, this->stage_ops_[next_op_i]));
          }
        }

        // Steal work.
        //
        const i64 kStealCount = 256;
      }
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::schedule(std::vector<Operation>&& ops) /*override*/
{
  this->stage_ops_.insert(this->stage_ops_.end(),
                          std::make_move_iterator(ops.begin()),
                          std::make_move_iterator(ops.end()));
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::step() /*override*/
{
  const usize op_count = this->stage_ops_.size();

  // Reset the progress of all threads.
  //
  for (i32 thread_i = 0; thread_i < this->n_threads_; ++thread_i) {
    this->thread_progress_[thread_i]->store(thread_i);
  }

  // Go!
  //
  this->barrier_.arrive_and_wait();

  // Wait for all threads to finish.
  //
  this->barrier_.arrive_and_wait();

  // Clear the ops queue for the last stage.
  //
  this->stage_ops_.clear();

  return {op_count};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::retire(ExecutionStrategy* parent) /*override*/
{
  this->done_.store(true);
  this->barrier_.arrive_and_wait();

  for (std::thread& t : this->threads_) {
    t.join();
  }

  this->threads_.clear();

  return {0};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class Concurrent

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Concurrent::Concurrent() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::schedule(std::vector<Operation>&& ops) /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::step() /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::retire(ExecutionStrategy* parent) /*override*/
{
  return {0};
}

}  // namespace script
}  // namespace turtle_kv
