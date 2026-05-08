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
#include <ranges>

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
StatusOr<usize> ExecutionTimer::activate(ExecutionStrategy* parent) /*override*/
{
  return this->invoke_with_timer([&]() -> StatusOr<usize> {
    return this->base_.activate(parent);
  });
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
StatusOr<usize> Interleave::activate(ExecutionStrategy*) /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::schedule(std::vector<Operation>&& ops) /*override*/
{
  this->step_buffer_.insert(this->step_buffer_.end(),
                            std::make_move_iterator(ops.begin()),
                            std::make_move_iterator(ops.end()));
  ops.clear();
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::step() /*override*/
{
  if (!this->step_buffer_.empty()) {
    this->sequences_.emplace_back(std::move(this->step_buffer_));
    this->step_buffer_.clear();
  }
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Interleave::retire(ExecutionStrategy* parent) /*override*/
{
  std::vector<Operation> merged_ops;

  using Iter = std::vector<Operation>::iterator;
  std::vector<std::ranges::subrange<Iter>> inputs;
  for (auto& seq : this->sequences_) {
    inputs.emplace_back(seq.begin(), seq.end());
  }

  std::default_random_engine rng{/*seed=*/49};
  std::uniform_int_distribution<usize> pick_input{0, inputs.size() - 1};

  while (!inputs.empty()) {
    usize src_i = pick_input(rng);
    auto& src = inputs[src_i];
    merged_ops.emplace_back(std::move(src.front()));
    src.advance(1);
    if (src.empty()) {
      std::swap(src, inputs.back());
      inputs.pop_back();
      pick_input = std::uniform_int_distribution<usize>{0, inputs.size() - 1};
    }
  }

  this->sequences_.clear();

  return parent->schedule(std::move(merged_ops));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class Parallel

namespace {
constexpr i32 kMaxThreads = 256;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Parallel::Parallel(ScriptContext& context, i32 n_threads) noexcept
    : context_{context}
    , n_threads_{std::min(kMaxThreads, n_threads)}
    , barrier_{this->n_threads_ + 1, batt::DoNothing{}}
    , threads_{}
    , thread_state_{new batt::CpuCacheLineIsolated<ThreadState>[std::min<usize>(kMaxThreads,
                                                                                this->n_threads_)]}
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

  LOG(INFO) << "Parallel::activate; n_threads=" << this->n_threads_;

  for (i32 thread_i = 0; thread_i < this->n_threads_; ++thread_i) {
    this->threads_.emplace_back([thread_i, this] {
      auto on_thread_exit = batt::finally([&] {
        if (this->context_.kv_store) {
          this->context_.kv_store->reset_thread_context();
          this->context_.kv_store->release_thread_context();
        }
      });

      VLOG(1) << BATT_INSPECT(thread_i) << " started";
      for (;;) {
        VLOG(1) << BATT_INSPECT(thread_i) << " idle";

        auto on_loop_iteration_exit = batt::finally([&] {
          if (this->context_.kv_store) {
            this->context_.kv_store->reset_thread_context();
          }
        });

        //----- --- -- -  -  -   -
        // Wait for the stage to start.
        //
        this->barrier_.arrive_and_wait();

        //----- --- -- -  -  -   -
        // If no more stages, exit.
        //
        if (this->done_.load()) {
          VLOG(1) << BATT_INSPECT(thread_i) << " exiting";
          break;
        }

        VLOG(1) << BATT_INSPECT(thread_i) << " entered stage";

        auto on_stage_exit = batt::finally([&] {
          VLOG(1) << BATT_INSPECT(thread_i) << " finished stage";

          // Wait for the stage to finish.
          //
          this->barrier_.arrive_and_wait();
        });

        //----- --- -- -  -  -   -
        // Do work.

        // The number of operations to fetch per atomic increment of a thread's progress counter.
        //
        const i64 kFetchCount = 64;

        // The step size required in order to fetch the desired number of thread-specific
        // operations.
        //
        const i64 kStepSize = kFetchCount * this->n_threads_;

        // The total number of operations to be executed by all threads.
        //
        const i64 op_count = BATT_CHECKED_CAST(i64, this->stage_ops_.size());
        const i64 ops_per_thread_rem = (op_count % this->n_threads_);

        // Start with this thread, then attempt to steal work from other threads, in round-robin
        // order.
        //
        for (i64 thread_delta = 0; thread_delta < this->n_threads_; ++thread_delta) {
          const i64 shard_k = (thread_i + thread_delta) % this->n_threads_;
          ThreadState& state = *this->thread_state_[shard_k];
          for (;;) {
            // Claim the next `kFetchCount` operations in thread `k`'s nominal assignment.
            //
            i64 next_op_i = state.progress.fetch_add(kStepSize);
            if (next_op_i >= op_count) {
              break;
            }
            BATT_CHECK_EQ(next_op_i % this->n_threads_, shard_k);
            BATT_CHECK_EQ((next_op_i + kStepSize) % this->n_threads_, shard_k);

            // Calculate the end of the claimed operations.
            //
            const i64 last_op_i =
                std::min(op_count - ops_per_thread_rem + shard_k, next_op_i + kStepSize);

            BATT_CHECK_EQ((last_op_i + kStepSize) % this->n_threads_, shard_k);

            // Execute claimed operations.
            //
            for (; next_op_i != last_op_i; next_op_i += this->n_threads_) {
              BATT_CHECK_EQ(next_op_i % this->n_threads_, shard_k);
              BATT_CHECK_LT(next_op_i, op_count);

              state.status.Update(execute_op(this->context_, this->stage_ops_[next_op_i]));
            }
            BATT_CHECK_EQ(next_op_i % this->n_threads_, shard_k);
          }
        }
      }
      // ~on_scope_edit will wait for all other threads to reach the end of the outer loop.
    });
  }

  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::schedule(std::vector<Operation>&& ops) /*override*/
{
  this->stage_ops_.insert(this->stage_ops_.end(),
                          std::make_move_iterator(ops.begin()),
                          std::make_move_iterator(ops.end()));

  ops.clear();

  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::step() /*override*/
{
  const usize op_count = this->stage_ops_.size();

  VLOG(1) << "stage definition complete; waking threads |" << BATT_INSPECT(op_count);

  // Reset the progress of all threads.
  //
  for (i32 thread_i = 0; thread_i < this->n_threads_; ++thread_i) {
    this->thread_state_[thread_i]->reset(thread_i);
  }

  // Go!
  //
  this->barrier_.arrive_and_wait();

  VLOG(1) << "stage started; waiting for threads to finish";

  // Wait for all threads to finish.
  //
  this->barrier_.arrive_and_wait();

  // Clear the ops queue for the last stage.
  //
  this->stage_ops_.clear();

  // Combine thread Status values.
  //
  Status combined;
  for (i32 thread_i = 0; thread_i < this->n_threads_; ++thread_i) {
    combined.Update(this->thread_state_[thread_i]->status);
  }
  BATT_REQUIRE_OK(combined);

  return {op_count};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::retire(ExecutionStrategy* parent) /*override*/
{
  VLOG(1) << "parallel block done; waking threads...";

  this->done_.store(true);
  this->barrier_.arrive_and_wait();

  VLOG(1) << "threads woken; waiting for shutdown...";

  for (std::thread& t : this->threads_) {
    t.join();
  }

  VLOG(1) << "threads shut down; done!";

  this->threads_.clear();

  return {0};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class Concurrent

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Concurrent::Concurrent(ScriptContext& context) noexcept : context_{context}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::activate(ExecutionStrategy*) /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::schedule(std::vector<Operation>&& ops) /*override*/
{
  this->next_task_.insert(this->next_task_.end(),
                          std::make_move_iterator(ops.begin()),
                          std::make_move_iterator(ops.end()));

  ops.clear();

  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::step() /*override*/
{
  if (!this->next_task_.empty()) {
    this->all_tasks_.emplace_back(std::move(this->next_task_));
    this->next_task_.clear();
  }

  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Concurrent::retire(ExecutionStrategy* parent) /*override*/
{
  LOG(INFO) << "Concurrent::retire; n_tasks=" << this->all_tasks_.size();
  BATT_CHECK(this->next_task_.empty());

  usize op_count = 0;
  std::vector<Status> status(this->all_tasks_.size(), OkStatus());
  std::vector<std::thread> threads;

  for (auto& task_ops : this->all_tasks_) {
    op_count += task_ops.size();
    threads.emplace_back([this, p_ops = &task_ops, task_status = &status[threads.size()]] {
      auto on_thread_exit = batt::finally([&] {
        this->context_.kv_store->reset_thread_context();
        this->context_.kv_store->release_thread_context();
      });

      for (Operation& op : *p_ops) {
        task_status->Update(execute_op(this->context_, op));
        if (!task_status->ok()) {
          break;
        }
      }
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }

  VLOG(1) << "Concurrent tasks finished;" << BATT_INSPECT(op_count);

  this->all_tasks_.clear();

  for (Status& task_status : status) {
    BATT_REQUIRE_OK(task_status);
  }

  return {op_count};
}

}  // namespace script
}  // namespace turtle_kv
