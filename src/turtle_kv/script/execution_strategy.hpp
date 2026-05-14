//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_EXECUTION_STRATEGY_HPP

#include <turtle_kv/script/operations.hpp>

#include <turtle_kv/import/status.hpp>

#include <batteries/cpu_align.hpp>
#include <batteries/do_nothing.hpp>

#include <atomic>
#include <barrier>
#include <thread>
#include <type_traits>
#include <vector>

namespace turtle_kv {

class ScriptContext;

namespace script {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A strategy for scheduling and executing operations inside a script.
 */
class ExecutionStrategy
{
 public:
  ExecutionStrategy(const ExecutionStrategy&) = delete;
  ExecutionStrategy& operator=(const ExecutionStrategy&) = delete;

  virtual ~ExecutionStrategy() = default;

  /** \brief Called right before this ExecutionStrategy is pushed on the stack.
   */
  virtual StatusOr<usize> activate(ExecutionStrategy* parent) = 0;

  /** \brief Adds a series of operations to be executed.
   *
   * \return the number of ops executed
   */
  virtual StatusOr<usize> schedule(std::vector<Operation>&& ops) = 0;

  /** \brief Called at the end of each command.
   *
   * \return the number of ops executed
   */
  virtual StatusOr<usize> step() = 0;

  /** \brief Called right after this ExecutionStrategy is popped off the stack.
   *
   * \return the number of ops executed
   */
  virtual StatusOr<usize> retire(ExecutionStrategy* parent) = 0;

 protected:
  ExecutionStrategy() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Executes all operations immediately.
 */
class ExecuteImmediately : public ExecutionStrategy
{
 public:
  explicit ExecuteImmediately(ScriptContext& context) noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

 private:
  ScriptContext& context_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Reports timing information for executed ops.
 */
class ExecutionTimer : public ExecutionStrategy
{
 public:
  explicit ExecutionTimer(ScriptContext& context, ExecutionStrategy& base) noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  template <std::invocable<> Fn>
    requires std::is_same_v<std::invoke_result_t<Fn>, StatusOr<usize>>
  StatusOr<usize> invoke_with_timer(Fn&& fn);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ScriptContext& context_;
  ExecutionStrategy& base_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Buffers all operations, then runs them at the next call to `step`.
 */
class ExecuteAtStep : public ExecutionStrategy
{
 public:
  explicit ExecuteAtStep(ExecutionStrategy& base) noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

 private:
  ExecutionStrategy& base_;
  std::vector<std::vector<Operation>> buffer_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Saves sequences of operations between steps, then randomly interleaves all the sequences
 * into a single sequence which is scheduled onto the parent ExecutionStrategy when retired.
 */
class Interleave : public ExecutionStrategy
{
 public:
  Interleave() noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

 private:
  std::vector<Operation> step_buffer_;
  std::vector<std::vector<Operation>> sequences_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Runs all scheduled operations in parallel at each step on a pool of threads.
 *
 * Let `n_threads` be the number of threads specified for this strategy, and let `thread_i` be the
 * index of a given thread (`0 <= thread_i < n_threads`).  Each thread will iterate through the
 * single sequence of operations collected in the last step, starting at `thread_i` and incrementing
 * their position by `n_threads` after each operation.  When a thread finishes, it will poll the
 * other threads, trying to steal work from them until all operations have been executed.
 *
 * The thread pool is launched when this object is created, and it is terminated on retire.
 */
class Parallel : public ExecutionStrategy
{
 public:
  explicit Parallel(ScriptContext& context, i32 n_threads) noexcept;

  ~Parallel() noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct ThreadState {
    std::atomic<i64> progress;
    Status status;

    //----- --- -- -  -  -   -

    void reset(i32 thread_i) noexcept
    {
      this->progress.store(thread_i);
      this->status = OkStatus();
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ScriptContext& context_;
  i32 n_threads_;
  std::barrier<batt::DoNothing> barrier_;
  std::vector<std::thread> threads_;
  std::unique_ptr<batt::CpuCacheLineIsolated<ThreadState>[]> thread_state_;
  std::atomic<bool> done_;
  std::vector<Operation> stage_ops_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Collects the sequence of operations between each step; for each of these, a thread is
 * created on retire, each of which will run the collected operations in order.
 */
class Concurrent : public ExecutionStrategy
{
 public:
  explicit Concurrent(ScriptContext& context) noexcept;

  StatusOr<usize> activate(ExecutionStrategy* parent) override;

  StatusOr<usize> schedule(std::vector<Operation>&& ops) override;

  StatusOr<usize> step() override;

  StatusOr<usize> retire(ExecutionStrategy* parent) override;

 private:
  ScriptContext& context_;
  std::vector<Operation> next_task_;
  std::vector<std::vector<Operation>> all_tasks_;
};

}  // namespace script
}  // namespace turtle_kv
