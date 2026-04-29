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
Parallel::Parallel() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::schedule(std::vector<Operation>&& ops) /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::step() /*override*/
{
  return {0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> Parallel::retire(ExecutionStrategy* parent) /*override*/
{
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
