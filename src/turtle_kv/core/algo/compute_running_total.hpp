#pragma once

#include <turtle_kv/core/algo/decay_to_item.hpp>
#include <turtle_kv/core/algo/tuning_defaults.hpp>

#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/packed_sizeof_edit.hpp>

#include <batteries/algo/parallel_running_total.hpp>

#include <batteries/async/worker_pool.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayValue = false>
inline batt::RunningTotal compute_running_total(
    batt::WorkerPool& worker_pool,
    const MergeCompactor::ResultSet</*decay_to_items=*/false>& result_set,
    DecayToItem<kDecayValue> decay_to_item [[maybe_unused]] = {})
{
  auto merged_edits = result_set.get();

  // Calculate a prefix-sum of the sizes of all items so we know where to cut to make exactly
  // the right sized batch.
  //
  const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();

  const batt::WorkSliceParams running_total_params{
      .min_task_size = algo_defaults.running_total_edit_size.min_task_size,
      .max_tasks = batt::TaskCount{worker_pool.size() + /*this_thread*/ 1},
  };

  worker_pool.reset();

  return batt::parallel_running_total(
      worker_pool,
      merged_edits.begin(),
      merged_edits.end(),
      [](const auto& edit) -> usize {
        if (DecayToItem<kDecayValue>::keep_item(edit)) {
          return PackedSizeOfEdit{}(edit);
        }
        return 0;
      },
      running_total_params);
}

}  // namespace turtle_kv
