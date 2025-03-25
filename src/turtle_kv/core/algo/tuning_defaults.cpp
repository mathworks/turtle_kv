#include <turtle_kv/core/algo/tuning_defaults.hpp>
//

#include <turtle_kv/import/int_types.hpp>

#include <batteries/env.hpp>

#include <atomic>

namespace turtle_kv {

namespace {

usize global_default_max_tasks()
{
  return batt::getenv_as<usize>("TURTLE_KV_ALGO_MAX_TASKS")
      .value_or(std::thread::hardware_concurrency());
}

batt::WorkSliceParams get_slice_params(const char* var_name, usize default_min_task_size)
{
  return batt::WorkSliceParams{
      .min_task_size =                                                                      //
      batt::TaskSize{batt::getenv_as<usize>(                                                //
                         batt::to_string("TURTLE_KV_", var_name, "_MIN_TASK_SIZE").c_str()  //
                         )
                         .value_or(default_min_task_size)},

      .max_tasks =                                                         //
      batt::TaskCount{batt::getenv_as<usize>(                              //
                          batt::to_string(var_name, "_MAX_TASKS").c_str()  //
                          )
                          .value_or(global_default_max_tasks())},
  };
}

std::atomic<int> sync_{0};

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ParallelAlgoDefaults& parallel_algo_defaults()
{
  static ParallelAlgoDefaults defaults_{
      .running_total_edit_size = get_slice_params("PARALLEL_RUNNING_TOTAL_EDIT_SIZE", 14000),
      .accumulate_edit_size = get_slice_params("PARALLEL_ACCUMULATE_EDIT_SIZE", 1200),
      //
      .copy_edits = get_slice_params("PARALLEL_COPY_EDITS", 1100),
      .merge_edits = get_slice_params("PARALLEL_MERGE_EDITS", 1100),
      .compact_edits = get_slice_params("PARALLEL_COMPACT_EDITS", 1100),
      //
      .copy_decayed_items = get_slice_params("PARALLEL_COPY_DECAYED_ITEMS", 1100),
      //
      .unpack_delta_batch_edits = get_slice_params("UNPACK_DELTA_BATCH_EDITS", 2048),
      //
      .map_pending_bytes = get_slice_params("PARALLEL_MAP_PENDING_BYTES", 3600),
      .reduce_pending_bytes = get_slice_params("PARALLEL_REDUCE_PENDING_BYTES", 3600),
  };

  (void)sync_.load(std::memory_order_acquire);

  return defaults_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void sync_parallel_algo_defaults()
{
  sync_.fetch_add(1, std::memory_order_release);
}

}  // namespace turtle_kv
