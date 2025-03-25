#pragma once

#include <batteries/async/slice_work.hpp>

namespace turtle_kv {

struct ParallelAlgoDefaults {
  batt::WorkSliceParams running_total_edit_size;
  batt::WorkSliceParams accumulate_edit_size;
  batt::WorkSliceParams copy_edits;
  batt::WorkSliceParams merge_edits;
  batt::WorkSliceParams compact_edits;
  batt::WorkSliceParams copy_decayed_items;
  batt::WorkSliceParams unpack_delta_batch_edits;

  //----- --- -- -  -  -   -
  // DEPRECATED (see update_pending_bytes(...), update_pending_bytes.cpp)
  batt::WorkSliceParams map_pending_bytes;
  batt::WorkSliceParams reduce_pending_bytes;
  //----- --- -- -  -  -   -
};

ParallelAlgoDefaults& parallel_algo_defaults();

void sync_parallel_algo_defaults();

}  // namespace turtle_kv
