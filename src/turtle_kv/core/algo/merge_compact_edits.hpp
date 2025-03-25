#pragma once

#include <turtle_kv/core/algo/parallel_compact.hpp>
#include <turtle_kv/core/algo/tuning_defaults.hpp>
#include <turtle_kv/core/edit_view.hpp>

#include <batteries/algo/parallel_merge.hpp>

#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/algo/parallel_copy.hpp>
#include <batteries/suppress.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Merge and compact a series (slice) of key-ordered "segments" comprised of EditView slices.
//
// If any of the slices in input_segments is NOT key-ordered, behavior is undefined.
//
// `output_buffer_1` and `output_buffer_2` must BOTH be as large as the total number of edits
// contained in `input_segments`.  The returned slice may point into any of these three params;
// callers should plan accordingly.
//
template <bool kDecayToItem>
inline std::conditional_t<kDecayToItem, Slice<const ItemView>, Slice<const EditView>>
merge_compact_edits(batt::WorkerPool& worker_pool,
                    const Slice<const Slice<const EditView>>& input_segments,
                    Slice<EditView> output_buffer_1, Slice<EditView> output_buffer_2,
                    DecayToItem<kDecayToItem> decay_to_item)
{
  using ResultT = std::conditional_t<kDecayToItem, Slice<const ItemView>, Slice<const EditView>>;

  if (input_segments.empty()) {
    return ResultT{nullptr, nullptr};
  }

  const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();

  const usize n_threads = worker_pool.size() + /* this thread: */ 1;

  SmallVec<Slice<const EditView>, 64> dst_segments_vec,
      src_segments_vec(input_segments.begin(), input_segments.end());

  auto* src_segments = &src_segments_vec;
  auto* dst_segments = &dst_segments_vec;

  // Each time through the loop, we cut the number of merged/sorted segments in half by doing a
  // bunch of adjacent pair-wise merges in parallel.
  //
  while (src_segments->size() > 1) {
    dst_segments->clear();
    {
      batt::ScopedWorkContext context{worker_pool};
      {
        EditView* dst_segment_begin = output_buffer_1.begin();
        for (auto src_iter = src_segments->begin(); src_iter != src_segments->end();) {
          Slice<const EditView> src_0 = *src_iter;
          ++src_iter;
          if (src_iter == src_segments->end()) {
            Slice<EditView> dst = as_slice(dst_segment_begin, src_0.size());
            BATT_CHECK_LE(dst.end(), output_buffer_1.end());
            dst_segments->emplace_back(dst);

            parallel_copy(context, src_0.begin(), src_0.end(), dst.begin(),          //
                          /*min_task_size=*/algo_defaults.copy_edits.min_task_size,  //
                          /*max_tasks=*/batt::TaskCount{n_threads});
            break;
          }

          Slice<const EditView> src_1 = *src_iter;
          ++src_iter;

          Slice<EditView> dst = as_slice(dst_segment_begin, src_0.size() + src_1.size());
          BATT_CHECK_LE(dst.end(), output_buffer_1.end());
          dst_segments->emplace_back(dst);
          dst_segment_begin += dst.size();

          auto work_fn = [&context, &algo_defaults, src_0, src_1, dst, n_threads] {
            parallel_merge(context,                     //
                           src_0.begin(), src_0.end(),  //
                           src_1.begin(), src_1.end(),  //
                           dst.begin(),                 //
                           KeyOrder{},                  //
                           /*min_task_size=*/algo_defaults.merge_edits.min_task_size,
                           /*max_tasks=*/batt::TaskCount{n_threads});
          };

          if (src_iter == src_segments->end()) {
            work_fn();
          } else {
            BATT_CHECK_OK(context.async_run(std::move(work_fn)))
                << "worker_pool must not be closed!";
          }
        }
      }
    }
    std::swap(src_segments, dst_segments);
    std::swap(output_buffer_1, output_buffer_2);
  }

  Slice<const EditView> merged_edits = src_segments->front();

  Slice<EditView> chunked_compacted_edits = output_buffer_1;
  SmallVec<Chunk<EditView*>, 64> compacted_chunks(/*max chunks=*/n_threads + 1);

  compacted_chunks.erase(
      parallel_compact_into_chunks(worker_pool,                                                  //
                                   merged_edits.begin(), merged_edits.end(),                     //
                                   chunked_compacted_edits.begin(),                              //
                                   compacted_chunks.begin(),                                     //
                                   KeyEqual{},                                                   //
                                   BATT_OVERLOADS_OF(combine),                                   //
                                   decay_to_item,                                                //
                                   /*min_task_size=*/algo_defaults.compact_edits.min_task_size,  //
                                   /*max_tasks=*/batt::TaskCount{n_threads}                      //
                                   ),
      compacted_chunks.end());

  auto flat_compacted_edits = flatten(compacted_chunks.begin(), std::prev(compacted_chunks.end()));

  Slice<EditView> compacted_edits{output_buffer_2.begin(),
                                  output_buffer_2.begin() + flat_compacted_edits.size()};
  {
    batt::ScopedWorkContext context{worker_pool};

    parallel_copy(context,                                                   //
                  flat_compacted_edits.begin(), flat_compacted_edits.end(),  //
                  compacted_edits.begin(),                                   //
                  /*min_task_size=*/algo_defaults.copy_edits.min_task_size,  //
                  /*max_tasks=*/batt::TaskCount{n_threads});
  }

  // This really is OK because we guarantee that ItemView has the exact same memory layout as
  // EditView.
  //
  BATT_SUPPRESS_IF_GCC("-Wstrict-aliasing")
  return reinterpret_cast<ResultT&&>(compacted_edits);
  BATT_UNSUPPRESS_IF_GCC()
}

}  // namespace turtle_kv
