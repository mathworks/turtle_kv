#pragma once

#include <turtle_kv/core/algo/compute_running_total.hpp>

#include <turtle_kv/core/merge_compactor.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>

#include <batteries/algo/running_total.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/worker_pool.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct BatchUpdateContext {
  batt::WorkerPool& worker_pool;
  llfs::PageLoader& page_loader;
  batt::CancelToken cancel_token;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Uses the worker_pool to perform a parallel merge-compaction of the lines
   * produced by the passed `generator_fn`, up to and including (but stopping at) `max_key`.
   */
  template <typename GeneratorFn>
  StatusOr<MergeCompactor::ResultSet</*decay_to_items=*/false>> merge_compact_edits(
      const KeyView& max_key,
      GeneratorFn&& generator_fn);

  /** \brief Does the same as `this->merge_compact_edits`, but pushes a single MergeFrame first and
   * passes that to `frame_push_fn`.
   */
  template <typename FramePushFn>
  StatusOr<MergeCompactor::ResultSet</*decay_to_items=*/false>> merge_compact_edits_in_frame(
      const KeyView& max_key,
      FramePushFn&& frame_push_fn);

  /** \brief Computes and returns the running total (prefix sum) of the edit sizes in result_set.
   */
  batt::RunningTotal compute_running_total(
      const MergeCompactor::ResultSet</*decay_to_items=*/false>& result_set) const
  {
    return ::turtle_kv::compute_running_total(this->worker_pool, result_set);
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct BatchUpdate {
  struct TrimResult {
    usize n_items_trimmed = 0;
    usize n_bytes_trimmed = 0;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BatchUpdateContext context;
  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  Optional<batt::RunningTotal> edit_size_totals;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Resets `this->edit_size_totals` to reflect `this->result_set`.
   */
  void update_edit_size_totals();

  /** \brief Returns the inclusive (closed) interval of keys in this batch.
   */
  CInterval<KeyView> get_key_crange() const
  {
    BATT_CHECK(!this->result_set.empty());
    return this->result_set.get_key_crange();
  }

  /** \brief Trim items from the end/back of the result_set, such that the total batch size (in
   * bytes) is not greater than `byte_size_limit`.
   */
  TrimResult trim_back_down_to_size(usize byte_size_limit);

  /** \brief Calculates the size of `result_set` if necessary, and returns the total number of bytes
   * in this batch.
   */
  usize get_byte_size();
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const BatchUpdate::TrimResult& t);

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename GeneratorFn>
inline StatusOr<MergeCompactor::ResultSet</*decay_to_items=*/false>>
BatchUpdateContext::merge_compact_edits(const KeyView& max_key, GeneratorFn&& generator_fn)
{
  MergeCompactor compactor{this->worker_pool};
  compactor.set_generator(BATT_FORWARD(generator_fn));

  MergeCompactor::EditBuffer edit_buffer;

  this->worker_pool.reset();
  return compactor.read(edit_buffer, max_key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename FramePushFn>
inline StatusOr<MergeCompactor::ResultSet</*decay_to_items=*/false>>
BatchUpdateContext::merge_compact_edits_in_frame(const KeyView& max_key,
                                                 FramePushFn&& frame_push_fn)
{
  return this->merge_compact_edits(  //
      max_key,                       //
      [&](MergeCompactor::GeneratorContext& context) -> Status {
        MergeFrame frame;
        //----- --- -- -  -  -   -
        frame_push_fn(frame);
        //----- --- -- -  -  -   -
        context.push_frame(&frame);
        return context.await_frame_consumed(&frame);
      });
}

}  // namespace turtle_kv
