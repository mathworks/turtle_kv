#pragma once

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/merge_line.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/small_vec.hpp>

namespace turtle_kv {

class MergeCompactor;

class MergeFrame
{
 public:
  friend class MergeCompactor;
  friend class MergeLine;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeFrame() = default;

  MergeFrame(const MergeFrame&) = delete;
  MergeFrame& operator=(const MergeFrame&) = delete;

  void push_line(BoxedSeq<EditSlice>&& line_slices);

 private:
  // The MergeCompactor (if any) onto which this frame has been pushed.  A MergeFrame may only be on
  // one compaction stack at a time.
  //
  MergeCompactor* pushed_to_ = nullptr;

  // Bitmap; which elements of `lines` are in use.  This should only be set by the consumer-side
  // code; Producers should simply observe when the active_mask is 0 as a signal that it is ok to
  // move on.
  //
  u64 active_mask_ = 0;

  SmallVec<MergeLine, 12> lines_;
};

}  // namespace turtle_kv
