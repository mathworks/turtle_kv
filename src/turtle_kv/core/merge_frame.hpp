#pragma once

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/merge_line.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/seq.hpp>

namespace turtle_kv {

class MergeCompactorBase;

class MergeFrame
{
 public:
  friend class MergeCompactor;
  friend class MergeScanner;
  friend class MergeLine;

  static constexpr usize kMaxLines = 64;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeFrame() = default;

  MergeFrame(const MergeFrame&) = delete;
  MergeFrame& operator=(const MergeFrame&) = delete;

  ~MergeFrame() noexcept;

  void push_line(BoxedSeq<EditSlice>&& line_slices);

  MergeLine* get_line(usize i)
  {
    return reinterpret_cast<MergeLine*>(&this->lines_storage_) + i;
  }

  usize line_count() const
  {
    return this->line_count_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The MergeCompactor (if any) onto which this frame has been pushed.  A MergeFrame may only be on
  // one compaction stack at a time.
  //
  MergeCompactorBase* pushed_to_ = nullptr;

  // Bitmap; which elements of `lines` are in use.  This should only be set by the consumer-side
  // code; Producers should simply observe when the active_mask is 0 as a signal that it is ok to
  // move on.
  //
  u64 active_mask_ = 0;

  // Memory to store the maximum sized MergeLine array.
  //
  std::aligned_storage_t<sizeof(MergeLine) * kMaxLines, alignof(MergeLine)> lines_storage_;

  // The number of lines which have been constructed in `this->lines_storage_`.
  //
  usize line_count_ = 0;
};

}  // namespace turtle_kv
