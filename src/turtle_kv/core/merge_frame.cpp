#include <turtle_kv/core/merge_frame.hpp>
//

#include <utility>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeFrame::~MergeFrame() noexcept
{
  MergeLine* line = this->get_line(0);
  for (usize i = 0; i < this->line_count_; ++i) {
    line->~MergeLine();
    ++line;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeFrame::push_line(BoxedSeq<EditSlice>&& line_slices)
{
  BATT_CHECK_LT(this->line_count_, MergeFrame::kMaxLines);

  new (this->get_line(this->line_count_)) MergeLine{this, std::move(line_slices)};
  ++this->line_count_;
}

}  // namespace turtle_kv
