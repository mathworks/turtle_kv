#include <turtle_kv/core/merge_frame.hpp>
//

#include <utility>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeFrame::push_line(BoxedSeq<EditSlice>&& line_slices)
{
  this->lines_.emplace_back(this, std::move(line_slices));
}

}  // namespace turtle_kv
