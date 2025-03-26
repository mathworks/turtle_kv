#include <turtle_kv/core/merge_line.hpp>
//

#include <turtle_kv/core/merge_frame.hpp>

#include <turtle_kv/import/logging.hpp>
#include <turtle_kv/import/optional.hpp>

#include <batteries/assert.hpp>
#include <batteries/case_of.hpp>

#include <utility>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeLine::MergeLine(MergeFrame* frame, BoxedSeq<EditSlice>&& edit_slices) noexcept
    : frame_{frame}
    , depth_{0}
    , first_{}
    , rest_{std::move(edit_slices)}
    , back_handle_{}
{
  this->advance();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MergeLine::empty()
{
  return batt::case_of(this->first_, [&](const auto& first_slice) -> bool {
    return first_slice.empty() && !this->rest_.peek();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeLine::advance()
{
  // Set `first_` to the next non-empty slice.
  //
  for (;;) {
    Optional<EditSlice> next_slice = this->rest_.next();
    if (!next_slice) {
      // End of sequence.
      break;
    }
    this->first_ = *next_slice;
    if (!is_empty(this->first_)) {
      // Found a non-empty slice.
      break;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
EditSlice MergeLine::cut(const KeyView& last_key)
{
  return batt::case_of(this->first_, [&](auto& first_slice) -> EditSlice {
    if (first_slice.empty()) {
      return first_slice;
    }

    using EditT = std::decay_t<decltype(first_slice.front())>;

    const EditT* slice_end =
        std::upper_bound(first_slice.begin(), first_slice.end(), last_key, KeyOrder{});
    Slice<const EditT> prefix = as_slice(first_slice.begin(), slice_end);
    first_slice = as_slice(slice_end, first_slice.end());
    if (first_slice.empty()) {
      this->advance();
    }
    VLOG(1) << "cut(key=" << last_key << ") -> [" << prefix.size()
            << " items]; (empty=" << this->empty() << ") -> " << batt::dump_range(prefix)
            << " (this_after = " << debug_print(*this) << ")";
    return prefix;
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MergeLine::begins_after(const KeyView& key) const
{
  return batt::case_of(this->first_, [&](const auto& first_slice) -> bool {
    // By convention, empty lines begin at +inf.
    //
    if (first_slice.empty()) {
      return true;
    }

    return key < get_key(first_slice.front());
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MergeLine::get_index_in_frame() const
{
  BATT_CHECK_NOT_NULLPTR(this->frame_);
  BATT_CHECK_GE(this, this->frame_->lines_.data());
  BATT_CHECK_LT(this, this->frame_->lines_.data() + this->frame_->lines_.size());

  return this - this->frame_->lines_.data();
}

}  // namespace turtle_kv
