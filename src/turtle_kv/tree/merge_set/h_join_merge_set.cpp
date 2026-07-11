#include "h_join_merge_set.hpp"
//

#include "merge_set.hpp"

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<std::string_view> HJoinMergeSet::seek_impl(
    usize byte_size,
    const std::string_view& key_upper_bound) const noexcept
{
  BATT_CHECK(!this->components_.empty());

  Interval<std::string_view> result;
  Interval<usize> bytes_remaining{byte_size, byte_size};

  for (const std::unique_ptr<MergeSet>& segment : this->components_) {
    const Interval<usize> segment_byte_size = get_byte_size(*segment);

    if (bytes_remaining.lower_bound != 0) {
      if (bytes_remaining.lower_bound > segment_byte_size.upper_bound) {
        bytes_remaining.lower_bound -= segment_byte_size.upper_bound;
      } else {
        result.lower_bound = seek(*segment, bytes_remaining.lower_bound).lower_bound;
        bytes_remaining.lower_bound = 0;
      }
    }

    if (bytes_remaining.upper_bound != 0) {
      if (bytes_remaining.upper_bound > segment_byte_size.lower_bound) {
        bytes_remaining.upper_bound -= segment_byte_size.lower_bound;
      } else {
        result.upper_bound = seek(*segment, bytes_remaining.upper_bound).upper_bound;
        bytes_remaining.upper_bound = 0;
      }
    }

    if (bytes_remaining.lower_bound == 0 && bytes_remaining.upper_bound == 0) {
      return result;
    }
  }

  BATT_CHECK_LE(bytes_remaining.lower_bound, bytes_remaining.upper_bound);

  if (bytes_remaining.lower_bound != 0) {
    result.lower_bound = key_upper_bound;
  }

  if (bytes_remaining.upper_bound != 0) {
    result.upper_bound = key_upper_bound;
  }

  return result;
}

}  // namespace merge_set
}  // namespace turtle_kv
