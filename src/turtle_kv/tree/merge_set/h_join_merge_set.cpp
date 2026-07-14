#include "h_join_merge_set.hpp"
//

#include "merge_set.hpp"

#include <turtle_kv/core/key_range.hpp>

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CInterval<u64> h_get_byte_size_impl(const std::vector<std::unique_ptr<MergeSet>>& segments) noexcept
{
  if (segments.empty()) {
    return {0, 0};
  }

  CInterval<u64> result{0, 0};

  for (const std::unique_ptr<MergeSet>& p_component : segments) {
    const CInterval<u64> component_byte_size = get_byte_size(*p_component);
    result.lower_bound += component_byte_size.lower_bound;
    result.upper_bound += component_byte_size.upper_bound;
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<KeyView> h_seek_impl(const std::vector<std::unique_ptr<MergeSet>>& segments,
                              usize byte_size,
                              const KeyView& key_upper_bound) noexcept
{
  BATT_CHECK(!segments.empty());

  Interval<KeyView> result;
  CInterval<usize> bytes_remaining{byte_size, byte_size};

  for (const std::unique_ptr<MergeSet>& segment : segments) {
    const CInterval<usize> segment_byte_size = get_byte_size(*segment);

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::tuple<MergeSet, MergeSet> HJoinMergeSet::split_impl(const MergeSet& m,
                                                         const KeyView& split_key) const noexcept
{
  // Find the segment where the split_key lives and split that one.
  //
  auto eq_it = std::equal_range(this->components_.begin(),
                                this->components_.end(),
                                split_key,
                                ExtendedKeyRangeOrder{});

  // Base cases.
  //
  if (eq_it.first == this->components_.end()) {
    return {clone(m), MergeSet{}};
  }

  if (eq_it.second == this->components_.begin()) {
    return {MergeSet{}, clone(m)};
  }

  HJoinMergeSet before_split_impl;
  HJoinMergeSet after_split_impl;

  before_split_impl.key_lower_bound_ = this->key_lower_bound_;
  before_split_impl.key_upper_bound_ = split_key;
  before_split_impl.max_depth_ = m.depth_;

  after_split_impl.key_lower_bound_ = split_key;
  after_split_impl.key_upper_bound_ = this->key_upper_bound_;
  after_split_impl.max_depth_ = m.depth_;

  // Copy all segments that are definitely before `split_key` to `before_split`.
  //
  std::for_each(  //
      this->components_.begin(),
      eq_it.first,
      [&before_split_impl](const std::unique_ptr<MergeSet>& p_segment) {
        before_split_impl.add(clone(*p_segment));
      });

  // Handle the matched range if non-empty.
  //
  if (eq_it.first != eq_it.second) {
    // If the split key is *not* at the exact start of the matched segment, then split that segment
    // and assign the resulting parts accordingly.
    //
    if (get_key_range(**eq_it.first).lower_bound < split_key) {
      MergeSet middle_lower, middle_upper;

      std::tie(middle_lower, middle_upper) = split(**eq_it.first, split_key);

      before_split_impl.add(std::move(middle_lower));
      after_split_impl.add(std::move(middle_upper));

    } else {
      // The split_key is exactly the start of the middle segment; assign it in whole to
      // `after_split`.
      //
      after_split_impl.add(clone(**eq_it.first));
    }
  }

  // Copy all segments that are definitely after `split_key` to `after_split`.
  //
  std::for_each(  //
      eq_it.second,
      this->components_.end(),
      [&after_split_impl](const std::unique_ptr<MergeSet>& p_segment) {
        after_split_impl.add(clone(*p_segment));
      });

  // Form the output sets.
  //
  return {
      MergeSet{std::move(before_split_impl), m.depth_},
      MergeSet{std::move(after_split_impl), m.depth_},
  };
}

}  // namespace merge_set
}  // namespace turtle_kv
