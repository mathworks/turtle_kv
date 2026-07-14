//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "v_join_merge_set.hpp"
//

#include "h_join_merge_set.hpp"
#include "merge_set.hpp"

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<KeyView> VJoinMergeSet::seek_impl(usize byte_size,
                                           const KeyView& key_upper_bound) const noexcept
{
  Interval<KeyView> result;

  const std::vector<std::unique_ptr<MergeSet>>& levels = this->components_;

  // At the two extremes, the levels could have all the same keys or share none.  In the former
  // case, we need to take the min/max of the level-wise seek result, and in the latter, we do the
  // same thing as for HJoinMergeSet.
  //
  result = h_seek_impl(levels, byte_size, key_upper_bound);

  for (const std::unique_ptr<MergeSet>& level : levels) {
    Interval<KeyView> level_result = seek(*level, byte_size);
    result.lower_bound = std::min(result.lower_bound, level_result.lower_bound);
    result.upper_bound = std::max(result.upper_bound, level_result.upper_bound);
  }

  return result;
}

}  // namespace merge_set
}  // namespace turtle_kv
