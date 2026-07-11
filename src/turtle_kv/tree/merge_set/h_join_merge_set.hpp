#pragma once
#define TURTLE_KV_TREE_MERGE_SET_H_JOIN_MERGE_SET_HPP

#include "composite_merge_set.hpp"

#include <batteries/assert.hpp>

namespace turtle_kv {
namespace merge_set {

struct HJoinMergeSet : CompositeMergeSet {
  i32 max_depth_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Interval<std::string_view> seek_impl(usize byte_size,
                                       const std::string_view& key_upper_bound) const noexcept;
};

}  // namespace merge_set
}  // namespace turtle_kv
