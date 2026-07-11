#pragma once
#define TURTLE_KV_TREE_MERGE_SET_V_JOIN_MERGE_SET_HPP

#include "composite_merge_set.hpp"

namespace turtle_kv {
namespace merge_set {

struct VJoinMergeSet : CompositeMergeSet {
  Interval<std::string_view> seek(usize byte_size) const noexcept
  {
    return {};
  }
};

}  // namespace merge_set
}  // namespace turtle_kv
