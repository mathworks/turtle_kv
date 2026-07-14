//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_MERGE_SET_V_JOIN_MERGE_SET_HPP

#include "composite_merge_set.hpp"

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

namespace turtle_kv {
namespace merge_set {

struct VJoinMergeSet : CompositeMergeSet {
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Interval<KeyView> seek_impl(usize byte_size, const KeyView& key_upper_bound) const noexcept;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  std::tuple<MergeSet, MergeSet> split_impl(const MergeSet& m,
                                            const KeyView& split_key) const noexcept;
};

}  // namespace merge_set
}  // namespace turtle_kv
