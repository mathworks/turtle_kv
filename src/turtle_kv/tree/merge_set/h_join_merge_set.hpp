//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_MERGE_SET_H_JOIN_MERGE_SET_HPP

#include "composite_merge_set.hpp"

#include <turtle_kv/core/key_view.hpp>

#include <batteries/assert.hpp>

namespace turtle_kv {
namespace merge_set {

struct MergeSet;

CInterval<u64> get_byte_size(const MergeSet& m) noexcept;
Interval<i32> get_depth(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CInterval<u64> h_get_byte_size_impl(
    const std::vector<std::unique_ptr<MergeSet>>& segments) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<KeyView> h_seek_impl(const std::vector<std::unique_ptr<MergeSet>>& segments,
                              usize byte_size,
                              const KeyView& key_upper_bound) noexcept;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct HJoinMergeSet : CompositeMergeSet {
  i32 max_depth_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void add(MergeSet&& src) noexcept
  {
    this->max_depth_ = std::max(this->max_depth_, get_depth(src).upper_bound);
    this->CompositeMergeSet::add(std::move(src));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Interval<KeyView> seek_impl(usize byte_size, const KeyView& key_upper_bound) const noexcept
  {
    return h_seek_impl(this->components_, byte_size, key_upper_bound);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  CInterval<u64> get_byte_size_impl() const noexcept
  {
    return h_get_byte_size_impl(this->components_);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  std::tuple<MergeSet, MergeSet> split_impl(const MergeSet& m,
                                            const KeyView& split_key) const noexcept;
};

}  // namespace merge_set
}  // namespace turtle_kv
