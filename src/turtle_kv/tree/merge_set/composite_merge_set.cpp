//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "composite_merge_set.hpp"
//
#include "h_join_merge_set.hpp"
#include "merge_set.hpp"
#include "v_join_merge_set.hpp"

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void CompositeMergeSet::add(MergeSet&& src) noexcept
{
  this->components_.emplace_back(std::make_unique<MergeSet>(std::move(src)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::derived_from<CompositeMergeSet> T>
T clone_impl(const T& src, batt::StaticType<T>) noexcept
{
  T dst;

  dst.key_lower_bound_ = src.key_lower_bound_;
  dst.key_upper_bound_ = src.key_upper_bound_;
  dst.components_.reserve(src.components_.size());

  for (const std::unique_ptr<MergeSet>& component : src.components_) {
    dst.components_.emplace_back(std::make_unique<MergeSet>(clone(*component)));
  }

  return dst;
}

template HJoinMergeSet clone_impl<HJoinMergeSet>(const HJoinMergeSet& src,
                                                 batt::StaticType<HJoinMergeSet>) noexcept;

template VJoinMergeSet clone_impl<VJoinMergeSet>(const VJoinMergeSet& src,
                                                 batt::StaticType<VJoinMergeSet>) noexcept;

}  // namespace merge_set
}  // namespace turtle_kv
