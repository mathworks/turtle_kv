#include "merge_set.hpp"
//

#include <batteries/case_of.hpp>

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeSet clone(const MergeSet& m) noexcept
{
  MergeSet m2;

  m2.impl_ = batt::case_of(  //
      m.impl_,
      [&](const EmptyMergeSet& e) -> MergeSet::Impl {
        return e;
      },
      [&](const InMemoryMergeSet& i) -> MergeSet::Impl {
        return i;
      },
      [&](const InStorageMergeSet& i) -> MergeSet::Impl {
        return i;
      },
      [&](const HJoinMergeSet& h) -> MergeSet::Impl {
        return clone_impl(h);
      },
      [&](const VJoinMergeSet& v) -> MergeSet::Impl {
        return clone_impl(v);
      });

  m2.depth_ = m.depth_;
  m2.byte_size_ = m.byte_size_;
  m2.key_range_ = m.key_range_;

  return m2;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<i32> get_depth(const MergeSet& m) noexcept
{
  return batt::case_of(  //
      m.impl_,
      [&](const EmptyMergeSet&) -> Interval<i32> {
        return {m.depth_, m.depth_};
      },
      [&](const InMemoryMergeSet&) -> Interval<i32> {
        return {m.depth_, m.depth_ + 1};
      },
      [&](const InStorageMergeSet&) -> Interval<i32> {
        return {m.depth_, m.depth_ + 1};
      },
      [&](const HJoinMergeSet& h) -> Interval<i32> {
        return {m.depth_, h.max_depth_};
      },
      [&](const VJoinMergeSet& v) -> Interval<i32> {
        return {m.depth_, m.depth_ + (i32)v.components_.size()};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CInterval<u64> get_byte_size(const MergeSet& m) noexcept
{
  return m.byte_size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<KeyView> get_key_range(const MergeSet& m) noexcept
{
  return m.key_range_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<KeyView> seek(const MergeSet& m, u64 byte_size) noexcept
{
  return batt::case_of(  //
      m.impl_,
      [&](const EmptyMergeSet&) -> Interval<KeyView> {
        return m.key_range_;
      },
      [&](const auto& impl) -> Interval<KeyView> {
        return impl.seek_impl(byte_size, m.key_range_.upper_bound);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::tuple<MergeSet, MergeSet> split(const MergeSet& m, const KeyView& split_key) noexcept
{
  return batt::case_of(  //
      m.impl_,
      [&](const EmptyMergeSet& e) -> std::tuple<MergeSet, MergeSet> {
        return std::tuple<MergeSet, MergeSet>{e, e};
      },
      [&](const auto& impl) -> std::tuple<MergeSet, MergeSet> {
        return impl.split_impl(m, split_key);
      });
}

}  // namespace merge_set
}  // namespace turtle_kv
