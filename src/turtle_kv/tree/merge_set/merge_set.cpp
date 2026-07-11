#include "merge_set.hpp"
//

#include <batteries/case_of.hpp>

namespace turtle_kv {
namespace merge_set {

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
Interval<u64> get_byte_size(const MergeSet& m) noexcept
{
  return m.byte_size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<std::string_view> get_key_range(const MergeSet& m) noexcept
{
  return m.key_range_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<std::string_view> seek(const MergeSet& m, u64 byte_size) noexcept
{
  return batt::case_of(  //
      m.impl_,
      [&](const EmptyMergeSet&) -> Interval<std::string_view> {
        return m.key_range_;
      },
      [&](const auto& impl) -> Interval<std::string_view> {
        return impl.seek_impl(byte_size, m.key_range_.upper_bound);
      });
}

}  // namespace merge_set
}  // namespace turtle_kv
