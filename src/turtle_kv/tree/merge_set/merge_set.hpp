#pragma once
#define TURTLE_KV_TREE_MERGE_SET_MERGE_SET_HPP

#include "empty_merge_set.hpp"
#include "h_join_merge_set.hpp"
#include "in_memory_merge_set.hpp"
#include "in_storage_merge_set.hpp"
#include "v_join_merge_set.hpp"

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <tuple>
#include <variant>

namespace turtle_kv {
namespace merge_set {

struct MergeSet {
  using Impl = std::variant<  //
      EmptyMergeSet,          //
      InMemoryMergeSet,       //
      InStorageMergeSet,      //
      HJoinMergeSet,          //
      VJoinMergeSet           //
      >;

  CInterval<u64> byte_size_;
  Interval<KeyView> key_range_;
  Impl impl_;
  i32 depth_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeSet() noexcept : byte_size_{0, 0}, key_range_{{}, {}}, impl_{EmptyMergeSet{}}, depth_{0}
  {
  }

  explicit MergeSet(const EmptyMergeSet&) noexcept : MergeSet{}
  {
  }

  explicit MergeSet(HJoinMergeSet&& impl, i32 depth) noexcept
      : byte_size_{impl.get_byte_size_impl()}
      , key_range_{impl.key_lower_bound_, impl.key_upper_bound_}
      , impl_{std::move(impl)}
      , depth_{depth}
  {
  }

  MergeSet(const MergeSet&) = delete;
  MergeSet& operator=(const MergeSet&) = delete;

  MergeSet(MergeSet&&) = default;
  MergeSet& operator=(MergeSet&&) = default;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeSet clone(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the level depth range of the passed MergeSet.
 */
Interval<i32> get_depth(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the minimum known bounding range of merged byte size for the passed set.
 */
CInterval<u64> get_byte_size(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the minimum known bounding key range of the passed set.
 */
Interval<KeyView> get_key_range(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the minimum interval in which lies the true upper bound corresponding to the
 * specified number of bytes (as measured from the beginning of the final merged version of `m`).
 */
Interval<KeyView> seek(const MergeSet& m, u64 byte_size) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::tuple<MergeSet, MergeSet> split(const MergeSet& m, const KeyView& split_key) noexcept;

}  // namespace merge_set
}  // namespace turtle_kv
