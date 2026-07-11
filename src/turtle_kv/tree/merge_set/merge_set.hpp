#pragma once
#define TURTLE_KV_TREE_MERGE_SET_MERGE_SET_HPP

#include "empty_merge_set.hpp"
#include "h_join_merge_set.hpp"
#include "in_memory_merge_set.hpp"
#include "in_storage_merge_set.hpp"
#include "v_join_merge_set.hpp"

#include <turtle_kv/import/int_types.hpp>

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

  Impl impl_;
  i32 depth_;
  Interval<u64> byte_size_;
  Interval<std::string_view> key_range_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<i32> get_depth(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<u64> get_byte_size(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<std::string_view> get_key_range(const MergeSet& m) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<std::string_view> seek(const MergeSet& m, u64 byte_size) noexcept;

}  // namespace merge_set
}  // namespace turtle_kv
