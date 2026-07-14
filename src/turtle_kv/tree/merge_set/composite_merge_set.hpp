#pragma once
#define TURTLE_KV_TREE_MERGE_SET_COMPOSITE_MERGE_SET_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

#include <batteries/type_traits.hpp>

#include <memory>
#include <string>
#include <vector>

namespace turtle_kv {
namespace merge_set {

struct MergeSet;

struct CompositeMergeSet {
  std::vector<std::unique_ptr<MergeSet>> components_;
  std::string key_lower_bound_;
  std::string key_upper_bound_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void add(MergeSet&& src) noexcept;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::derived_from<CompositeMergeSet> T>
T clone_impl(const T& src, batt::StaticType<T> = {}) noexcept;

}  // namespace merge_set
}  // namespace turtle_kv
