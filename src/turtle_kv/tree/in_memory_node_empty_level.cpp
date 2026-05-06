#include <turtle_kv/tree/in_memory_node_empty_level.hpp>
//

#include <turtle_kv/tree/in_memory_node.hpp>

#include <batteries/case_of.hpp>

namespace turtle_kv {

using Level = InMemoryNodeLevel;
using EmptyLevel = InMemoryNodeEmptyLevel;
using MergedLevel = InMemoryNodeMergedLevel;
using SegmentedLevel = InMemoryNodeSegmentedLevel;
using HybridLevel = InMemoryNodeHybridLevel;


//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Level InMemoryNodeEmptyLevel::merge(Level&& sibling_level, usize node_pivot_count) &&
{
  return batt::case_of(
      sibling_level,
      [&](EmptyLevel&) -> Level {
        return *this;
      },
      [&](MergedLevel& right_merged_level) -> Level {
        return std::move(right_merged_level);
      },
      [&](SegmentedLevel& right_segmented_level) -> Level {
        right_segmented_level.push_front_pivots(node_pivot_count);
        return std::move(right_segmented_level);
      },
      [&](HybridLevel& right_hybrid_level) -> Level {
        right_hybrid_level.push_front_pivots(node_pivot_count);
        return std::move(right_hybrid_level);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SmallFn<void(std::ostream&)> InMemoryNodeEmptyLevel::dump() const
{
  return [](std::ostream& out) {
    out << "EmptyLevel{}";
  };
}

}  // namespace turtle_kv
