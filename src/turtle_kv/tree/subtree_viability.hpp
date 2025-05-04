#pragma once

#include <batteries/case_of.hpp>
#include <batteries/static_assert.hpp>

#include <ostream>
#include <variant>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct Viable {
};

inline std::ostream& operator<<(std::ostream& out, const Viable&)
{
  return out << "Viable";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct NeedsMerge {
  bool single_pivot : 1 = false;
  bool too_few_pivots : 1 = false;
  bool too_few_items : 1 = false;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit operator bool() const
  {
    return this->too_few_pivots ||  //
           this->too_few_items;
  }
};

inline std::ostream& operator<<(std::ostream& out, const NeedsMerge& t)
{
  return out << "NeedsMerge{.single_pivot=" << t.single_pivot
             << ", .too_few_pivots=" << t.too_few_pivots << ", .too_few_items=" << t.too_few_items
             << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct NeedsSplit {
  bool items_too_large : 1 = false;
  bool keys_too_large : 1 = false;
  bool too_many_pivots : 1 = false;
  bool too_many_segments : 1 = false;
  bool flushed_item_counts_too_large : 1 = false;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit operator bool() const
  {
    return this->items_too_large ||    //
           this->keys_too_large ||     //
           this->too_many_pivots ||    //
           this->too_many_segments ||  //
           this->flushed_item_counts_too_large;
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(NeedsSplit), 1);

inline std::ostream& operator<<(std::ostream& out, const NeedsSplit& t)
{
  return out << "NeedsSplit{.items_too_large=" << t.items_too_large
             << ", .keys_too_large=" << t.keys_too_large
             << ", .too_many_pivots=" << t.too_many_pivots
             << ", .too_many_segments=" << t.too_many_segments
             << ", .flushed_item_counts_too_large=" << t.flushed_item_counts_too_large << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using SubtreeViability = std::variant<Viable, NeedsMerge, NeedsSplit>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline std::ostream& operator<<(std::ostream& out, const SubtreeViability& t)
{
  batt::case_of(t, [&out](const auto& case_impl) {
    out << case_impl;
  });
  return out;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool compacting_levels_might_fix(const SubtreeViability& viability)
{
  return batt::case_of(
      viability,
      [](const Viable&) {
        return false;
      },
      [](const NeedsMerge&) {
        return false;
      },
      [](const NeedsSplit& needs_split) {
        return (needs_split.flushed_item_counts_too_large ||  //
                needs_split.too_many_segments) &&             //
               !needs_split.items_too_large &&                //
               !needs_split.keys_too_large &&                 //
               !needs_split.too_many_pivots;
      });
}

}  // namespace turtle_kv
