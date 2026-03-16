#pragma once

#include <turtle_kv/import/int_types.hpp>

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
  u16 pivot_count = 0;
  u16 segment_count = 0;
  i16 height = 0;
  u8 pad_[1];

  bool items_too_large : 1 = false;
  bool keys_too_large : 1 = false;
  bool too_many_pivots : 1 = false;
  bool too_many_segments : 1 = false;
  bool segment_filters_too_large : 1 = false;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit operator bool() const
  {
    return this->items_too_large ||    //
           this->keys_too_large ||     //
           this->too_many_pivots ||    //
           this->too_many_segments ||  //
           this->segment_filters_too_large;
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(NeedsSplit), 8);

inline std::ostream& operator<<(std::ostream& out, const NeedsSplit& t)
{
  return out << "NeedsSplit{.items_too_large=" << t.items_too_large
             << ", .keys_too_large=" << t.keys_too_large
             << ", .too_many_pivots=" << t.too_many_pivots << ", .pivot_count=" << t.pivot_count
             << ", .segment_count=" << t.segment_count << ", .height=" << t.height
             << ", .too_many_segments=" << t.too_many_segments
             << ", .segment_filters_too_large=" << t.segment_filters_too_large << ",}";
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
        return (needs_split.segment_filters_too_large ||  //
                needs_split.too_many_segments) &&         //
               !needs_split.items_too_large &&            //
               !needs_split.keys_too_large &&             //
               !needs_split.too_many_pivots;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief If a node at height == 2 needs to be split (i.e., too large), and the reason is that the
 * update buffer is too large
 */
inline bool normal_flush_might_fix(const SubtreeViability& viability)
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
        return needs_split.height == 2 &&              //
               (needs_split.segment_filters_too_large  //
                || needs_split.too_many_segments) &&   //
               !needs_split.items_too_large &&         //
               !needs_split.keys_too_large &&          //
               !needs_split.too_many_pivots;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool normal_flush_might_fix_root(const SubtreeViability& viability)
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
        return needs_split.too_many_segments &&  //
               !needs_split.too_many_pivots &&   //
               !needs_split.keys_too_large;
      });
}

}  // namespace turtle_kv
