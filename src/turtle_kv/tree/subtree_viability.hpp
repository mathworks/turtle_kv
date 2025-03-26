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

  explicit operator bool() const noexcept
  {
    return this->too_few_pivots ||  //
           this->too_few_items;
  }
};

inline std::ostream& operator<<(std::ostream& out, const NeedsMerge&)
{
  return out << "NeedsMerge";
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

  explicit operator bool() const noexcept
  {
    return this->items_too_large ||    //
           this->keys_too_large ||     //
           this->too_many_pivots ||    //
           this->too_many_segments ||  //
           this->flushed_item_counts_too_large;
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(NeedsSplit), 1);

inline std::ostream& operator<<(std::ostream& out, const NeedsSplit&)
{
  return out << "NeedsSplit";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using SubtreeViability = std::variant<Viable, NeedsMerge, NeedsSplit>;

inline std::ostream& operator<<(std::ostream& out, const SubtreeViability& t)
{
  batt::case_of(t, [&out](const auto& case_impl) {
    out << case_impl;
  });
  return out;
}

}  // namespace turtle_kv
