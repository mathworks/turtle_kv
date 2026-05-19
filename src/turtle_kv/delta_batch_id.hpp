//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_DELTA_BATCH_ID_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/slot.hpp>

#include <batteries/interval.hpp>
#include <batteries/operators.hpp>

#include <ostream>

namespace turtle_kv {

class DeltaBatchId
{
 public:
  using Self = DeltaBatchId;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  DeltaBatchId(EditOffset edit_offset_upper_bound,
               IndexInGroup index_in_group,
               IsLastInGroup is_last_in_group) noexcept
      : edit_offset_upper_bound_{edit_offset_upper_bound}
      , index_in_group_{index_in_group}
      , is_last_in_group_{is_last_in_group}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  EditOffset edit_offset_upper_bound() const noexcept
  {
    return this->edit_offset_upper_bound_;
  }

  IndexInGroup index_in_group() const noexcept
  {
    return this->index_in_group_;
  }

  IsLastInGroup is_last_in_group() const noexcept
  {
    return this->is_last_in_group_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  EditOffset edit_offset_upper_bound_;
  IndexInGroup index_in_group_;
  IsLastInGroup is_last_in_group_;
};

BATT_OBJECT_PRINT_IMPL((inline),
                       DeltaBatchId,
                       (edit_offset_upper_bound(),  //
                        index_in_group(),
                        is_last_in_group()))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool operator<(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  if (l.edit_offset_upper_bound() < r.edit_offset_upper_bound()) {
    return true;
  }
  if (l.edit_offset_upper_bound() != r.edit_offset_upper_bound()) {
    return false;
  }

  BATT_CHECK_IMPLIES(l.is_last_in_group(), l.index_in_group() >= r.index_in_group());
  BATT_CHECK_IMPLIES(r.is_last_in_group(), r.index_in_group() >= l.index_in_group());

  return l.index_in_group() < r.index_in_group();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool operator==(const DeltaBatchId& l, const DeltaBatchId& r) noexcept
{
  BATT_CHECK_EQ(l.index_in_group() == r.index_in_group(),
                l.is_last_in_group() == r.is_last_in_group());

  return l.edit_offset_upper_bound() == r.edit_offset_upper_bound() &&
         l.index_in_group() == r.index_in_group();
}

BATT_TOTALLY_ORDERED((inline), DeltaBatchId, DeltaBatchId)
BATT_EQUALITY_COMPARABLE((inline), DeltaBatchId, DeltaBatchId)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline EditOffset get_edit_offset_upper_bound(const DeltaBatchId& id)
{
  return id.edit_offset_upper_bound();
}

}  // namespace turtle_kv
