//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_META_STATE_HPP

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/import/interval.hpp>

#include <batteries/operators.hpp>

namespace turtle_kv {

struct PackedChangeLogMetaState;

struct ChangeLogMetaState {
  /** \brief The (logical) interval of active block indices.
   *
   * lower_bound: the "trim position" -- this is the position in the file, not the logical offset.
   * upper_bound: the "flush position" -- this is the position in the file (not logical offset)
   * one past the *last* known written and untrimmed block.
   */
  Interval<BlockIndex> block_range{BlockIndex{0}, BlockIndex{0}};

  /** \brief Blocks with edit offset upper bound less than or equal to this value can be
   * safely overwritten.
   */
  EditOffset trim_edit_offset{0};

  //----- --- -- -  -  -   -

  static ChangeLogMetaState with_initial_values() noexcept;

  //----- --- -- -  -  -   -

  void pack_to(PackedChangeLogMetaState* packed) const noexcept;
};

BATT_OBJECT_PRINT_IMPL((inline), ChangeLogMetaState, (block_range, trim_edit_offset))

struct PackedChangeLogMetaState {
  // The physical address in the ChangeLogFile of the oldest known
  // active block. The lower bound will never Greater Than the upper_bound. NOT guaranteed to be
  // up to date. The actual oldest active block may be newer. The lower_bound should guarantee it
  // is Less Than the lower bound of the actual oldest active block.
  //
  little_i64 active_blocks_lower_bound;

  // Logical address in the ChangeLogFile of the newest known active block. NOT guaranteed to be
  // up to date. The actual newest active block may be newer. The upper_bound should guarantee it
  // is Less Than the upper bound of the actual newest active block.
  //
  little_i64 active_blocks_upper_bound;

  little_i64 trim_edit_offset;

  u8 reserved_[40];

  //----- --- -- -  -  -   -

  ChangeLogMetaState unpack() const noexcept;
};

static_assert(sizeof(PackedChangeLogMetaState) == 64);

}  // namespace turtle_kv
