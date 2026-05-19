//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_RECOVERED_CHANGE_LOG_STATE_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/change_log/change_log_meta_state.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/interval.hpp>

#include <vector>

namespace turtle_kv {

/** \brief The result of recovering active block state from a ChangeLogFile.
 */
struct RecoveredChangeLogState : ChangeLogMetaState {
  EditOffset next_edit_offset{0};
  std::vector<EditOffset> active_blocks_upper_bounds;
};

}  // namespace turtle_kv
