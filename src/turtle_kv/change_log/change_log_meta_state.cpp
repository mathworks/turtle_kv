//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_meta_state.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogMetaState::pack_to(PackedChangeLogMetaState* packed) const noexcept
{
  std::memset(packed, 0, sizeof(PackedChangeLogMetaState));

  packed->active_blocks_lower_bound = this->block_range.lower_bound.value();
  packed->active_blocks_upper_bound = this->block_range.upper_bound.value();
  packed->trim_edit_offset = this->trim_edit_offset.value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogMetaState PackedChangeLogMetaState::unpack() const noexcept
{
  ChangeLogMetaState unpacked;

  unpacked.block_range.lower_bound = BlockIndex{this->active_blocks_lower_bound.value()};
  unpacked.block_range.upper_bound = BlockIndex{this->active_blocks_upper_bound.value()};
  unpacked.trim_edit_offset = EditOffset{this->trim_edit_offset.value()};

  return unpacked;
}

}  // namespace turtle_kv
