//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>
#include <turtle_kv/tree/leaf/packed_leaf_block.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class InMemoryBlockLoader
{
 public:
  explicit InMemoryBlockLoader(const PackedBlockedLeafPage* leaf) noexcept : leaf_{leaf}
  {
  }

  StatusOr<const PackedLeafBlock*> load_block(u32 block_index) noexcept
  {
    return &*(this->leaf_->blocks_begin() + block_index);
  }

 private:
  const PackedBlockedLeafPage* leaf_;
};

}  // namespace testing
}  // namespace turtle_kv
