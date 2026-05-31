//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_LEAF_PAGE2_HPP

#include <turtle_kv/core/packed_key_value2.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_pointer.hpp>

namespace turtle_kv {

struct PackedLeafPageHeader2 {
  static constexpr u64 kMagic = 0x6456beb7f9558445ull;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u32 key_count;                                                  // +4 = 12
  u32 total_packed_size;                                          // +4 = 16
  llfs::PackedPointer<llfs::PackedArray<PackedKeyValue2>> items;  // +4 = 20
  u8 pad_[12];                                                    // +12 = 32
#if 0
  u32 index_step;                                                // +4 = 16
  u32 index_size;                                                // +4 = 20
  llfs::PackedPointer<const llfs::PackedBPTrie> trie_index;      // +4 = 32
#endif
};

}  // namespace turtle_kv
