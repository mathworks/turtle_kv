//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_BLOCKED_LEAF_PAGE_LOADER_CONCEPT_HPP

#include <turtle_kv/tree/leaf/packed_blocked_leaf_page.hpp>
#include <turtle_kv/tree/leaf/packed_leaf_block.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id.hpp>

#include <concepts>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept BlockedLeafPageLoader = requires(T& loader, llfs::PageId page_id, u32 block_index) {
  { loader.set_page(page_id) } -> std::convertible_to<StatusOr<const PackedBlockedLeafPage*>>;
  { loader.load_block(block_index) } -> std::convertible_to<StatusOr<const PackedLeafBlock*>>;
};

}  // namespace turtle_kv