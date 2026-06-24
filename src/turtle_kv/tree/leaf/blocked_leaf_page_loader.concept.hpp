//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_BLOCKED_LEAF_PAGE_LOADER_CONCEPT_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/import/status.hpp>

#include <llfs/page_id>

#include <concepts>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept BlockedLeafPageLoader = requires(T& loader, llfs::PageId page_id, BlockIndex block_i) {
  loader.release_block(page_id, block_i);
  { loader.load_block(page_id, block_i) } -> std::convertible_to<StatusOr<ConstBuffer>>;
};

}  // namespace turtle_kv
