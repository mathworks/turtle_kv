//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_LEAF_BLOCK_SCANNER_HPP

#include <turtle_kv/tree/packed_blocked_leaf_page.hpp>

#include <turtle_kv/core/packed_key_value_slot_slice.hpp>

#include <turtle_kv/import/status.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>

#include <concepts>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
concept PackedLeafBlockProvider = requires(T& provider, usize block_index) {
  { provider.get_block(block_index) } -> std::convertible_to<StatusOr<const PackedLeafBlock&>>;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <PackedLeafBlockProvider BlockProviderT>
class PackedLeafBlockScanner
{
 public:
  class Impl
  {
   public:
    using Item = StatusOr<PackedKeyValueSlotSlice>;

    Optional<Item> poll() noexcept
    {
    }

    Optional<Item> next() noexcept
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    void advance() noexcept
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    PackedBlockedLeafPage::HeaderShardView header_;

    BlockProviderT& provider_;

    PiecewiseFilter<u32>& filter_;

    usize block_index_;

    Optional<StatusOr<const PackedLeafBlock&>> block_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  Impl* impl_;
};

}  // namespace turtle_kv
