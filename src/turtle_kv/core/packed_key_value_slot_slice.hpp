//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CORE_PACKED_KEY_VALUE_SLOT_SLICE_HPP

#include <turtle_kv/core/packed_key_value_slot.hpp>

#include <turtle_kv/import/slice.hpp>

#include <variant>

namespace turtle_kv {

using PackedKeyValueSlotSlice = std::variant<  //
    Slice<const PackedKeyValueSlotRef>,
    Slice<const PackedKeyValueSlotPtr>>;

struct ToPackedKeyValueSlotSlice {
  PackedKeyValueSlotSlice operator()(const Slice<const PackedKeyValueSlotRef>& ref_slice)
  {
    return PackedKeyValueSlotSlice{ref_slice};
  }

  PackedKeyValueSlotSlice operator()(const Slice<const PackedKeyValueSlotPtr>& ptr_slice)
  {
    return PackedKeyValueSlotSlice{ptr_slice};
  }
};

}  // namespace turtle_kv
