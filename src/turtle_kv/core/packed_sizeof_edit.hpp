#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/int_types.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedSizeOfEdit {
  static constexpr usize kPackedKeyLengthSize = sizeof(little_u32);
  static constexpr usize kPackedValueOffsetSize = sizeof(little_u32);
  static constexpr usize kPackedValueOpSize = 1;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize operator()(const usize key_size, const usize value_size) const
  {
    return
        // Key metadata
        //
        kPackedKeyLengthSize +  //

        // Key data
        //
        key_size +                //
        kPackedValueOffsetSize +  //

        // Value data
        //
        kPackedValueOpSize +  //
        value_size;
  }

  template <typename T>
  usize operator()(const T& item) const
  {
    return this->operator()(get_key(item).size(), get_value(item).size());
  }
};

}  // namespace turtle_kv
