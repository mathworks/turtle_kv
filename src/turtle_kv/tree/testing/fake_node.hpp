#pragma once

#include <turtle_kv/tree/testing/fake_level.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/assert.hpp>

#include <string_view>
#include <vector>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct FakeNode {
  std::vector<std::string_view> pivot_keys_;
  std::vector<usize> items_per_pivot_;
  FakeLevel level_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize pivot_count() const
  {
    return this->pivot_keys_.size() - 1;
  }

  std::string_view get_pivot_key(usize i) const
  {
    BATT_CHECK_LT(i, this->pivot_keys_.size());

    return this->pivot_keys_[i];
  }

  Slice<const std::string_view> get_pivot_keys() const
  {
    return as_slice(this->pivot_keys_);
  }
};

}  // namespace testing
}  // namespace turtle_kv
