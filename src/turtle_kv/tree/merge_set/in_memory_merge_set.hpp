#pragma once
#define TURTLE_KV_TREE_MERGE_SET_EMPTY_MERGE_SET_HPP

#include "fake_key_value.hpp"

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/slice.hpp>

#include <memory>
#include <vector>

namespace turtle_kv {
namespace merge_set {

struct InMemoryMergeSet {
  std::shared_ptr<std::vector<FakeKeyValue>> storage_;
  Interval<std::string_view> key_range_;
  Interval<usize> index_range_;
  //----- --- -- -  -  -   -
  std::string key_upper_bound_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Slice<const FakeKeyValue> live_slice() const noexcept
  {
    return {
        (*this->storage_).data() + this->index_range_.lower_bound,
        (*this->storage_).data() + this->index_range_.upper_bound,
    };
  }

  Interval<std::string_view> seek_impl(u64 byte_size,
                                       const std::string_view& key_upper_bound) const noexcept
  {
    usize total = 0;
    usize index = this->index_range_.lower_bound;
    for (const FakeKeyValue& kv : this->live_slice()) {
      const usize n = packed_sizeof(kv);
      if (total + n > byte_size) {
        break;
      }
      total += n;
      ++index;
    }
    if (index == this->storage_->size()) {
      return {
          get_key((*this->storage_)[index]),
          key_upper_bound,
      };
    }
    return {
        get_key((*this->storage_)[index]),
        get_key((*this->storage_)[index + 1]),
    };
  }
};

}  // namespace merge_set
}  // namespace turtle_kv
