#pragma once
#define TURTLE_KV_TREE_MERGE_SET_FAKE_KEY_VALUE_HPP

#include "fake_key_value.hpp"

#include <turtle_kv/import/int_types.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace turtle_kv {
namespace merge_set {

struct FakeBlock {
  std::vector<FakeKeyValue> items_;
};

struct FakeLeaf {
  std::vector<std::string> block_keys_;
  std::vector<usize> block_starts_;
  std::vector<FakeBlock> blocks_;
  usize block_size_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize block_count() const noexcept
  {
    return this->blocks_.size();
  }

  usize block_size() const noexcept
  {
    return this->block_size_;
  }

  usize block_containing_index(usize index) const noexcept
  {
    BATT_CHECK(!this->block_starts_.empty());

    const auto second = std::next(this->block_starts_.begin());
    auto iter = std::upper_bound(second, this->block_starts_.end(), index);

    return std::distance(second, iter);
  }
};

}  // namespace merge_set
}  // namespace turtle_kv
