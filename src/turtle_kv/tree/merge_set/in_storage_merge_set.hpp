#pragma once
#define TURTLE_KV_TREE_MERGE_SET_IN_STORAGE_MERGE_SET_HPP

#include "fake_leaf.hpp"

#include <turtle_kv/util/piecewise_filter.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

#include <string>

namespace turtle_kv {
namespace merge_set {

struct InStorageMergeSet {
  std::shared_ptr<const FakeLeaf> leaf_;
  std::string key_upper_bound_;
  PiecewiseFilter<usize> filter_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Interval<std::string_view> seek_impl(u64 byte_size,
                                       const std::string_view& key_upper_bound) const noexcept
  {
    const usize block_count = this->leaf_->block_count();
    const usize block_size = this->leaf_->block_size();

    usize live_i = this->filter_.live_lower_bound(0);
    usize block_i = this->leaf_->block_containing_index(live_i);

    while (block_i < block_count) {
      if (byte_size < block_size) {
        break;
      }

      byte_size -= block_size;
      live_i = this->filter_.live_lower_bound(this->leaf_->block_starts_[block_i + 1]);
      block_i = this->leaf_->block_containing_index(live_i);
    }

    return {
        (block_i + 0 < block_count) ? this->leaf_->block_keys_[block_i + 0] : key_upper_bound,
        (block_i + 1 < block_count) ? this->leaf_->block_keys_[block_i + 1] : key_upper_bound,
    };
  }
};

}  // namespace merge_set
}  // namespace turtle_kv
