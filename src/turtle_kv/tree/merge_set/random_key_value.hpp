#pragma once
#define TURTLE_KV_TREE_MERGE_SET_RANDOM_KEY_VALUE_HPP

#include "fake_key_value.hpp"
#include "fake_leaf.hpp"

#include <turtle_kv/import/int_types.hpp>

#include <batteries/utility.hpp>

#include <string>

namespace turtle_kv {
namespace merge_set {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Rng, typename PickLen, typename PickChar>
inline std::string random_str(Rng&& rng, PickLen&& pick_len, PickChar&& pick_char) noexcept
{
  const usize len = pick_len(rng);
  std::string s(len, '\0');
  for (usize i = 0; i < len; ++i) {
    s[i] = (char)pick_char(rng);
  }
  return s;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Rng,
          typename PickKeyLen,
          typename PickKeyChar,
          typename PickValueLen,
          typename PickValueChar>
inline FakeKeyValue random_key_value(Rng&& rng,
                                     PickKeyLen&& pick_key_len,
                                     PickKeyChar&& pick_key_char,
                                     PickValueLen&& pick_value_len,
                                     PickValueChar&& pick_value_char) noexcept
{
  return FakeKeyValue{
      .key_ = random_str(BATT_FORWRD(rng),  //
                         BATT_FORWARD(pick_key_len),
                         BATT_FORWARD(pick_key_char)),
      .value_ = random_str(BATT_FORWRD(rng),  //
                           BATT_FORWARD(pick_value_len),
                           BATT_FORWARD(pick_value_char)),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Rng,
          typename PickKeyLen,
          typename PickKeyChar,
          typename PickValueLen,
          typename PickValueChar>
inline std::vector<FakeKeyValue> random_block(usize size_limit,
                                              Rng&& rng,
                                              PickKeyLen&& pick_key_len,
                                              PickKeyChar&& pick_key_char,
                                              PickValueLen&& pick_value_len,
                                              PickValueChar&& pick_value_char) noexcept
{
  std::vector<FakeKeyValue> block;
  usize size = 0;
  for (;;) {
    block.emplace_back(random_key_value(BATT_FORWARD(rng),
                                        BATT_FORWARD(pick_key_len),
                                        BATT_FORWARD(pick_key_char),
                                        BATT_FORWARD(pick_value_len),
                                        BATT_FORWARD(pick_value_char)));
    size += packed_sizeof(block.back());
    if (size > size_limit) {
      block.pop_back();
      break;
    }
  }
  return block;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Rng,
          typename PickNumBlocks,
          typename PickKeyLen,
          typename PickKeyChar,
          typename PickValueLen,
          typename PickValueChar>
inline FakeLeaf random_leaf(usize block_size_limit,
                            Rng&& rng,
                            PickNumBlocks&& pick_num_blocks,
                            PickKeyLen&& pick_key_len,
                            PickKeyChar&& pick_key_char,
                            PickValueLen&& pick_value_len,
                            PickValueChar&& pick_value_char) noexcept
{
  FakeLeaf leaf;
  usize key_count = 0;

  leaf.block_size_ = block_size_limit;

  const usize num_blocks = pick_num_blocks(rng);
  for (usize i = 0; i < num_blocks; ++i) {
    FakeBlock block{
        .items_ = random_block(block_size_limit,
                               BATT_FORWARD(rng),
                               BATT_FORWARD(pick_key_len),
                               BATT_FORWARD(pick_key_char),
                               BATT_FORWARD(pick_value_len),
                               BATT_FORWARD(pick_value_char)),
    };

    leaf.block_starts_.push_back(key_count);
    leaf.block_keys_.push_back(std::string{get_key(block.items_.front())});
    leaf.blocks_.emplace_back(std::move(block));

    key_count += block.items_.size();
  }
  leaf.block_starts_.push_back(key_count);

  return leaf;
}

}  // namespace merge_set
}  // namespace turtle_kv
