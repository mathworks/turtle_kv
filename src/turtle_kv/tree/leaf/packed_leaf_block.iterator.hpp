//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_LEAF_PACKED_LEAF_BLOCK_ITERATOR_HPP

#include "packed_leaf_block.hpp"

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <boost/iterator/iterator_facade.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackedLeafBlock::Iterator
    : public boost::iterator_facade<        //
          PackedLeafBlock::Iterator,        // <- Derived
          const PackedLeafBlock,            // <- Value
          std::random_access_iterator_tag,  // <- CategoryOrTraversal
          const PackedLeafBlock&,           // <- Reference
          isize                             // <- Difference
          >
{
 public:
  using Self = Iterator;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = const PackedLeafBlock;
  using reference = const PackedLeafBlock&;

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  Iterator() = default;

  explicit Iterator(const PackedLeafBlock* block, isize block_size) noexcept
      : block_{block}
      , block_size_{block_size}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  reference dereference() const
  {
    return *this->block_;
  }

  bool equal(const Self& other) const
  {
    return this->block_ == other.block_ && this->block_size_ == other.block_size_;
  }

  void increment()
  {
    this->advance(1);
  }

  void decrement()
  {
    this->advance(-1);
  }

  void advance(isize delta)
  {
    this->block_ = static_cast<const PackedLeafBlock*>(
        advance_pointer(this->block_, delta * this->block_size_));
  }

  isize distance_to(const Self& other) const
  {
    return (byte_distance(this->block_, other.block_)) / this->block_size_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  const PackedLeafBlock* block() const noexcept
  {
    return this->block_;
  }

  usize block_size() const noexcept
  {
    return static_cast<usize>(this->block_size_);
  }

  isize block_isize() const noexcept
  {
    return this->block_size_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  const PackedLeafBlock* block_ = nullptr;
  isize block_size_ = 0;
};

}  // namespace turtle_kv
