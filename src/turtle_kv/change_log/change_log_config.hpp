//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_CONFIG_HPP

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/interval.hpp>

#if BATT_PLATFORM_IS_LINUX
#include <limits.h>
#endif

namespace turtle_kv {

struct PackedChangeLogConfig;

struct ChangeLogConfig {
  BlockSize block_size;
  BlockCount block_count;
  FileOffset block0_offset;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static ChangeLogConfig with_default_values() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void pack_to(PackedChangeLogConfig* packed_config) const noexcept;

  /** \brief The maximum number of blocks which can be written in a single multi-chunk write.
   */
  usize max_write_batch_size() const noexcept
  {
    return
#if BATT_PLATFORM_IS_LINUX
        IOV_MAX;
#else
        128 * kKiB / this->block_size;
#endif
  }

  /** \brief Returns the file offset corresponding to the end of the last block.
   */
  FileOffset last_block_end_offset() const noexcept
  {
    return this->block_offset_from_index(BlockIndex{this->block_count});
  }

  /** \brief Returns the file offset of the beginning of the specific block.
   */
  FileOffset block_offset_from_index(BlockIndex block_index) const noexcept
  {
    return FileOffset{this->block0_offset + (this->block_size * block_index)};
  }

  /** \brief Returns the index of the block that *ends* at `block_end_offset`.
   */
  BlockIndex block_index_from_end_offset(FileOffset block_end_offset) const noexcept
  {
    if (block_end_offset == this->block0_offset) {
      return BlockIndex{this->block_count - 1};
    }

    FileOffset block_begin_offset{block_end_offset - this->block_size};
    BlockIndex block_index{(block_begin_offset - this->block0_offset) / this->block_size};

    BATT_CHECK_EQ(block_begin_offset, this->block_offset_from_index(block_index))
        << "The passed `block_end_offset` must be aligned to the block size!"
        << BATT_INSPECT(this->block_size) << BATT_INSPECT(block_end_offset)
        << BATT_INSPECT(block_index);

    return block_index;
  }

  /** \brief Calculates and returns the maximum recovery block range, given the passed Interval.
   */
  Interval<BlockIndex> block_range_to_recover(
      const Interval<BlockIndex>& read_block_range) const noexcept
  {
    Interval<BlockIndex> block_range = read_block_range;

    // The lower bound is always exact, since it is written atomically to storage along with each
    // update to the trim edit offset. However, the last true block may be up to
    // `max_write_batch_size()` blocks after the saved upper bound, but it is never past the maximum
    // (lower_bound + count).
    //
    block_range.upper_bound =
        BlockIndex{std::min<i64>(read_block_range.upper_bound + this->max_write_batch_size(),
                                 read_block_range.lower_bound + this->block_count)};

    return block_range;
  }

  /** \brief Increments the passed block index variable, with no wrap-around.
   */
  BlockIndex& increment_no_wrap(BlockIndex& block_index) const noexcept
  {
    block_index = BlockIndex{block_index + 1};
    return block_index;
  }

  /** \brief Increments the passed block index variable, applying wrap-around.
   */
  BlockIndex& increment_with_wrap(BlockIndex& block_index) const noexcept
  {
    BATT_CHECK_LT(block_index, this->block_count);

    if (this->increment_no_wrap(block_index) == this->block_count) {
      block_index = BlockIndex{0};
    }
    return block_index;
  }

  /** \brief Returns a copy of the passed BlockIndex, wrapped so it fits within the valid range.
   */
  BlockIndex wrap_block_index(BlockIndex index) const noexcept
  {
    return BlockIndex{index % this->block_count};
  }

  /** \brief Increments the lower bound of the passed interval, wrapping around at
   * this->block_count while maintaining the size of the interval.
   */
  Interval<BlockIndex>& increment_lower_bound(Interval<BlockIndex>& block_range) const noexcept
  {
    this->check_invariants(block_range);
    auto on_scope_exit = batt::finally([&] {
      this->check_invariants(block_range);
    });

    this->increment_no_wrap(block_range.lower_bound);

    return this->wrap_block_range(block_range);
  }

  /** \brief If the lower bound at least this->block_count, shifts upper and lower bound down by
   * block_count, modifying the passed interval so that afterwards the size is preserved, and
   * lower_bound is in [0, this->block_count).
   */
  Interval<BlockIndex>& wrap_block_range(Interval<BlockIndex>& block_range) const noexcept
  {
    BATT_CHECK_LT(block_range.lower_bound, this->block_count * 2);

    if (block_range.lower_bound >= this->block_count) {
      block_range.lower_bound = BlockIndex{block_range.lower_bound - this->block_count};
      block_range.upper_bound = BlockIndex{block_range.upper_bound - this->block_count};
    }
    return block_range;
  }

  /** \brief Increments the upper bound of the passed interval.
   */
  Interval<BlockIndex>& increment_upper_bound(Interval<BlockIndex>& block_range) const noexcept
  {
    this->check_invariants(block_range);
    auto on_scope_exit = batt::finally([&] {
      this->check_invariants(block_range);
    });

    this->increment_no_wrap(block_range.upper_bound);

    return block_range;
  }

  /** \brief Shifts the passed interval up by one, preserving the size.
   */
  Interval<BlockIndex>& increment_block_range(Interval<BlockIndex>& block_range) const noexcept
  {
    this->check_invariants(block_range);
    auto on_scope_exit = batt::finally([&] {
      this->check_invariants(block_range);
    });

    this->increment_no_wrap(block_range.lower_bound);
    this->increment_no_wrap(block_range.upper_bound);

    return this->wrap_block_range(block_range);
  }

  /** \brief Returns the upper bound of the passed interval, with wrap-around.
   */
  BlockIndex wrapped_upper_bound(const Interval<BlockIndex>& block_range) const noexcept
  {
    this->check_invariants(block_range);
    if (block_range.upper_bound < this->block_count) {
      return block_range.upper_bound;
    }
    const BlockIndex wrapped{block_range.upper_bound - this->block_count};
    BATT_CHECK_LE(wrapped, this->block_count);
    return wrapped;
  }

  /** \brief Returns the second passed BlockIndex, "unwrapped" if necessary to make it >= the first.
   */
  BlockIndex unwrap_block_index(BlockIndex lower_bound, BlockIndex index) const noexcept
  {
    if (index < lower_bound) {
      return BlockIndex{index + this->block_count};
    }
    return index;
  }

  /** \brief Modifies the (logical) upper bound of the passed block range to include index.
   */
  void extend_block_range_to_include(Interval<BlockIndex>& block_range,
                                     BlockIndex index) const noexcept
  {
    index = this->unwrap_block_index(block_range.lower_bound, index);
    this->increment_no_wrap(index);
    block_range.upper_bound = std::max(block_range.upper_bound, index);
  }

  /** \brief Panics if the passed (logical) interval is not well formed for this configuration.
   *
   * block_range.lower_bound is a physical index; block_range.upper_bound is logical.
   */
  void check_invariants(const Interval<BlockIndex>& block_range) const noexcept
  {
    BATT_CHECK_LE(0, block_range.lower_bound);
    BATT_CHECK_LT(block_range.lower_bound, this->block_count);
    BATT_CHECK_LE(block_range.lower_bound, block_range.upper_bound);
    BATT_CHECK_LE(block_range.upper_bound - block_range.lower_bound, this->block_count);
  }
};

inline bool operator==(const ChangeLogConfig& l, const ChangeLogConfig& r)
{
  return l.block_size == r.block_size       //
         && l.block_count == r.block_count  //
         && l.block0_offset == r.block0_offset;
}

inline bool operator!=(const ChangeLogConfig& l, const ChangeLogConfig& r)
{
  return !(l == r);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Packed representation of a ChangeLogConfig.
 */
struct PackedChangeLogConfig {
  static constexpr u64 kMagic = 0x53ee6863bf7a1254ull;

  big_u64 magic;
  little_i64 block_size;
  little_i64 block_count;
  little_i64 block0_offset;

  u8 reserved_[32];

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogConfig unpack() const noexcept;
};

static_assert(sizeof(PackedChangeLogConfig) == 64);

}  // namespace turtle_kv
