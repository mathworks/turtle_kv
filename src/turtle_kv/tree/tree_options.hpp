#pragma once

#include <turtle_kv/config.hpp>

#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/math.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/config.hpp>
#include <llfs/packed_bloom_filter_page.hpp>
#include <llfs/packed_bytes.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/page_size.hpp>
#include <llfs/varint.hpp>

#include <batteries/assert.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/env.hpp>

#include <array>
#include <cmath>

namespace turtle_kv {

// TODO [tastolfi 2025-03-10] make this dynamic/adaptive
//
constexpr usize kTrieIndexReserveSize = 16384;

constexpr usize kMaxTreeHeight = llfs::kMaxPageRefDepth - 1;

class TreeOptions;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const TreeOptions& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

class TreeOptions
{
 public:
  friend std::ostream& operator<<(std::ostream& out, const TreeOptions& t);

  using Self = TreeOptions;

  struct GlobalOptions {
    std::atomic<bool> page_cache_obsolete_hints{false};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u16 kDefaultFilterBitsPerKey = 12;
  static constexpr u32 kDefaultKeySizeHint = 24;
  static constexpr u32 kDefaultValueSizeHint = 100;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self with_default_values();

  static GlobalOptions& global_options();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageSize node_size() const
  {
    return llfs::PageSize{batt::checked_cast<u32>(u64{1} << this->node_size_log2_)};
  }

  llfs::PageSizeLog2 node_size_log2() const
  {
    return llfs::PageSizeLog2{this->node_size_log2_};
  }

  Self& set_node_size(u64 size)
  {
    this->node_size_log2_ = log2_ceil(size);
    BATT_CHECK_EQ(size, this->node_size()) << "node_size must be a power of 2";
    return *this;
  }

  Self& set_node_size_log2(u8 size_log2)
  {
    this->node_size_log2_ = size_log2;
    return *this;
  }

  //----- --- -- -  -  -   -

  llfs::PageSize leaf_size() const
  {
    return llfs::PageSize{BATT_CHECKED_CAST(u32, u64{1} << this->leaf_size_log2_)};
  }

  llfs::PageSizeLog2 leaf_size_log2() const
  {
    return llfs::PageSizeLog2{this->leaf_size_log2_};
  }

  Self& set_leaf_size(u64 size)
  {
    this->leaf_size_log2_ = log2_ceil(size);
    BATT_CHECK_EQ(size, this->leaf_size()) << "leaf_size must be a power of 2";
    return *this;
  }

  Self& set_leaf_size_log2(u8 size_log2)
  {
    this->leaf_size_log2_ = size_log2;
    return *this;
  }

  /** \brief The max number of bytes in the payload (data) of a leaf page.
   */
  usize leaf_data_size() const;

  usize flush_size() const
  {
    return this->leaf_data_size() - kTrieIndexReserveSize;
  }

  //----- --- -- -  -  -   -

  Self& set_filter_bits_per_key(Optional<u16> bits_per_key)
  {
    this->filter_bits_per_key_ = bits_per_key;
    return *this;
  }

  usize filter_bits_per_key() const
  {
    return this->filter_bits_per_key_.value_or(Self::kDefaultFilterBitsPerKey);
  }

  Self& set_filter_page_size_log2(u8 size_log2)
  {
    this->filter_page_size_log2_ = size_log2;
    return *this;
  }

  Self& set_filter_page_size(u64 size)
  {
    return this->set_filter_page_size_log2(log2_ceil(size));
  }

  llfs::PageSizeLog2 filter_page_size_log2() const
  {
    if (this->filter_page_size_log2_) {
      return llfs::PageSizeLog2{*this->filter_page_size_log2_};
    }

    const usize expected_filter_bits =
        batt::round_up_bits(9, this->expected_items_per_leaf() * filter_bits_per_key())
#if TURTLE_KV_USE_QUOTIENT_FILTER
        * 100 / 88
#endif
        ;

    const usize expected_filter_bytes = expected_filter_bits / 8;

    const usize expected_filter_page_size = sizeof(llfs::PackedPageHeader) +
                                            sizeof(llfs::PackedBloomFilterPage) +
                                            expected_filter_bytes;

    return llfs::PageSizeLog2{static_cast<u32>(log2_ceil(expected_filter_page_size))};
  }

  llfs::PageSize filter_page_size() const
  {
    return llfs::PageSize{u32{1} << this->filter_page_size_log2()};
  }

  //----- --- -- -  -  -   -

  u32 key_size_hint() const
  {
    return this->key_size_hint_;
  }

  Self& set_key_size_hint(u32 n_bytes)
  {
    this->key_size_hint_ = n_bytes;
    return *this;
  }

  u32 value_size_hint() const
  {
    return this->value_size_hint_;
  }

  Self& set_value_size_hint(u32 n_bytes)
  {
    this->value_size_hint_ = n_bytes;
    return *this;
  }

  usize expected_item_size() const
  {
    static constexpr usize kKeyHeader = sizeof(llfs::PackedBytes);
    static constexpr usize kValueHeader = sizeof(llfs::PackedBytes);

    return kKeyHeader + this->key_size_hint_ + kValueHeader + this->value_size_hint_;
  }

  usize expected_items_per_leaf() const
  {
    return this->leaf_data_size() / this->expected_item_size();
  }

  usize expected_item_slot_size() const
  {
    static constexpr usize kVolumeEventHeader = 1;
    static constexpr usize kTabletEventHeader = 1;

    const usize payload_size = this->expected_item_size() + kVolumeEventHeader + kTabletEventHeader;

    return llfs::packed_sizeof_varint(payload_size) + payload_size;
  }

  u32 max_item_size() const
  {
    return this->max_item_size_.value_or(this->node_size() / 7);
  }

  Self& set_max_item_size(u32 n)
  {
    this->max_item_size_ = n;
    return *this;
  }

  //----- --- -- -  -  -   -

  llfs::MaxRefsPerPage max_page_refs_per_node() const
  {
    constexpr usize kPackedPageRefSizeEstimate = sizeof(u64) * 2;

    return llfs::MaxRefsPerPage{this->node_size() / kPackedPageRefSizeEstimate};
  }

  llfs::MaxRefsPerPage max_page_refs_per_leaf() const
  {
    constexpr usize kPackedPageRefSizeEstimate = sizeof(u64) * 2;

    return llfs::MaxRefsPerPage{this->leaf_size() / kPackedPageRefSizeEstimate};
  }

  //----- --- -- -  -  -   -

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  TreeOptions() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The node page size.
  //
  u8 node_size_log2_ = 12 /*4kb*/;

  // The leaf page size.
  //
  u8 leaf_size_log2_ = 21 /*2mb*/;

  // The target filter bits per key.
  //
  Optional<u16> filter_bits_per_key_ = None;

  // The leaf (Bloom) filter page size.
  //
  Optional<u8> filter_page_size_log2_ = None;

  // The maximum size of a key/value pair (default: calculate based on node size).
  //
  Optional<u32> max_item_size_ = None;

  // Expected (average) size of a key (in bytes).
  //
  u32 key_size_hint_ = Self::kDefaultKeySizeHint;

  // Expected (average) size of a value (in bytes).
  //
  u32 value_size_hint_ = Self::kDefaultValueSizeHint;
};

}  // namespace turtle_kv
