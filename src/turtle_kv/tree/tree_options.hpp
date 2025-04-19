#pragma once

#include <turtle_kv/config.hpp>

#include <turtle_kv/vqf_filter_page_view.hpp>

#include <turtle_kv/core/packed_sizeof_edit.hpp>
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

  static constexpr usize kMaxLevels = 6;
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

  //----- --- -- -  -  -   -

  Self& set_min_flush_factor(double factor)
  {
    this->min_flush_factor_ = factor;
    return *this;
  }

  Self& set_max_flush_factor(double factor)
  {
    this->max_flush_factor_ = factor;
    return *this;
  }

  usize min_flush_size() const
  {
    const usize nominal = this->flush_size() * this->min_flush_factor_;
    return std::min(nominal, this->flush_size());
  }

  usize max_flush_size() const
  {
    const usize nominal = this->flush_size() * this->max_flush_factor_;
    return std::max(nominal, this->min_flush_size());
  }

  //----- --- -- -  -  -   -

  Self& set_filter_bits_per_key(Optional<u16> bits_per_key)
  {
    this->filter_bits_per_key_ = bits_per_key;
    return *this;
  }

  usize filter_bits_per_key() const
  {
    const usize bits_per_key = this->filter_bits_per_key_.value_or(Self::kDefaultFilterBitsPerKey);

#if TURTLE_KV_USE_QUOTIENT_FILTER
    return (bits_per_key == 0) ? 0 : std::max(kMinQuotientFilterBitsPerKey, bits_per_key);
#else
    return bits_per_key;
#endif
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

#if TURTLE_KV_USE_BLOOM_FILTER
    const usize expected_filter_bits =
        batt::round_up_bits(9, this->expected_items_per_leaf() * this->filter_bits_per_key());

    const usize expected_filter_bytes = expected_filter_bits / 8;

    const usize expected_filter_page_size = sizeof(llfs::PackedPageHeader) +
                                            sizeof(llfs::PackedBloomFilterPage) +
                                            expected_filter_bytes;

#elif TURTLE_KV_USE_QUOTIENT_FILTER

    const double load_factor_8bit = vqf_filter_load_factor<8>(this->filter_bits_per_key());
    const double load_factor_16bit = vqf_filter_load_factor<16>(this->filter_bits_per_key());

    const double items_per_leaf = this->expected_items_per_leaf();

    const double expected_slots_8bit = items_per_leaf / load_factor_8bit;
    const double expected_slots_16bit = items_per_leaf / load_factor_16bit;

    const usize filter_size_8bit = vqf_required_size<8>((u64)std::ceil(expected_slots_8bit));
    const usize filter_size_16bit = vqf_required_size<16>((u64)std::ceil(expected_slots_16bit));

    const usize expected_filter_page_size = std::max(filter_size_8bit, filter_size_16bit) +
                                            sizeof(llfs::PackedPageHeader) +
                                            sizeof(PackedVqfFilter);

#else
#error No filter type selected!
#endif

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
    return PackedSizeOfEdit::kPackedKeyLengthSize +    //
           this->key_size_hint_ +                      //
           PackedSizeOfEdit::kPackedValueOffsetSize +  //
           PackedSizeOfEdit::kPackedValueOpSize +      //
           this->value_size_hint_;
  }

  usize expected_items_per_leaf() const
  {
    return this->leaf_data_size() / this->expected_item_size();
  }

  //----- --- -- -  -  -   -

  usize trie_index_reserve_size() const
  {
    if (BATT_HINT_TRUE(this->trie_index_reserve_size_)) {
      return *this->trie_index_reserve_size_;
    }
    return ((this->expected_items_per_leaf() * this->key_size_hint() + 15) / 16) * 5 / 8;
  }

  Self& set_trie_index_reserve_size(Optional<usize> n_bytes)
  {
    this->trie_index_reserve_size_ = n_bytes;
    return *this;
  }

  llfs::PageSize trie_index_sharded_view_size() const
  {
    return llfs::PageSize{u32{1} << batt::log2_ceil(this->trie_index_reserve_size() + 256)};
  }

  usize flush_size() const
  {
    return this->leaf_data_size() - this->trie_index_reserve_size();
  }

  //----- --- -- -  -  -   -

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

  usize max_buffer_levels() const
  {
    return TreeOptions::kMaxLevels - this->buffer_level_trim();
  }

  u16 buffer_level_trim() const
  {
    return this->buffer_level_trim_;
  }

  Self& set_buffer_level_trim(u16 n)
  {
    BATT_CHECK_LT(n, TreeOptions::kMaxLevels);
    this->buffer_level_trim_ = n;
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
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

  // The number of levels in node update buffers *not* to use, in order to force more eager/greedy
  // compaction.
  //
  u16 buffer_level_trim_ = 0;

  Optional<usize> trie_index_reserve_size_;

  double min_flush_factor_ = 1.0;
  double max_flush_factor_ = 1.0;
};

}  // namespace turtle_kv
