#pragma once

#include <turtle_kv/mem_table_entry.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>

#include <array>
#include <ostream>
#include <vector>

namespace turtle_kv {

struct BonsaiFilterScanStats {
  usize over_max_len_count = 0;
  usize block_missing_prefix_count = 0;
};

template <usize kMaxKeyLen = 256, usize kFilterWords = 16, usize kNodesPerBlock = 4>
class BonsaiFilter
{
 public:
  using Self = BonsaiFilter;

  using ScanStats = BonsaiFilterScanStats;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct Node {
    /** \brief A 256-bit set of the branches for this node.
     */
    std::array<u64, 4> branches;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Branch-related

    void insert_branch(i32 branch);

    bool contains_branch(i32 branch) const;

    i32 first_branch() const;

    i32 last_branch() const;

    i32 next_branch(i32 branch) const;

    i32 prev_branch(i32 branch) const;
  };

  /** \brief A hash table block in the filter.
   */
  struct Block {
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief A single-hash-function Bloom Filter storing the set of keys and prefixes which hash
     * to this block.
     */
    std::array<u64, kFilterWords> filter;

    /** \brief The Nodes for this block.
     */
    std::array<Node, kNodesPerBlock> nodes;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Prefix-related

    static usize prefix_word_i_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 6) % kFilterWords;
    }

    static usize prefix_bit_i_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 0) % 64;
    }

    void insert_prefix(u64 hash_val);

    bool contains_prefix(u64 hash_val) const;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Key-related

    static usize key_word_i_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 23) % kFilterWords;
    }

    static usize key_bit_i_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 17) % 64;
    }

    void insert_key(u64 hash_val);

    bool contains_key(u64 hash_val) const;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Node-related

    static usize node_i_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 31) % kNodesPerBlock;
    }

    const Node& const_node(u64 hash_val) const
    {
      return this->nodes[Block::node_i_from_hash_val(hash_val)];
    }

    Node& mutable_node(u64 hash_val)
    {
      return this->nodes[Block::node_i_from_hash_val(hash_val)];
    }

  } __attribute__((aligned(32)));

  static_assert(alignof(Block) == 32);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static ScanStats& thread_scan_stats()
  {
    thread_local ScanStats scan_stats_;
    return scan_stats_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultStrHash str_hash_;
  std::vector<Block> blocks_;
  const batt::fixed_point::LinearProjection<u64, usize> block_from_hash_val_{this->blocks_.size()};
  usize max_key_len_ = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BonsaiFilter(usize block_count) noexcept : blocks_(block_count)
  {
    std::memset((void*)this->blocks_.data(), 0, this->blocks_.size() * sizeof(Block));
  }

  auto dump_config() const
  {
    return [this](std::ostream& out) {
      out << "BonsaiFilter<" << kMaxKeyLen << ", " << kFilterWords << ", " << kNodesPerBlock
          << ">{.blocks=[" << this->blocks_.size() << "], block_size=" << sizeof(Block)
          << ", filter_size=" << (kFilterWords * 64) << " bits, nodes_per_block=" << kNodesPerBlock
          << ",}";
    };
  }

  usize byte_size() const
  {
    return this->blocks_.size() * sizeof(Block);
  }

  void put(const std::string_view& key)
  {
    this->max_key_len_ = std::max(key.size(), this->max_key_len_);

    for (usize prefix_len = 0;; ++prefix_len) {
      const u64 prefix_hash_val = this->str_hash_(key.substr(0, prefix_len));
      const usize block_i = this->block_from_hash_val_(prefix_hash_val);
      Block& block = this->blocks_[block_i];
      Node& node = block.mutable_node(prefix_hash_val);

      block.insert_prefix(prefix_hash_val);

      if (prefix_len == key.size()) {
        node.insert_branch(0);
        block.insert_key(prefix_hash_val);
        break;
      }

      node.insert_branch(key[prefix_len]);
    }
  }

  template <typename EmitFn>
  void scan_all(EmitFn&& emit_fn, ScanStats& stats = Self::thread_scan_stats()) const
  {
    std::array<char, kMaxKeyLen> buffer;

    this->scan_all_impl(buffer.data(), 0, BATT_FORWARD(emit_fn), stats);
  }

  template <typename EmitFn>
  void scan_all_impl(char* buffer, usize prefix_len, EmitFn&& emit_fn, ScanStats& stats) const
  {
    if (prefix_len > this->max_key_len_) {
      ++stats.over_max_len_count;
      return;
    }

    const std::string_view prefix{buffer, prefix_len};
    const u64 prefix_hash_val = this->str_hash_(prefix);
    const usize block_i = this->block_from_hash_val_(prefix_hash_val);
    const Block& block = this->blocks_[block_i];

    if (!block.contains_prefix(prefix_hash_val)) {
      ++stats.block_missing_prefix_count;
      return;
    }

    const Node& node = block.const_node(prefix_hash_val);

    // If the key filter contains this prefix and branches contains '\0' (null-terminator), then
    // emit the prefix as a found key.
    //
    if (prefix_len != 0 && block.contains_key(prefix_hash_val) && (node.branches[0] & 1) == 1) {
      emit_fn(prefix);
    }

    char ch_base = 0;
    for (i32 word_i = 0; word_i < 4; ++word_i, ch_base += 64) {
      const u64 mask = node.branches[word_i];
      for (i32 bit_i = batt::first_bit(mask); bit_i < 64; bit_i = batt::next_bit(mask, bit_i)) {
        const char ch = (char)(bit_i + ch_base);
        buffer[prefix_len] = ch;
        this->scan_all_impl(buffer, prefix_len + 1, emit_fn, stats);
      }
    }
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline std::ostream& operator<<(std::ostream& out, const BonsaiFilterScanStats& t)
{
  return out << "ScanStats{.over_max_len_count=" << t.over_max_len_count
             << ", .block_missing_prefix_count=" << t.block_missing_prefix_count << "}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline void BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Block::insert_prefix(
    u64 prefix_hash_val)
{
  const usize word_i = Block::prefix_word_i_from_hash_val(prefix_hash_val);
  const usize bit_i = Block::prefix_bit_i_from_hash_val(prefix_hash_val);

  auto& prefix_filter = this->filter;

  prefix_filter[word_i] |= (u64{1} << bit_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline bool BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Block::contains_prefix(
    u64 prefix_hash_val) const
{
  const usize word_i = Block::prefix_word_i_from_hash_val(prefix_hash_val);
  const usize bit_i = Block::prefix_bit_i_from_hash_val(prefix_hash_val);

  auto& prefix_filter = this->filter;

  return (prefix_filter[word_i] & (u64{1} << bit_i)) != 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline void BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Block::insert_key(
    u64 key_hash_val)
{
  const usize word_i = Block::key_word_i_from_hash_val(key_hash_val);
  const usize bit_i = Block::key_bit_i_from_hash_val(key_hash_val);

  auto& key_filter = this->filter;

  key_filter[word_i] |= (u64{1} << bit_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline bool BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Block::contains_key(
    u64 key_hash_val) const
{
  const usize word_i = Block::key_word_i_from_hash_val(key_hash_val);
  const usize bit_i = Block::key_bit_i_from_hash_val(key_hash_val);

  auto& key_filter = this->filter;

  return (key_filter[word_i] & (u64{1} << bit_i)) != 0;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline void BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::insert_branch(i32 branch)
{
  const u16 index = branch;
  this->branches[index / 64] |= (u64{1} << (index % 64));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline bool BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::contains_branch(
    i32 branch) const
{
  const u16 index = branch;
  return (this->branches[index / 64] & (u64{1} << (index % 64))) != 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline i32 BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::first_branch() const
{
  if (this->branches[0] != 0) {
    return batt::first_bit(this->branches[0]);
  }
  if (this->branches[1] != 0) {
    return batt::first_bit(this->branches[1]) + 64;
  }
  if (this->branches[2] != 0) {
    return batt::first_bit(this->branches[2]) + 128;
  }
  if (this->branches[3] != 0) {
    return batt::first_bit(this->branches[3]) + 192;
  }
  return 256;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline i32 BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::last_branch() const
{
  if (this->branches[3] != 0) {
    return batt::last_bit(this->branches[3]) + 192;
  }
  if (this->branches[2] != 0) {
    return batt::last_bit(this->branches[2]) + 128;
  }
  if (this->branches[1] != 0) {
    return batt::last_bit(this->branches[1]) + 64;
  }
  if (this->branches[0] != 0) {
    return batt::last_bit(this->branches[0]);
  }
  return -1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline i32 BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::next_branch(
    i32 branch) const
{
  const i32 word_i = branch / 64;
  const i32 bit_i = branch % 64;
  const i32 next_bit_i = batt::next_bit(this->branches[word_i], bit_i);

  if (next_bit_i != 64) {
    return (word_i * 64) + next_bit_i;
  }

  switch (word_i) {
    case 0:
      if (this->branches[1] != 0) {
        return batt::first_bit(this->branches[1]) + 64;
      }
      // fall-through
    case 1:
      if (this->branches[2] != 0) {
        return batt::first_bit(this->branches[2]) + 128;
      }
      // fall-through
    case 2:
      if (this->branches[3] != 0) {
        return batt::first_bit(this->branches[3]) + 192;
      }
      // fall-through
    case 3:
      break;
  }

  return 256;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kMaxKeyLen, usize kFilterWords, usize kNodesPerBlock>
inline i32 BonsaiFilter<kMaxKeyLen, kFilterWords, kNodesPerBlock>::Node::prev_branch(
    i32 branch) const
{
  const i32 word_i = branch / 64;
  const i32 bit_i = branch % 64;
  const i32 prev_bit_i = batt::prev_bit(this->branches[word_i], bit_i);

  if (prev_bit_i != -1) {
    return (word_i * 64) + prev_bit_i;
  }

  switch (word_i) {
    case 3:
      if (this->branches[2] != 0) {
        return batt::last_bit(this->branches[2]) + 128;
      }
      // fall-through
    case 2:
      if (this->branches[2] != 0) {
        return batt::last_bit(this->branches[1]) + 64;
      }
      // fall-through
    case 1:
      if (this->branches[0] != 0) {
        return batt::last_bit(this->branches[0]);
      }
      // fall-through
    case 0:
      break;
  }

  return -1;
}

}  // namespace turtle_kv
