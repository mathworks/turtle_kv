#pragma once

#include <turtle_kv/mem_table_entry.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <type_traits>

namespace turtle_kv {

class RangeIndex
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  /** \brief A hash table bucket/trie-node.  Represents the set of key prefixes which hash to the
   * same location in the table.
   */
  struct Bucket {
    static constexpr i32 bit_index_from_hash_val(u64 hash_val)
    {
      return (hash_val >> 11) % 64;
    }

    static constexpr u64 bit_mask_from_hash_val(u64 hash_val)
    {
      return u64{1} << Bucket::bit_index_from_hash_val(hash_val);
    }

    template <
        typename BucketT,
        typename AtomicT =
            std::conditional_t<std::is_const_v<BucketT>, const std::atomic<u64>, std::atomic_u64>>
    static AtomicT& filter_word_from_hash_val(BucketT* bucket, u64 hash_val)
    {
      return *reinterpret_cast<AtomicT*>(&this->prefix_filter[(hash_val >> 17) % 4]);
    }

    static constexpr i32 bit_index_from_next_char(u32 next_char)
    {
      return next_char % 64;
    }

    static constexpr u64 bit_mask_from_next_char(u32 next_char)
    {
      return u64{1} << Bucket::bit_index_from_next_char(next_char);
    }

    template <
        typename BucketT,
        typename AtomicT =
            std::conditional_t<std::is_const_v<BucketT>, const std::atomic<u64>, std::atomic_u64>>
    static AtomicT& children_word_from_next_char(BucketT* bucket, u32 next_char)
    {
      return *reinterpret_cast<AtomicT*>(&this->children[(next_char >> 6) % 4]);
    }

    //----- --- -- -  -  -   -

    /** \brief A k=1 Bloom Filter holding the set of prefixes currently represented by this bucket.
     */
    std::array<u64, 4> prefix_filter;

    /** \brief A 256-bit mask of the child (next char) descendants of this node.
     */
    std::array<u64, 4> children;

    //----- --- -- -  -  -   -

    void insert(u64 prefix_hash_val, u32 next_char)
    {
      std::atomic<u64>& filter_word = Bucket::filter_word_from_hash_val(this, prefix_hash_val);
      std::atomic<u64>& children_word = Bucket::children_word_from_next_char(this, next_char);

      filter_word.fetch_or(Bucket::bit_mask_from_hash_val(prefix_hash_val));
      children_word.fetch_or(Bucket::bit_mask_from_next_char(next_char));
    }

    bool contains_prefix(u64 prefix_hash_val) const
    {
      const std::atomic<u64>& filter_word =
          Bucket::filter_word_from_hash_val(this, prefix_hash_val);

      return (filter_word.load() & Bucket::bit_mask_from_hash_val(prefix_hash_val)) != 0;
    }

    bool has_null_terminator() const
    {
      return (this->children[0] & 1) == 1;
    }

  } __attribute__((aligned(64)));

  static_assert(sizeof(Bucket) == 64);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Scanner
  {
   public:
    std::string_view get_key() const
    {
      return std::string_view{
          this->buffer_.data(),
          this->stack_.size(),
      };
    }

   private:
    std::array<char, 256> buffer_;
    i32 char_;
    SmallVec<Bucket*, 256> stack_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultStrHash str_hash_;
  std::vector<Bucket> buckets_;
  const LinearProjection<u64, usize> bucket_from_hash_val_{this->buckets_.size()};
  std::array<u8, 256> max_prefix_len_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit RangeIndex(usize bucket_count) noexcept : buckets_(bucket_count)
  {
    std::memset((void*)this->buckets_.data(), 0, this->buckets_.size() * sizeof(Bucket));
    this->max_prefix_len_.fill(0);
  }

  void insert(const std::string_view& key)
  {
    BATT_CHECK_LT(key.size(), 256);

    for (usize prefix_len = 0; prefix_len <= key.size(); ++prefix_len) {
      const u64 prefix_hash_val = this->str_hash_(key.substr(0, prefix_len));
      const u32 next_char = (prefix_len == key.size()) ? '\0' : (u8)key[prefix_len];

      auto& max_prefix_len = this->max_prefix_len_[prefix_hash_val % this->max_prefix_len_.size()];
      max_prefix_len = std::max<u8>(max_prefix_len, prefix_len);

      const usize bucket_i = this->bucket_from_hash_val_(prefix_hash_val);
      Bucket& bucket = this->buckets_[bucket_i];

      bucket.insert(prefix_hash_val, next_char);
    }
  }

  std::vector<std::string> scan_all() const
  {
    std::vector<std::string> out;
    std::array<char, 64> buffer;

    this->scan_all_impl(buffer, 0, out);

    return out;
  }

  void scan_all_impl(std::array<char, 64>& buffer,
                     usize prefix_len,
                     std::vector<std::string>& out) const
  {
    const std::string_view prefix{buffer.data(), prefix_len};
    const u64 prefix_hash_val = this->str_hash_(prefix);
    const u16 max_prefix_len =
        this->max_prefix_len_[prefix_hash_val % this->max_prefix_len_.size()];

    if (prefix_len > max_prefix_len) {
      return;
    }

    const usize bucket_i = this->bucket_from_hash_val_(prefix_hash_val);
    const Bucket& bucket = this->buckets_[bucket_i];

    if (!bucket.contains_prefix(prefix_hash_val)) {
      return;
    }

    // If children contains '\0' (null-terminator), then add the prefix.
    //
    if (prefix_len != 0 && bucket.has_null_terminator()) {
      out.emplace_back(prefix);
    }

    char ch_base = 0;
    for (i32 word_i = 0; word_i < 4; ++word_i, ch_base += 64) {
      const u64 mask = bucket.children[word_i];
      for (i32 bit_i = batt::first_bit(mask); bit_i < 64; bit_i = batt::next_bit(mask, bit_i)) {
        const char ch = (char)(bit_i + ch_base);
        buffer[prefix_len] = ch;
        this->scan_all_impl(buffer, prefix_len + 1, out);
      }
    }
  }
};

}  // namespace turtle_kv
