#pragma once

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/math.hpp>

#include <array>
#include <atomic>
#include <memory>
#include <string_view>

namespace turtle_kv {

class MemTableOrderedIndex
{
 public:
  using Self = MemTableOrderedIndex;

  static constexpr u64 kHashSeed = 0xca4c99891b5125a6ull;
  static constexpr usize kContentsSize = 32;
  static constexpr usize kMaxKeyLength = 512;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Bucket State Constants

  static constexpr u64 kStateEmpty = 0;
  static constexpr u64 kStateModeMask = 3;
  static constexpr u64 kStateFragmentMode = 1;
  static constexpr u64 kStateBranchMode = 2;
  static constexpr u64 kStateLocked = 128;
  static constexpr u64 kStateSeqIncrement = 256;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The type of `contents` when we are in fragment state.  Stores (inline) the key
   * fragment.  Nodes with only one child are represented in this way to save space.
   */
  struct KeyFragment {
    u8 length;
    std::array<char, 31> bytes;

    //----- --- -- -  -  -   -

    std::string_view as_str()
    {
      return std::string_view{this->bytes.data(), this->length};
    }

    const char* begin() const
    {
      return this->bytes.data();
    }

    const char* end() const
    {
      return this->bytes.data() + this->length;
    }
  };

  static_assert(sizeof(KeyFragment) == kContentsSize);

  /** \brief The type of `contents` when we are in branched state.  One bit for every possible
   * next byte value.
   */
  struct BranchBitmap {
    std::array<std::atomic<u64>, 4> branches;

    //----- --- -- -  -  -   -

    bool get(i32 i) const
    {
      return get_bit(this->branches[(i >> 6) & 0x3].load(), (i & 0x3f));
    }

    void set(i32 i)
    {
      this->branches[(i >> 6) & 0x3].fetch_or(u64{1} << (i & 0x3f));
    }

    i32 next(i32 i) const
    {
      if (i < 0) {
        u64 w = this->branches[0].load();
        if (w != 0) {
          return __builtin_ctzll(w);
        }
        w = this->branches[1].load();
        if (w != 0) {
          return __builtin_ctzll(w) + 64;
        }
        w = this->branches[2].load();
        if (w != 0) {
          return __builtin_ctzll(w) + 128;
        }
        w = this->branches[3].load();
        if (w != 0) {
          return __builtin_ctzll(w) + 192;
        }
      } else {
        u64 w;
        i32 j = (i >> 6 & 0x3);
        i32 k = (i & 0x3f);
        switch (j) {
          case 0:
            w = this->branches[0].load();
            if (w != 0) {
              return next_bit(w, k);
            }
            // fall-through
          case 1:
            w = this->branches[1].load();
            if (w != 0) {
              return next_bit(w, k) + 64;
            }
            // fall-through
          case 2:
            w = this->branches[2].load();
            if (w != 0) {
              return next_bit(w, k) + 128;
            }
            // fall-through
          case 3:
            w = this->branches[3].load();
            if (w != 0) {
              return next_bit(w, k) + 192;
            }
            break;

          default:
            BATT_PANIC() << "i is too large!" << BATT_INSPECT(i);
            BATT_UNREACHABLE();
        }
      }
      return 256;
    }
  };

  static_assert(sizeof(BranchBitmap) == kContentsSize);

  struct Bucket {
    /** \brief Low 8 bits are reserved for flags, the remaining 56 bits are for a Seqlock-style
     * sequence number.
     */
    std::atomic<u64> state;

    /** \brief Padding; reserved for future use.  (Idea: maybe a tiny Bloom filter?)
     */
    std::aligned_storage_t<8, 8> reserved_;

    /** \brief Store the maximum prefix length, in 2-byte units, for each 4-bit LSB value of the
     * prefix hash code.
     */
    std::array<std::atomic<u8>, 16> max_prefix_length;

    /** \brief The contents of the bucket.
     */
    std::aligned_storage_t<kContentsSize, /*align=*/8> contents;

    //----- --- -- -  -  -   -

    KeyFragment& as_fragment()
    {
      return *reinterpret_cast<KeyFragment*>(std::addressof(this->contents));
    }

    BranchBitmap& as_branch()
    {
      return *reinterpret_cast<BranchBitmap*>(std::addressof(this->contents));
    }
  };

  static_assert(sizeof(Bucket) == 64);

  using BucketStorage = std::aligned_storage_t<sizeof(Bucket), alignof(Bucket)>;

  class Cursor
  {
   public:
    friend class MemTableOrderedIndex;

    struct Frame {
      Bucket* bucket;
      u64 observed_state;
    };

    //----- --- -- -  -  -   -

    Cursor() noexcept;
    ~Cursor() noexcept;

    //----- --- -- -  -  -   -

    Optional<std::string_view> peek() const;

    Optional<std::string_view> next();

    //----- --- -- -  -  -   -
   private:
    SmallVec<Frame, 32> stack_;
    std::array<char, kMaxKeyLength> key_buffer_;
    std::string_view key_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   --

  explicit MemTableOrderedIndex(usize bucket_count) noexcept;

  MemTableOrderedIndex(const MemTableOrderedIndex&) = delete;

  MemTableOrderedIndex& operator=(const MemTableOrderedIndex&) = delete;

  ~MemTableOrderedIndex() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void insert(std::string_view key);

  void scan_to(std::string_view key, Cursor& cursor);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Spin-locks the bucket and returns the observed state.
   */
  [[nodiscard]] u64 lock_bucket(Bucket* bucket);

  /** \brief Releases the bucket's spin lock and returns the observed state.
   */
  [[nodiscard]] u64 unlock_bucket(Bucket* bucket);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Bucket root_;

  std::unique_ptr<BucketStorage[]> bucket_storage_;

  Slice<Bucket> buckets_;

  batt::fixed_point::LinearProjection<u64, usize> hash_to_bucket_index_;
};

}  // namespace turtle_kv
