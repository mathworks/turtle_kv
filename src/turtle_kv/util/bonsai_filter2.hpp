#pragma once

#include <turtle_kv/mem_table_entry.hpp>

#include <batteries/math.hpp>

#include <glog/logging.h>

#include <iomanip>

namespace turtle_kv {

class BonsaiFilter2
{
 public:
  using Self = BonsaiFilter2;

  // static constexpr bool kVerbose = false;
  static constexpr usize kMaxKeyLen = 64;

  struct ScanStats {
  };

  static bool& verbose()
  {
    static bool b_ = false;
    return b_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultStrHash str_hash_;
  std::vector<u32> buckets_;
  batt::fixed_point::LinearProjection<u64, usize> bucket_from_hash_val_;
  usize min_key_len_ = ~usize{0};
  usize max_key_len_ = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BonsaiFilter2(usize bucket_count) noexcept
      : str_hash_{}
      , buckets_(bucket_count, 0)
      , bucket_from_hash_val_{bucket_count}
  {
    BATT_CHECK_EQ(this->buckets_.size(), bucket_count);
    std::memset(this->buckets_.data(), 0, sizeof(u32) * bucket_count);

    if (Self::verbose()) {
      LOG(INFO) << BATT_INSPECT(bucket_count);
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  auto dump_config() const
  {
    return [this](std::ostream& out) {
      out << "BonsaiFilter2{.bucket_count=" << this->buckets_.size() << ",}";
    };
  }

  usize byte_size() const
  {
    return this->buckets_.size() * sizeof(u32);
  }

  void put(const std::string_view& key)
  {
    if (Self::verbose()) {
      LOG(INFO) << "put(" << batt::c_str_literal(key) << ")";
    }

    std::array<char, kMaxKeyLen * 2> buffer;
    usize len = 0;
    for (char ch : key) {
      buffer[len] = ch & 0xf;
      ++len;
      buffer[len] = (ch >> 4) & 0xf;
      ++len;
    }
    if (Self::verbose()) {
      LOG(INFO) << " buffer: "
                << batt::c_str_literal(std::string_view{buffer.data(), key.size() * 2});
    }

    for (usize prefix_len = 0;; ++prefix_len) {
      const u64 prefix_hash_val = this->str_hash_(std::string_view{buffer.data(), prefix_len});
      const usize bucket_i = this->bucket_from_hash_val_(prefix_hash_val);
      u32& bucket = this->buckets_[bucket_i];

      const u32 old_val = bucket;
      auto on_scope_exit = batt::finally([&] {
        if (Self::verbose()) {
          std::cerr << "bucket[" << std::setw(7) << bucket_i << "]: " << std::bitset<32>{old_val}
                    << " -> " << std::bitset<32>{bucket} << std::endl;
        }
      });

      if (prefix_len == len) {
        bucket |= (u16{1} << ((prefix_hash_val & 0xf) + 16));

        this->min_key_len_ = std::min(key.size(), this->min_key_len_);
        this->max_key_len_ = std::max(key.size(), this->max_key_len_);

        break;
      } else {
        bucket |= (u16{1} << buffer[prefix_len]);
      }
    }
  }

  template <typename EmitFn>
  void scan_all(EmitFn&& emit_fn, ScanStats&) const
  {
    std::array<char, kMaxKeyLen> key_buffer;
    std::array<char, kMaxKeyLen * 2> prefix_buffer;

    this->scan_all_impl<0>(key_buffer.data(), 0, prefix_buffer.data(), 0, BATT_FORWARD(emit_fn));
  }

  template <int kPhase, typename EmitFn>
  void scan_all_impl(char* key_buffer,
                     usize key_len,
                     char* prefix_buffer,
                     usize prefix_len,
                     EmitFn&& emit_fn) const
  {
    const std::string_view prefix{prefix_buffer, prefix_len};
    const u64 prefix_hash_val = this->str_hash_(prefix);
    const usize bucket_i = this->bucket_from_hash_val_(prefix_hash_val);
    const u32& bucket = this->buckets_[bucket_i];

    if (Self::verbose()) {
      std::cerr << ((kPhase == 0) ? "scan<phase=0>" : "    <phase=1>") << ": bucket["
                << std::setw(7) << bucket_i << "] = " << std::bitset<32>{bucket}
                << " key_len == " << std::setw(2) << key_len
                << " key=" << batt::c_str_literal(std::string_view{key_buffer, key_len})
                << std::endl;
    }

    if (kPhase == 0 && (bucket & (u16{1} << ((prefix_hash_val & 0xf) + 16))) != 0 &&
        key_len >= this->min_key_len_ && key_len <= this->max_key_len_) {
      if (Self::verbose()) {
        std::cerr << "  -- emit" << std::endl;
      }
      emit_fn(std::string_view{key_buffer, key_len});
    }

    if (key_len + 1 > kMaxKeyLen) {
      return;
    }

    u32 branch_mask = (bucket & ((u32{1} << 16) - 1));

    while (branch_mask != 0) {
      i32 bit_i = __builtin_ctzl(branch_mask);
      //----- --- -- -  -  -   -
      prefix_buffer[prefix_len] = (char)bit_i;

      if (kPhase == 0) {
        key_buffer[key_len] = (char)(bit_i & 0xf);
        this->scan_all_impl<1>(key_buffer, key_len, prefix_buffer, prefix_len + 1, emit_fn);

      } else /* kPhase == 1 */ {
        key_buffer[key_len] |= ((char)((bit_i << 4) & 0xf0));
        this->scan_all_impl<0>(key_buffer, key_len + 1, prefix_buffer, prefix_len + 1, emit_fn);
      }
      //----- --- -- -  -  -   -
      branch_mask &= ~(u32{1} << bit_i);
    }
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
inline std::ostream& operator<<(std::ostream& out, const BonsaiFilter2::ScanStats&)
{
  return out << "";
}

}  // namespace turtle_kv
