#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/key.hpp>

#include <utility>

namespace turtle_kv {

using ::llfs::get_key;
using ::llfs::KeyEqual;
using ::llfs::KeyOrder;
using ::llfs::KeyRangeOrder;
using ::llfs::KeyView;

BATT_ALWAYS_INLINE inline KeyView get_key(const char* c_str)
{
  return KeyView{c_str};
}

BATT_ALWAYS_INLINE inline KeyView get_key(const std::string& s)
{
  return KeyView{s};
}

struct KeySuffixOrder {
  // WARNING: Comparison behavior is undefined if either `left` or `right` is shorter than
  // `this->skip_n`!
  //
  usize skip_n;

  template <typename Left, typename Right>
  bool operator()(const Left& left, const Right& right) const noexcept
  {
    const auto& left_key = get_key(left);
    const auto& right_key = get_key(right);
    const usize common_size = std::min(left_key.size(), right_key.size()) - skip_n;

    int common_cmp = std::memcmp(left_key.data() + skip_n, right_key.data() + skip_n, common_size);
    return common_cmp < 0 || (common_cmp == 0 && (left_key.size() < right_key.size()));
  }
};

inline const KeyView& global_min_key()
{
  static const KeyView min_key_{"", 0};
  return min_key_;
}

inline bool is_global_min_key(const KeyView& key)
{
  return key.empty();
}

inline const KeyView&& global_max_key()
{
  static const KeyView max_key_ = batt::StringUpperBound();
  return std::forward<const KeyView>(max_key_);
}

inline bool is_global_max_key(const KeyView& key)
{
  return key.data() == global_max_key().data();
}

/** \brief Returns 0 if `key` is the global_max_key(); otherwise `key.size()`.
 *
 * We don't pack globally-maximal key values as they would be too large; instead we pack them as
 * empty strings and infer that the packed key is the max key from context.
 */
inline usize packed_key_data_size(const KeyView& key)
{
  if (is_global_max_key(key)) {
    return 0;
  }
  return key.size();
}

}  // namespace turtle_kv
