#pragma once
#define TURTLE_KV_TREE_MERGE_SET_FAKE_KEY_VALUE_HPP

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <string>
#include <string_view>

namespace turtle_kv {
namespace merge_set {

struct FakeKeyValue {
  std::string key_;
  std::string value_;
};

inline KeyView get_key(const FakeKeyValue& view) noexcept
{
  return view.key_;
}

inline usize packed_sizeof(const FakeKeyValue& kv) noexcept
{
  return kv.key_.size() + kv.value_.size();
}

inline std::string get_min_upper_bound(const std::string_view& view) noexcept
{
  std::string s{view};
  if (s.back() == (char)255) {
    s += '\0';
  } else {
    ++s.back();
  }
  return s;
}

}  // namespace merge_set
}  // namespace turtle_kv
