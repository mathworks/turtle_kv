#pragma once
#define TURTLE_KV_TREE_MERGE_SET_FAKE_KEY_VALUE_VIEW_HPP

#include <turtle_kv/core/key_view.hpp>

#include <string_view>

namespace turtle_kv {
namespace merge_set {

struct FakeKeyValueView {
  KeyView key_;
  std::string_view value_;
};

inline const KeyView& get_key(const FakeKeyValueView& view) noexcept
{
  return view.key_;
}

}  // namespace merge_set
}  // namespace turtle_kv
