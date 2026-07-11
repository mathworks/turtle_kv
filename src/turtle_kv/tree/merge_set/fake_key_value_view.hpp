#pragma once
#define TURTLE_KV_TREE_MERGE_SET_FAKE_KEY_VALUE_VIEW_HPP

#include <string_view>

namespace turtle_kv {
namespace merge_set {

struct FakeKeyValueView {
  std::string_view key_;
  std::string_view value_;
};

inline const std::string_view& get_key(const FakeKeyValueView& view) noexcept
{
  return view.key_;
}

}  // namespace merge_set
}  // namespace turtle_kv
