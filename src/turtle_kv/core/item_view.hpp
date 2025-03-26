#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/optional.hpp>

#include <ostream>

namespace turtle_kv {

struct ItemView;

std::ostream& operator<<(std::ostream& out, const ItemView& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct ItemView {
  explicit ItemView(KeyView key, ValueView value) noexcept : key(key), value(value)
  {
  }

  friend std::ostream& operator<<(std::ostream& out, const ItemView& t);

  KeyView key;
  ValueView value;
};

BATT_ALWAYS_INLINE inline const KeyView& get_key(const ItemView& item)
{
  return item.key;
}

inline Optional<ItemView> to_item_view(const ItemView& item)
{
  return {item};
}

inline const ValueView& get_value(const ItemView& item)
{
  return item.value;
}

struct ToItemView {
  template <typename T>
  auto operator()(T&& v) const
  {
    return to_item_view(std::forward<T>(v));
  }
};

inline bool operator==(const ItemView& l, const ItemView& r)
{
  return l.key == r.key && l.value == r.value;
}

template <typename K, typename V>
inline bool operator==(const ItemView& l, const std::pair<K, V>& r)
{
  return l.key == get_key(r.first) && l.value.as_str() == get_value_as_str(r.second);
}

template <typename K, typename V>
inline bool operator==(const std::pair<K, V>& l, const ItemView& r)
{
  return get_key(l.first) == r.key && get_value_as_str(l.second) == r.value;
}

}  // namespace turtle_kv
