#pragma once

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/util/flatten.hpp>

#include <turtle_kv/import/interval.hpp>

#include <llfs/key.hpp>

#include <batteries/assert.hpp>
#include <batteries/utility.hpp>

namespace turtle_kv {

inline const CInterval<KeyView>& get_key_range(const CInterval<KeyView>& key_range)
{
  return key_range;
}

inline const Interval<KeyView>& get_key_range(const Interval<KeyView>& key_range)
{
  return key_range;
}

template <typename T>
inline CInterval<KeyView> get_key_range(const Chunk<const T*>& chunk)
{
  BATT_CHECK(!chunk.items.empty());
  return CInterval<KeyView>{
      .lower_bound = get_key(chunk.items.front()),
      .upper_bound = get_key(chunk.items.back()),
  };
}

template <HasKeyView T>
inline CInterval<KeyView> get_key_range(const T& has_key_view)
{
  return CInterval<KeyView>{
      .lower_bound = get_key(has_key_view),
      .upper_bound = get_key(has_key_view),
  };
}

struct ExtendedKeyRangeOrder : llfs::KeyRangeOrder {
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool less_than(const Interval<KeyView>& l, const Interval<KeyView>& r) const
  {
    return llfs::KeyRangeOrder::operator()(l, r);
  }

  bool less_than(const CInterval<KeyView>& l, const CInterval<KeyView>& r) const
  {
    return llfs::KeyRangeOrder::operator()(l, r);
  }

  bool less_than(const CInterval<KeyView>& l, const Interval<KeyView>& r) const
  {
    return llfs::KeyOrder{}(l.upper_bound, r.lower_bound);
  }

  bool less_than(const Interval<KeyView>& l, const CInterval<KeyView>& r) const
  {
    return !llfs::KeyOrder{}(r.lower_bound, l.upper_bound);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const
  {
    return this->less_than(get_key_range(l), get_key_range(r));
  }
};

}  // namespace turtle_kv
