#pragma once

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/packed_key_value.hpp>

#include <turtle_kv/import/ref.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/small_fn.hpp>

#include <algorithm>
#include <ostream>
#include <variant>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using EditSlice = std::variant<  //
    Slice<const EditView>,       //
    Slice<const PackedKeyValue>  //
    >;

// The key of the first edit in the slice; the slice MUST be non-empty.
//
KeyView get_min_key(const EditSlice& edit_slice);

// The key of the last edit in the slice; the slice MUST be non-empty.
//
KeyView get_max_key(const EditSlice& edit_slice);

// Returns true iff this slice is empty.
//
bool is_empty(const EditSlice& edit_slice);

// Returns the number of items in this slice.
//
usize get_item_count(const EditSlice& edit_slice);

/** \brief Returns true iff the edit_slice is sorted by key.
 */
bool is_sorted_by_key(const EditSlice& edit_slice);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename T>
decltype(auto) get_min_key(const Ref<T>& r)
{
  return get_min_key(r.get());
}

template <typename T>
decltype(auto) get_max_key(const Ref<T>& r)
{
  return get_max_key(r.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct MinKeyHeapOrder {
  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const noexcept
  {
    return get_min_key(r) < get_min_key(l);
  }
};

struct MaxKeyHeapOrder {
  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const noexcept
  {
    return get_max_key(r) < get_max_key(l);
  }
};

}  // namespace turtle_kv

namespace batt {

SmallFn<void(std::ostream&)> dump_range(const ::turtle_kv::EditSlice& edit_slice,
                                        Pretty pretty = Pretty::False);

}  // namespace batt
