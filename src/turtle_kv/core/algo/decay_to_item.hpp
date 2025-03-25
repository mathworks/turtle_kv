#pragma once

#include <turtle_kv/core/edit_view.hpp>

namespace turtle_kv {

template <bool kValue>
struct DecayToItem;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <>
struct DecayToItem<false> {
  static constexpr bool value = false;

  constexpr operator bool() const
  {
    return value;
  }

  template <typename T>
  __attribute__((always_inline)) static bool keep_item(const T&) noexcept
  {
    return true;
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <>
struct DecayToItem<true> {
  static constexpr bool value = true;

  constexpr operator bool() const
  {
    return value;
  }

  template <typename T>
  __attribute__((always_inline)) static bool keep_item(const T& obj) noexcept
  {
    return decays_to_item(obj);
  }
};

}  // namespace turtle_kv
