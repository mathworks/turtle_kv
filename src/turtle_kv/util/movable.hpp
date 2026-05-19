//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_MOVABLE_HPP

#include <batteries/type_traits.hpp>

#include <utility>

namespace turtle_kv {

template <typename T>
class Movable
{
 public:
  Movable() = default;

  template <typename... Args, typename = batt::EnableIfNoShadow<Movable, Args...>>
  explicit Movable(Args&&... args) noexcept : value_{BATT_FORWARD(args)...}
  {
  }

  Movable(const Movable&) = default;
  Movable& operator=(const Movable&) = default;

  Movable(Movable&& other) noexcept : value_{std::exchange(other.value_, T{})}
  {
  }

  Movable& operator=(Movable&& other) noexcept
  {
    if (this != &other) {
      this->value_ = std::exchange(other.value_, T{});
    }
    return *this;
  }

  operator T&()
  {
    return this->value_;
  }

  operator const T&() const
  {
    return this->value_;
  }

  T& ref() noexcept
  {
    return this->value_;
  }

  const T& cref() const noexcept
  {
    return this->value_;
  }

  template <typename U>
    requires(std::is_convertible_v<U, T>)
  Movable& operator=(const U& new_value)
  {
    this->value_ = new_value;
    return *this;
  }

 private:
  T value_;
};

}  // namespace turtle_kv
