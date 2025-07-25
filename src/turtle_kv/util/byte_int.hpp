#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <batteries/utility.hpp>

#include <ostream>
#include <type_traits>

namespace turtle_kv {

class ByteInt
{
 public:
  template <typename T>
  BATT_ALWAYS_INLINE constexpr static ByteInt from_u8(T value) noexcept
  {
    static_assert(std::is_same_v<T, u8>);
    return ByteInt{static_cast<i32>(static_cast<u32>(value))};
  }

  template <typename T>
  BATT_ALWAYS_INLINE constexpr static ByteInt from_char(T value) noexcept
  {
    static_assert(std::is_same_v<T, char>);
    return ByteInt::from_u8(static_cast<u8>(value));
  }

  template <typename T>
  BATT_ALWAYS_INLINE constexpr static ByteInt from_i32(T value) noexcept
  {
    static_assert(std::is_same_v<T, i32>);
    return ByteInt{value};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ByteInt() = default;
  ByteInt(const ByteInt&) = default;
  ByteInt& operator=(const ByteInt&) = default;

  BATT_ALWAYS_INLINE ByteInt& operator++() noexcept
  {
    ++this->value_;
    return *this;
  }

  BATT_ALWAYS_INLINE constexpr i32 to_i32() const noexcept
  {
    return this->value_;
  }

  BATT_ALWAYS_INLINE constexpr char to_char() const noexcept
  {
    return static_cast<char>(this->value_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  friend BATT_ALWAYS_INLINE constexpr ByteInt operator+(ByteInt l, ByteInt r) noexcept
  {
    return ByteInt{l.value_ + r.value_};
  }

  friend BATT_ALWAYS_INLINE constexpr bool operator==(ByteInt l, ByteInt r) noexcept
  {
    return l.value_ == r.value_;
  }

  friend BATT_ALWAYS_INLINE constexpr auto operator<=>(ByteInt l, ByteInt r) noexcept
  {
    return l.value_ <=> r.value_;
  }

  friend BATT_ALWAYS_INLINE std::ostream& operator<<(std::ostream& out, ByteInt bi) noexcept
  {
    return out << bi.value_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  BATT_ALWAYS_INLINE constexpr explicit ByteInt(i32 value) noexcept : value_{value}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i32 value_;
};

}  // namespace turtle_kv
