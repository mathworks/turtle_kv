#pragma once
#define TURTLE_KV_TREE_ACTIVE_PIVOTS_SET_HPP

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/stream_util.hpp>
#include <batteries/utility.hpp>

#include <array>
#include <bitset>
#include <ostream>
#include <type_traits>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept ActivePivotsSet = requires(T pivots) {
  // count
  { std::declval<const T&>().count() } -> std::convertible_to<usize>;

  // set
  { pivots.set(std::declval<i32>(), std::declval<bool>()) } -> std::same_as<void>;

  // get
  { std::declval<const T&>().get(std::declval<i32>()) } -> std::same_as<bool>;

  // first
  { std::declval<const T&>().first() } -> std::convertible_to<i32>;

  // last
  { std::declval<const T&>().last() } -> std::convertible_to<i32>;

  // printable
  {
    std::declval<std::ostream&>() << std::declval<const T&>().printable()
  } -> std::convertible_to<std::ostream&>;
};

template <typename T>
concept HasActivePivotsSet = requires(const T& obj) {
  { obj.get_active_pivots() } -> ActivePivotsSet;
};

class PackedActivePivotsSet64;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ActivePivotsSet128
{
  friend class PackedActivePivotsSet64;

 public:
  BATT_ALWAYS_INLINE usize count() const
  {
    return bit_count(this->bit_set_);
  }

  BATT_ALWAYS_INLINE void set(i32 i, bool v)
  {
    this->bit_set_ = set_bit(this->bit_set_, i, v);
  }

  BATT_ALWAYS_INLINE bool get(i32 i) const
  {
    return get_bit(this->bit_set_, i);
  }

  BATT_ALWAYS_INLINE i32 first() const
  {
    return first_bit(this->bit_set_);
  }

  BATT_ALWAYS_INLINE i32 last() const
  {
    return last_bit(this->bit_set_);
  }

  auto printable() const
  {
    return [this](std::ostream& out) {
      out << std::bitset<64>{this->bit_set_[1]} << "," << std::bitset<64>{this->bit_set_[1]};
    };
  }

 private:
  std::array<u64, 2> bit_set_ = {0, 0};
};

static_assert(ActivePivotsSet<ActivePivotsSet128>);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackedActivePivotsSet64
{
 public:
  /*implicit*/ PackedActivePivotsSet64(const ActivePivotsSet128& src) noexcept
      : bit_set_{src.bit_set_[0]}
  {
    BATT_CHECK_EQ(src.bit_set_[1], 0);
  }

  ActivePivotsSet128 unpack() const
  {
    ActivePivotsSet128 unpacked;
    unpacked.bit_set_[0] = this->bit_set_;
    return unpacked;
  }

  BATT_ALWAYS_INLINE usize count() const
  {
    return bit_count(this->bit_set_);
  }

  BATT_ALWAYS_INLINE void set(i32 i, bool v)
  {
    this->bit_set_ = set_bit(this->bit_set_, i, v);
  }

  BATT_ALWAYS_INLINE bool get(i32 i) const
  {
    return get_bit(this->bit_set_, i);
  }

  BATT_ALWAYS_INLINE i32 first() const
  {
    return first_bit(this->bit_set_);
  }

  BATT_ALWAYS_INLINE i32 last() const
  {
    return last_bit(this->bit_set_);
  }

  auto printable() const
  {
    return std::bitset<64>{this->bit_set_.value()};
  }

 private:
  little_u64 bit_set_;
};

static_assert(ActivePivotsSet<PackedActivePivotsSet64>);

}  // namespace turtle_kv
