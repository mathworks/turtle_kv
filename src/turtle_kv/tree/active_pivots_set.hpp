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
concept ConstActivePivotsSet = requires(const T& obj, i32 index, bool value, std::ostream& out) {
  // count
  { obj.count() } -> std::convertible_to<usize>;

  // get
  { obj.get(index) } -> std::convertible_to<bool>;

  // first
  { obj.first() } -> std::convertible_to<i32>;

  // last
  { obj.last() } -> std::convertible_to<i32>;

  // next
  { obj.next(index) } -> std::convertible_to<i32>;

  // printable
  { out << obj.printable() } -> std::convertible_to<std::ostream&>;

  // is_empty
  { obj.is_empty() } -> std::convertible_to<bool>;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept MutableActivePivotsSet = requires(T& obj, i32 index, bool value) {
  // set
  { obj.set(index, value) };
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept ActivePivotsSet = ConstActivePivotsSet<T> && MutableActivePivotsSet<T>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept HasConstActivePivotsSet = requires(const T& obj) {
  { obj.get_active_pivots() } -> ConstActivePivotsSet;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Base class for active pivot bit set representation. Implements basic bit operations.
 */
template <typename Bitset>
class ActivePivotsSetBase
{
 public:
  BATT_ALWAYS_INLINE ActivePivotsSetBase() noexcept : bit_set_{}
  {
  }

  BATT_ALWAYS_INLINE explicit ActivePivotsSetBase(const Bitset& bit_set) : bit_set_{bit_set}
  {
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

  BATT_ALWAYS_INLINE i32 next(i32 i) const
  {
    return next_bit(this->bit_set_, i);
  }

  BATT_ALWAYS_INLINE void insert(i32 i, bool v)
  {
    this->bit_set_ = insert_bit(this->bit_set_, i, v);
  }

  BATT_ALWAYS_INLINE bool is_empty() const
  {
    return this->bit_set_ == Bitset{};
  }

 protected:
  Bitset bit_set_;
};

class PackedActivePivotsSet64;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Representation of a 128-bit bit set of active pivots.
 */
class ActivePivotsSet128 : public ActivePivotsSetBase<std::array<u64, 2>>
{
  friend class PackedActivePivotsSet64;

 public:
  using Super = ActivePivotsSetBase<std::array<u64, 2>>;
  using Self = ActivePivotsSet128;

  BATT_ALWAYS_INLINE ActivePivotsSet128() noexcept : Super{}
  {
    // Make sure the bit set starts out empty.
    //
    this->clear();
  }

  /** \brief Removes all pivots from the set.
   */
  BATT_ALWAYS_INLINE void clear()
  {
    this->bit_set_.fill(0);
  }

  /** \brief Returns a printable representation of this object.
   */
  BATT_ALWAYS_INLINE auto printable() const
  {
    return [this](std::ostream& out) {
      out << std::bitset<64>{this->bit_set_[1]} << "," << std::bitset<64>{this->bit_set_[1]};
    };
  }

  /** \brief Removes the specified number (`count`) pivots from the bit set.
   */
  BATT_ALWAYS_INLINE void pop_front_pivots(i32 count)
  {
    if (count < 1) {
      return;
    }

    if (count < 64) {
      this->bit_set_[0] = (this->bit_set_[0] >> count) | (this->bit_set_[1] << (64 - count));
      this->bit_set_[1] >>= count;
    } else {
      this->bit_set_[0] = this->bit_set_[1] >> (count - 64);
      this->bit_set_[1] = 0;
    }
  }
};

static_assert(ActivePivotsSet<ActivePivotsSet128>);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A packed representation of a 64-bit bit set of active pivots.
 */
class PackedActivePivotsSet64 : public ActivePivotsSetBase<little_u64>
{
 public:
  BATT_ALWAYS_INLINE /*implicit*/ PackedActivePivotsSet64(const ActivePivotsSet128& src) noexcept
      : ActivePivotsSetBase<little_u64>{little_u64{src.bit_set_[0]}}
  {
    BATT_CHECK_EQ(src.bit_set_[1], 0);
  }

  BATT_ALWAYS_INLINE ActivePivotsSet128 unpack() const
  {
    ActivePivotsSet128 unpacked;
    unpacked.bit_set_[0] = this->bit_set_;
    unpacked.bit_set_[1] = 0;
    return unpacked;
  }

  BATT_ALWAYS_INLINE auto printable() const
  {
    return std::bitset<64>{this->bit_set_.value()};
  }
};

static_assert(ActivePivotsSet<PackedActivePivotsSet64>);
static_assert(sizeof(PackedActivePivotsSet64) == 8);

}  // namespace turtle_kv
