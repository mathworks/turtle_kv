//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PACKED_PIECEWISE_FILTER_VIEW_HPP

#include "piecewise_filter_storage_model.concept.hpp"

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/assert.hpp>
#include <batteries/checked_cast.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <iterator>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Read-only model of PiecewiseFilterStorageModel for packed filters.
 *
 * Packed piecewise filters are represented as an array of integers, which are the boundaries
 * between live and dropped intervals, plus an additional boolean/bit denoting whether the interval
 * from the global minimum to the first stored boundary is live or dropped (`start_is_live`).
 *
 * The global minimum and maximum bounds are never stored in the packed representation.  Instead,
 * the minimum is implied via the `start_is_live` bit, and the maximum by whether the number of
 * stored bounds (plus the implicit first bound, if start_is_live == true) is even or odd.  If it is
 * odd, then it is implied that there is a final bound equal to the global maximum.
 *
 * Examples:
 *
 * Live Intervals: {[0, 10), [20, 30), [40, 50)}
 * Packed: start_is_live=1, {10, 20, 30, 40, 50}
 *
 * Live Intervals: {[0, 10), [20, 30), [40, +inf)}
 * Packed: start_is_live=1, {10, 20, 30, 40}
 *
 * Live Intervals: {[10, 20), [30, 40), [50, 60)}
 * Packed: start_is_live=0, {10, 20, 30, 40, 50, 60}
 *
 * Live Intervals: {[10, 20), [30, 40), [50, +inf)}
 * Packed: start_is_live=0, {10, 20, 30, 40, 50}
 */
class PackedPiecewiseFilterStorage
{
 public:
  //----- --- -- -  -  -   -

  // Forward-declaration; the type returned by this->begin(), this->end()
  //
  class const_iterator;

  /** \brief The boundary integer type.  Must be unsigned.
   */
  using OffsetT = const little_u32;

  /** \brief The live range type; what `iterator` iterates over.
   */
  using value_type = Interval<u32>;

  /** \brief Non-const iterator aliases const_iterator, since this storage model is read-only.
   */
  using iterator = const_iterator;

  // Forward-declaration; defined below.
  //
  friend const Slice<const little_u32>& as_const_slice(const PackedPiecewiseFilterStorage& view);

  //----- --- -- -  -  -   -

  /** \brief Constructs an PackedPiecewiseFilterStorage representing the live interval [0, +inf).
   */
  PackedPiecewiseFilterStorage() = default;

  /** \brief Destructs the PackedPiecewiseFilterStorage.
   */
  ~PackedPiecewiseFilterStorage() = default;

  /** \brief PackedPiecewiseFilterStorage is copy constructible.
   */
  PackedPiecewiseFilterStorage(const PackedPiecewiseFilterStorage&) = default;

  /** \brief PackedPiecewiseFilterStorage is copy assignable.
   */
  PackedPiecewiseFilterStorage& operator=(const PackedPiecewiseFilterStorage&) = default;

  /** \brief Constructs PackedPiecewiseFilterStorage from the packed data in the arguments.
   *
   * See the class-level description for details on what `values` and `start_is_live` represent.
   */
  explicit PackedPiecewiseFilterStorage(const Slice<const little_u32>& values,
                                     bool start_is_live) noexcept
      : values_{values}
      , implicit_first_{start_is_live ? 1 : 0}
      , size_{(this->implicit_first_ + BATT_CHECKED_CAST(i32, this->values_.size()) + 1) / 2}
  {
  }

  //----- --- -- -  -  -   -

  /** \brief Returns an iterator to the first live interval in this filter.
   */
  const_iterator begin() const noexcept;

  /** \brief Returns an iterator to one past the last live interval in this filter.
   */
  const_iterator end() const noexcept;

  /** \brief Returns the number of live intervals in the filter; same as
   * `std::distance(this->begin(), this->end())`.
   */
  usize size() const noexcept;

  /** \brief Returns true iff `this->size() == 0`.
   */
  bool empty() const noexcept;

  /** \brief Returns the `i`-th live interval in the filter.  Behavior is undefined if `i` is not
   * less than `this->size()`.
   */
  Interval<u32> operator[](isize i) const noexcept;

  //----- --- -- -  -  -   -
 private:
  /** \brief Points at the stored boundaries, as described in the class-level doc.
   */
  Slice<const little_u32> values_;

  /** \brief Set to 1 if `start_is_live`, else 0.
   */
  i32 implicit_first_ = 1;

  /** \brief The number of live intervals in the filter.
   */
  i32 size_ = 1;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns a const reference to the stored values referenced by `view`.
 */
inline const Slice<const little_u32>& as_const_slice(const PackedPiecewiseFilterStorage& view)
{
  return view.values_;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Read-only, random access iterator over the live intervals of a packed piecewise filter.
 */
class PackedPiecewiseFilterStorage::const_iterator
    : public boost::iterator_facade<                  //
          PackedPiecewiseFilterStorage::const_iterator,  // <- Derived
          Interval<u32>,                              // <- Value
          std::random_access_iterator_tag,            // <- CategoryOrTraversal
          Interval<u32>,                              // <- Reference
          isize                                       // <- Difference
          >
{
 public:
  using Self = const_iterator;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = Interval<u32>;
  using reference = Interval<u32>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs an invalid iterator.
   */
  const_iterator() noexcept : view_{nullptr}, pos_{0}
  {
  }

  /** \brief Constructs an iterator to the `pos`-th live interval of `view`.
   *
   * `view` must remain in-scope while this object exists.
   */
  const_iterator(const PackedPiecewiseFilterStorage* view, isize pos) noexcept : view_{view}, pos_{pos}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the live interval at the current position.
   */
  reference dereference() const
  {
    return (*this->view_)[this->pos_];
  }

  /** \brief Returns true iff this iterator points to the same live interval of the same filter as
   * `other`.
   */
  bool equal(const Self& other) const
  {
    return this->view_ == other.view_ && this->pos_ == other.pos_;
  }

  /** \brief Moves this iterator forward by one.
   */
  void increment()
  {
    ++this->pos_;
  }

  /** \brief Moves this iterator backward by one.
   */
  void decrement()
  {
    --this->pos_;
  }

  /** \brief Moves this iterator by `delta`.
   */
  void advance(isize delta)
  {
    this->pos_ += delta;
  }

  /** \brief Returns the number of steps required to advance this iterator so it is equivalent to
   * `other`.  Will panic if this and other do not point at the same filter view.
   */
  isize distance_to(const Self& other) const
  {
    BATT_CHECK_EQ(this->view_, other.view_);
    return other.pos_ - this->pos_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Pointer to the filter view over which we are iterating.
   */
  const PackedPiecewiseFilterStorage* view_;

  /** \brief The (logical) position of this iterator within `view_`.
   */
  isize pos_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto PackedPiecewiseFilterStorage::begin() const noexcept -> const_iterator
{
  return const_iterator{this, 0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto PackedPiecewiseFilterStorage::end() const noexcept -> const_iterator
{
  return const_iterator{this, static_cast<isize>(this->size())};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize PackedPiecewiseFilterStorage::size() const noexcept
{
  return this->size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool PackedPiecewiseFilterStorage::empty() const noexcept
{
  return this->size_ == 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline Interval<u32> PackedPiecewiseFilterStorage::operator[](isize i) const noexcept
{
  // Cached for brevity below.
  //
  const isize n = this->values_.size();

  // The index within this->values_ of the i-th live interval's lower bound.
  //  May be negative if the first boundary is implicit (0)
  //
  const isize j0 = i * 2 - this->implicit_first_;

  // The index within this->values_ of the i-th live interval's upper bound.
  //  May be past the end of this->values_ if the last boundary is implicit (+inf)
  //
  const isize j1 = j0 + 1;

  const u32 lower_bound = (j0 < 0) ? std::numeric_limits<u32>::min() : this->values_[j0].value();
  const u32 upper_bound = (j1 < n) ? this->values_[j1].value() : std::numeric_limits<u32>::max();

  return Interval<u32>{lower_bound, upper_bound};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

static_assert(PiecewiseFilterStorageModel<PackedPiecewiseFilterStorage, u32>);

}  // namespace turtle_kv
