#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <boost/range/algorithm/equal_range.hpp>

#include <type_traits>

namespace turtle_kv {

/** \brief A representation of a filtered range of items.
 */
template <typename OffsetT>
class PiecewiseFilter
{
 public:
  static_assert(std::is_integral<OffsetT>::value && std::is_unsigned<OffsetT>::value,
                "Offset must be an unsigned integer type!");

  /** \brief Creates and returns a PiecewiseFilter instance from a range of intervals that contain
   * the filtered item indexes.
   */
  static StatusOr<PiecewiseFilter> from_dropped(const Slice<const Interval<OffsetT>>& dropped);

  /** \brief Constructs a default instance of a PiecewiseFilter object, initialized with no item
   * range and filtered items.
   */
  PiecewiseFilter() noexcept;

  /** \brief Filters out the item range provided by the specified interval of item indexes.
   *
   * \param i The item index range to filter out, specified as a half open interval.
   */
  void drop_index_range(Interval<OffsetT> i);

  /** \brief Returns whether or not the item at index `i` has been filtered out.
   *
   * \param i The item index being queried.
   *
   * \return `true` if the item at index `i` has not been filtered out. Note that if `i` is a
   * value greater than or equal to the total number of items, this function will return `false`.
   */
  bool live_at_index(OffsetT i) const;

  /** \brief Returns the next unfiltered index greater than or equal to `i`.
   *
   * \param i The item index being queried.
   *
   * \return An index representing the next unfiltered item closest to index `i`. If `i` itself is
   * unfiltered, `i` is returned.
   */
  OffsetT live_lower_bound(OffsetT i) const;

  /** \brief Returns a range of unfiltered items within the item index boundaries defined by `i`.
   *
   * \param i A half open interval that defines the search boundaries for a slice of live items.
   *
   * \return A half open interval representing a slice of live items starting at the lower bound
   * of `i` and extending no further than the upper bound of `i`. If no such interval exists, an
   * empty interval is returned.
   */
  Interval<OffsetT> find_live_range(Interval<OffsetT> i) const;

  /** \brief Returns a view of the filtered item intervals.
   */
  Slice<const Interval<OffsetT>> dropped() const;

  /** \brief Returns the total number of filtered items.
   */
  OffsetT dropped_total() const;

  /** \brief Validate the state of the dropped intervals.
   */
  bool check_invariants() const;

  SmallFn<void(std::ostream&)> dump() const;

 private:
  /** \brief The range of filtered out item indexes.
   */
  SmallVec<Interval<OffsetT>, 64> dropped_;

  /** \brief The total number of filtered out items.
   */
  OffsetT dropped_total_;
};

template <typename OffsetT, typename ItemT, typename Traits, typename OrderFn>
inline void drop_item_range(PiecewiseFilter<OffsetT>& filter,
                            const Slice<const ItemT>& items,
                            const BasicInterval<Traits>& item_range,
                            OrderFn&& order_fn)
{
  auto iters = boost::range::equal_range(items, item_range, order_fn);

  OffsetT start_i = BATT_CHECKED_CAST(OffsetT, std::distance(items.begin(), iters.first));
  OffsetT end_i = BATT_CHECKED_CAST(OffsetT, std::distance(items.begin(), iters.second));

  filter.drop_index_range(Interval<OffsetT>{start_i, end_i});
}
}  // namespace turtle_kv

#include <turtle_kv/util/piecewise_filter.ipp>
