#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/strong_typedef.hpp>

#include <boost/iterator/iterator_facade.hpp>
#include <boost/range/iterator_range.hpp>

#include <vector>

namespace turtle_kv {

BATT_STRONG_TYPEDEF(usize, MinPartSize);
BATT_STRONG_TYPEDEF(usize, MaxPartSize);
BATT_STRONG_TYPEDEF(usize, MaxItemSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Iterate through the result of `split_parts` as a sequence of intervals.
//
class SplitPartsIterator
    : public boost::iterator_facade<        //
          SplitPartsIterator,               // <- Derived
          Interval<usize>,                  // <- Value
          std::random_access_iterator_tag,  // <- CategoryOrTraversal
          Interval<usize>,                  // <- Reference
          isize                             // <- Difference
          >
{
 public:
  using Self = SplitPartsIterator;

  SplitPartsIterator() = default;
  SplitPartsIterator(const SplitPartsIterator&) = default;
  SplitPartsIterator& operator=(const SplitPartsIterator&) = default;

  explicit SplitPartsIterator(const usize* part_iter) noexcept : part_iter_{part_iter}
  {
  }

  Interval<usize> dereference() const
  {
    return Interval<usize>{this->part_iter_[0], this->part_iter_[1]};
  }

  bool equal(const Self& other) const
  {
    return this->part_iter_ == other.part_iter_;
  }

  void increment()
  {
    this->advance(1);
  }

  void decrement()
  {
    this->advance(-1);
  }

  void advance(isize delta)
  {
    this->part_iter_ += delta;
  }

  isize distance_to(const Self& other) const
  {
    return std::distance(this->part_iter_, other.part_iter_);
  }

 private:
  const usize* part_iter_ = nullptr;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// The result of `split_parts`.
//
struct SplitParts {
  SmallVec<usize, 4> offsets{0};

  using value_type = Interval<usize>;
  using iterator = SplitPartsIterator;
  using const_iterator = iterator;

  iterator begin() const
  {
    return iterator{&this->offsets.front()};
  }

  iterator end() const
  {
    return iterator{&this->offsets.back()};
  }

  value_type front() const
  {
    return *this->begin();
  }

  value_type back() const
  {
    return *std::prev(this->end());
  }

  usize size() const
  {
    return this->offsets.size() - 1;
  }

  bool empty() const
  {
    return this->size() == 0u;
  }

  value_type operator[](isize pos) const
  {
    return *(this->begin() + pos);
  }
};

// Returns the smallest possible segmentation of the pivots of a tree node which groups the passed
// sub_totals so that each part's size is:
//    - <= `max_part_size`
//    - >= `min_part_size`

// The returned vector is comprised of indices into the sub_totals array; it will be sized one
// greater than the number of parts, with the first element always being 0.
//
// `sub_totals` must be cummulative; e.g.:
//     - sub_totals[0] == (size of pivot 0)
//     - sub_totals[1] == (size of pivots 0 and 1)
//     - sub_totals[2] == (size of pivots 0, 1, and 2)
//     - etc.
//
// Let `result` be the vector returned by this function.  Then:
//     - the number of parts is `result.size() - 1`
//     - `result.front()` is always 0
//     - `result.back()` is always `sub_totals.size()`
//     - the first part is comprised of pivots [result[0], result[1])
//     - the second part is comprised of pivots [result[1], result[2])
//     - the third part is comprised of pivots [result[2], result[3])
//     - etc.
//
// The small vector size hint is 4 here because for most cases, we expect that the number of
// returned parts will be 1, 2, or 3.
//
template <typename SubTotals>
SplitParts split_parts(const SubTotals& sub_totals,
                       MinPartSize min_part_size,
                       MaxPartSize max_part_size,
                       MaxItemSize max_item_size);

}  // namespace turtle_kv

#include <turtle_kv/core/algo/split_parts.ipp>
