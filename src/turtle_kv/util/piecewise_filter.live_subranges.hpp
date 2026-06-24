//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PIECEWISE_FILTER_LIVE_SUBRANGES_HPP

#include "piecewise_filter.hpp"

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A (batt) Seq over the sub-ranges of a PiecewiseFilter which match some interval (the
 * `query_range`).
 */
template <typename OffsetT, PiecewiseFilterStorageModel<OffsetT> ModelT>
class BasicPiecewiseFilter<OffsetT, ModelT>::LiveSubranges
{
 public:
  using Iterator = PiecewiseFilter<OffsetT>::ConstIterator;
  using Item = Interval<OffsetT>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a LiveSubranges seq containing the passed range of live intervals (`match`),
   * with the first and last element clamped to the `query_range`.
   *
   * `match` *must* not extend more than one live interval past `query_range` at the front or back.
   */
  explicit LiveSubranges(Interval<OffsetT> query_range,
                         std::ranges::subrange<Iterator> match) noexcept
      : clamp_lower_{query_range.lower_bound}
      , clamp_upper_{query_range.upper_bound}
      , match_{match}
  {
  }

  /** \brief Returns the current live subrange, or None if the seq has been fully consumed.
   */
  Optional<Item> peek()
  {
    if (this->match_.empty()) {
      return None;
    }
    Interval<OffsetT> item = this->match_.front();
    if (this->clamp_lower_) {
      item.lower_bound = std::max(item.lower_bound, *this->clamp_lower_);
    }
    if (this->match_.size() == 1 && this->clamp_upper_) {
      item.upper_bound = std::min(item.upper_bound, *this->clamp_upper_);
    }
    return item;
  }

  /** \brief Returns the current live subrange, or None if the seq has been fully consumed,
   * consuming the returned item.
   */
  Optional<Item> next()
  {
    Optional<Item> item = this->peek();
    if (item) {
      this->clamp_lower_ = None;
      this->match_.advance(1);
    }
    return item;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief The lower bound to which to clamp the first interval of `this->match_`.
   */
  Optional<OffsetT> clamp_lower_;

  /** \brief The upper bound to which to clamp the last interval of `this->match_`.
   */
  Optional<OffsetT> clamp_upper_;

  /** \brief The current subrange of the live intervals in the filter.
   */
  std::ranges::subrange<Iterator> match_;
};

}  // namespace turtle_kv
