//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_PIECEWISE_FILTER_STORAGE_MODEL_CONCEPT_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

#include <batteries/stream_util.hpp>

#include <concepts>
#include <iterator>
#include <ostream>
#include <utility>

namespace turtle_kv {

template <typename T, typename OffsetT>
concept PiecewiseFilterStorageModel = requires(const T& model,
                                               T& src,
                                               T& dst,
                                               Interval<OffsetT> interval,
                                               usize i,
                                               std::ostream& out) {
  typename T::value_type;
  typename T::iterator;
  typename T::const_iterator;

  { model.begin() };
  { model.end() };
  { std::begin(model) } -> std::same_as<decltype(model.begin())>;
  { std::end(model) } -> std::same_as<decltype(model.end())>;
  { as_const_slice(model) };
  { model.empty() } -> std::convertible_to<bool>;
  { model.size() } -> std::convertible_to<usize>;
  { *model.begin() } -> std::convertible_to<const Interval<OffsetT>&>;
  { model[i] } -> std::convertible_to<const Interval<OffsetT>&>;
  { dst = std::move(src) };
  { out << batt::dump_range(model) } -> std::same_as<std::ostream&>;
};

template <typename T, typename OffsetT>
concept PiecewiseFilterMutableStorageModel =
    PiecewiseFilterStorageModel<T, OffsetT> &&
    requires(T model, T other, Interval<OffsetT> interval, usize i, std::ostream& out) {
      { model.clear() };
      { model.erase(model.end()) };
      { model.insert(model.end(), interval) };
      { model.insert(model.end(), model.begin(), model.end()) };
      { model[i] = interval };
    };

}  // namespace turtle_kv
