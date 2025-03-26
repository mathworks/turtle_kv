#pragma once

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/interval.hpp>

#include <batteries/stream_util.hpp>

namespace turtle_kv {

// Half-open key interval; includes lower_bound but stops right *before* upper_bound.
//
using KeyViewRange = Interval<KeyView>;

// Closed key interval; includes lower_bound, upper_bound, and everything in between.
//
using CKeyViewRange = CInterval<KeyView>;

inline KeyViewRange key_range(const KeyView& lower_bound, const KeyView& upper_bound) noexcept
{
  return {
      .lower_bound = lower_bound,
      .upper_bound = upper_bound,
  };
}

inline CKeyViewRange ckey_range(const KeyView& lower_bound, const KeyView& upper_bound) noexcept
{
  return {
      .lower_bound = lower_bound,
      .upper_bound = upper_bound,
  };
}

inline KeyViewRange full_key_range() noexcept
{
  return {
      .lower_bound = global_min_key(),
      .upper_bound = global_max_key(),
  };
}

inline CKeyViewRange full_key_crange() noexcept
{
  return {
      .lower_bound = global_min_key(),
      .upper_bound = global_max_key(),
  };
}

inline auto dump_key_range(const Interval<KeyView>& key_range) noexcept
{
  return [&key_range](std::ostream& out) {
    out << "["                                         //
        << batt::c_str_literal(key_range.lower_bound)  //
        << ", "                                        //
        << batt::c_str_literal(key_range.upper_bound)  //
        << ")";
  };
}

inline auto dump_key_range(const CInterval<KeyView>& key_range) noexcept
{
  return [&key_range](std::ostream& out) {
    out << "["                                         //
        << batt::c_str_literal(key_range.lower_bound)  //
        << ", "                                        //
        << batt::c_str_literal(key_range.upper_bound)  //
        << "]";
  };
}

}  // namespace turtle_kv
