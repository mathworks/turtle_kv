#pragma once

#include <batteries/interval.hpp>

#include <cstddef>
#include <ostream>

namespace turtle_kv {

using batt::BasicInterval;
using batt::CInterval;
using batt::GreatestLowerBound;
using batt::IClosed;
using batt::IClosedOpen;
using batt::Interval;
using batt::interval_traits_compatible;
using batt::IntervalTraits;
using batt::LeastUpperBound;
using batt::make_interval;

}  // namespace turtle_kv
