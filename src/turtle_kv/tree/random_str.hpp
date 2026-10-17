//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/import/int_types.hpp>

#include <batteries/stable_string_store.hpp>

#include <algorithm>
#include <cstring>
#include <random>
#include <string_view>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SizeDistribution>
std::string_view random_str(std::default_random_engine& rng,
                            SizeDistribution&& pick_size,
                            usize min_size,
                            usize max_size,
                            batt::StableStringStore& strings,
                            std::string_view prefix = "") noexcept
{
  std::uniform_int_distribution<i8> pick_char{'a', 'z'};

  const usize n = min_size + std::min(pick_size(rng), max_size - min_size);
  batt::MutableBuffer buf = strings.allocate(prefix.size() + n);
  char* chars = static_cast<char*>(buf.data());

  if (!prefix.empty()) {
    std::memcpy(chars, prefix.data(), prefix.size());
    chars += prefix.size();
  }

  for (usize i = 0; i < n; ++i, ++chars) {
    *chars = pick_char(rng);
  }

  return std::string_view{static_cast<const char*>(buf.data()), buf.size()};
}

}  // namespace turtle_kv
