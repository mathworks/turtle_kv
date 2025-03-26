#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <ostream>

namespace turtle_kv {

struct MaxPendingBytes {
  usize pivot_index = 0;
  usize byte_count = 0;
};

std::ostream& operator<<(std::ostream& out, const MaxPendingBytes& t);

}  // namespace turtle_kv
