#pragma once

#include <ostream>
#include <string_view>

namespace turtle_kv {

struct HexBinaryView {
  std::string_view str;
};

std::ostream& operator<<(std::ostream& out, const HexBinaryView& t);

}  // namespace turtle_kv
