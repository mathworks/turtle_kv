#include <turtle_kv/util/hex_binary_view.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const HexBinaryView& t)
{
  static const char hex_digits[16] = {
      '0',
      '1',
      '2',
      '3',
      '4',
      '5',
      '6',
      '7',
      '8',
      '9',
      'a',
      'b',
      'c',
      'd',
      'e',
      'f',
  };

  out << "<";
  for (std::size_t i = 0; i < t.str.size(); ++i) {
    char ch = t.str.data()[i];
    out << hex_digits[(ch >> 4) & 0xf] << hex_digits[ch & 0xf];
    if ((i % 2) == 1 && i + 1 < t.str.size()) {
      out << " ";
    }
  }
  return out << ">";
}

}  // namespace turtle_kv
