#include <turtle_kv/core/edit_view.hpp>
//

#include <turtle_kv/util/hex_binary_view.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const EditView& t)
{
  if (EditView::terse_printing_enabled()) {
    if (t.value.op() == ValueView::OP_DELETE) {
      out << batt::c_str_literal(t.key) << ":<deleted>";
    } else {
      out << batt::c_str_literal(t.key) << ":" << batt::c_str_literal(t.value.as_str());
    }
    out << " (op=" << (int)t.value.op() << ")";
  } else {
    out << "EditView{.key=";

    if (t.key.size() == strnlen(t.key.data(), t.key.size())) {
      out << batt::c_str_literal(t.key);
    } else {
      out << HexBinaryView{t.key};
    }
    out << ", .value=" << t.value << ",}";
  }
  return out;
}

}  // namespace turtle_kv
