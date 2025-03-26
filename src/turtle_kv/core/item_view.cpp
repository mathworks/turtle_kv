#include <turtle_kv/core/item_view.hpp>
//

#include <turtle_kv/core/edit_view.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const ItemView& t)
{
  return out << EditView::from_item_view(t);
}

}  // namespace turtle_kv
