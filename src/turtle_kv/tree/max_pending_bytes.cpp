#include <turtle_kv/tree/max_pending_bytes.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const MaxPendingBytes& t)
{
  return out << "MaxPendingBytes{.pivot_index=" << t.pivot_index << ", .byte_count=" << t.byte_count
             << ",}";
}

}  // namespace turtle_kv
