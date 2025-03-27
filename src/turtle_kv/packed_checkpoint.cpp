#include <turtle_kv/packed_checkpoint.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedCheckpoint& t)
{
  return out << "PackedCheckpoint{"                             //
             << ", .batch_upper_bound=" << t.batch_upper_bound  //
             << ", .tree_root=" << t.new_tree_root              //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedCheckpoint& checkpoint)
{
  return llfs::seq::single_item(checkpoint.new_tree_root.as_page_id())  //
         | llfs::seq::boxed();
}

}  // namespace turtle_kv
