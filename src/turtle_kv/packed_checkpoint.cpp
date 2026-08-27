#include <turtle_kv/packed_checkpoint.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedCheckpoint& t)
{
  return out << "PackedCheckpoint{"                                         //
             << ", .edit_offset_upper_bound=" << t.edit_offset_upper_bound  //
             << ", .tree_root=" << t.new_tree_root                          //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedCheckpoint& checkpoint)
{
  return llfs::seq::single_item(checkpoint.new_tree_root.as_page_id())  //
         | llfs::seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ActiveCheckpoints::push_back(const PackedCheckpoint& checkpoint)
{
  const u8 n = this->num_active_checkpoints;
  if (n < MAX_ACTIVE_CHECKPOINTS) {
    this->checkpoints[n] = checkpoint;
    this->num_active_checkpoints = n + 1;
  } else {
    for (u8 i = 0; i < MAX_ACTIVE_CHECKPOINTS - 1; ++i) {
      this->checkpoints[i] = this->checkpoints[i + 1];
    }
    this->checkpoints[MAX_ACTIVE_CHECKPOINTS - 1] = checkpoint;
  }
}

PackedCheckpoint ActiveCheckpoints::newest() const
{
  BATT_CHECK_NE(this->num_active_checkpoints, 0);
  return this->checkpoints[this->num_active_checkpoints - 1];
}

PackedCheckpoint ActiveCheckpoints::oldest() const
{
  BATT_CHECK_NE(this->num_active_checkpoints, 0);
  return this->checkpoints[0];
}

// TODO: [Gabe Bornstein 8/25/26] Not sure this is right or necessary. We could just use trace_refs
// on a single PackedCheckpoint.
//
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const ActiveCheckpoints& active)
{
  std::vector<llfs::PageId> refs;
  for (u8 i = 0; i < active.num_active_checkpoints; ++i) {
    refs.push_back(active.checkpoints[i].new_tree_root.as_page_id());
  }
  return batt::into_seq(std::move(refs)) | llfs::seq::map([](const llfs::PageId& id) {
           return id;
         }) |
         llfs::seq::boxed();
}

}  // namespace turtle_kv
