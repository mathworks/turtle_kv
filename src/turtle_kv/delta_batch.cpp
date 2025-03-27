#include <turtle_kv/delta_batch.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DeltaBatch::DeltaBatch(boost::intrusive_ptr<MemTable>&& mem_table) noexcept
    : mem_table_{std::move(mem_table)}
    , result_set_{None}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DeltaBatch::merge_compact_edits()
{
  // No need to compact twice!
  //
  if (this->result_set_) {
    BATT_CHECK(!this->result_set_->empty());
    return;
  }

  this->result_set_.emplace(this->mem_table_->compact());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DeltaBatchId DeltaBatch::batch_id() const noexcept
{
  return DeltaBatchId::from_mem_table_id(this->mem_table_->id());
}

}  // namespace turtle_kv
