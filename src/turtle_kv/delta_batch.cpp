#include <turtle_kv/delta_batch.hpp>
//

namespace turtle_kv {

#if TURTLE_KV_BIG_MEM_TABLES

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DeltaBatch::DeltaBatch(DeltaBatchId batch_id,
                                    boost::intrusive_ptr<MemTable>&& mem_table,
                                    ResultSet&& result_set) noexcept
    : batch_id_{batch_id}
    , mem_table_{std::move(mem_table)}
    , result_set_{std::move(result_set)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DeltaBatchId DeltaBatch::batch_id() const noexcept
{
  return this->batch_id_;
}

#else  // TURTLE_KV_BIG_MEM_TABLES

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

#endif  // !TURTLE_KV_BIG_MEM_TABLES

}  // namespace turtle_kv
