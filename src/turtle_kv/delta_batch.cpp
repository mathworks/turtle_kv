#include <turtle_kv/delta_batch.hpp>
//

namespace turtle_kv {

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

}  // namespace turtle_kv
