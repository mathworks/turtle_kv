//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/mem_table/mem_table.hpp>
//

#include <turtle_kv/mem_table/mem_table.ipp>

namespace turtle_kv {

template class BasicMemTable<MemTableChangeLogStorage, MemTablePageCacheAllocationTracker>;

}  // namespace turtle_kv
