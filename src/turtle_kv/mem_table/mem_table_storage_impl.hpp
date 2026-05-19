//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_STORAGE_IMPL_HPP

#include <turtle_kv/mem_table/mem_table_storage.hpp>

#include <turtle_kv/change_log/change_log_writer.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

namespace turtle_kv {

class MemTableChangeLogStorage
{
 public:
  using Writer = ChangeLogWriter;
  using WriterContext = ChangeLogWriter::Context;
  using BlockBuffer = ChangeLogWriter::BlockBuffer;
};

static_assert(MemTableStorageWriterContext<MemTableChangeLogStorage::WriterContext>);
static_assert(MemTableStorage<MemTableChangeLogStorage>);

}  // namespace turtle_kv
