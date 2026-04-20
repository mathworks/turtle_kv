//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_MOCK_MEM_TABLE_STORAGE_TEST_HPP

#include <turtle_kv/mem_table/mem_table_storage.hpp>

#include <gmock/gmock.h>

#include <functional>

namespace turtle_kv {
namespace testing {

struct MockMemTableStorage {
  struct Writer {
  };

  struct BlockBuffer {
    static constexpr usize kDefaultBlockSize = 4096;

    usize fake_block_size_{kDefaultBlockSize};
    i32 fake_ref_count_{0};

    MOCK_METHOD(usize, block_size, (), (const));
    MOCK_METHOD(i32, ref_count, (), (const));
    MOCK_METHOD(void, add_ref, (i32 delta), (const));
    MOCK_METHOD(void, remove_ref, (i32 count), ());
  };

  struct WriterContext {
    using BlockBuffer = MockMemTableStorage::BlockBuffer;
    using SlotCallbackFn =
        std::function<void(FirstVisitToBlock, BlockBuffer*, MutableBuffer, EditOffset)>;

    MOCK_METHOD(Status, append_slot, (usize /*byte_count*/, SlotCallbackFn), ());
  };
};

static_assert(MemTableStorageBlockBuffer<MockMemTableStorage::BlockBuffer>);
static_assert(MemTableStorageWriterContext<MockMemTableStorage::WriterContext>);
static_assert(MemTableStorage<MockMemTableStorage>);

}  // namespace testing
}  // namespace turtle_kv
