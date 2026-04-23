//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_STORAGE_HPP

#include <turtle_kv/change_log/api_types.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/status.hpp>

#include <type_traits>

namespace turtle_kv {

template <typename T>
concept MemTableStorageBlockBuffer =
    requires(T& block_buffer, const T& const_block_buffer, i32 delta) {
      { const_block_buffer.block_size() } -> std::convertible_to<usize>;
      { const_block_buffer.ref_count() } -> std::convertible_to<i32>;
      block_buffer.add_ref(delta);
      block_buffer.remove_ref(delta);
    };

template <typename T>
concept MemTableStorageWriterContext = requires(
    T& context,
    EditOffset min_edit_offset_lower_bound,
    usize byte_count,
    void (*serialize_fn)(FirstVisitToBlock, typename T::BlockBuffer*, MutableBuffer, EditOffset)) {
  requires MemTableStorageBlockBuffer<typename T::BlockBuffer>;

  {
    context.append_slot(min_edit_offset_lower_bound, byte_count, serialize_fn)
  } -> std::same_as<Status>;
};

template <typename T>
concept MemTableStorageWriter = requires(T writer) {
  { &writer } -> std::same_as<T*>;
};

template <typename T>
concept MemTableStorage = requires {
  requires MemTableStorageWriter<typename T::Writer>;
  requires MemTableStorageWriterContext<typename T::WriterContext>;
  requires MemTableStorageBlockBuffer<typename T::BlockBuffer>;
  requires MemTableStorageBlockBuffer<typename T::BlockBuffer>;
};

}  // namespace turtle_kv
