//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ENTRY_INSERTER_HPP

#include <turtle_kv/import/status.hpp>

#include <type_traits>

namespace turtle_kv {

class MemTableValueEntry;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Concept for inserter types used to modify a MemTableIndex.
 */
template <typename T>
concept MemTableEntryInserter = requires(T inserter, void* memory, MemTableValueEntry* entry) {
  /** \brief Must have an insert_new() member function (constructs a MemTableValueEntry using
   * placement new, at the given address).
   */
  { inserter.insert_new(memory) } -> std::same_as<Status>;

  /** \brief Must have an update_existing() member function (updates an existing value object with
   * the new value).
   */
  { inserter.update_existing(entry) } -> std::same_as<Status>;
};

}  // namespace turtle_kv
