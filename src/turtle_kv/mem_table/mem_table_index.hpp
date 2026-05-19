//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_INDEX_HPP

#include <turtle_kv/mem_table/mem_table_entry.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <type_traits>

namespace turtle_kv {

class MemTableValueEntry;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief
 */
template <typename S, typename T>
concept MemTableIndexScanner = requires(S scanner, T& index) {
  /** \brief Must be constructible from reference to index.
   */
  S{index};

  /** \brief Must be move-constructible.
   */
  S{std::move(scanner)};

  /** \brief Must have public destructor.
   */
  scanner.~S();

  /** \brief Must be move-assignable.
   */
  { scanner = std::move(scanner) } -> std::same_as<S&>;

  /** \brief Must have is_done() member function returning bool (whether there are any more values
   * in the scan).
   */
  { scanner.is_done() } -> std::convertible_to<bool>;

  /** \brief Must have advance() member function returning void (moves the current position to the
   * next value).
   */
  { scanner.advance() } -> std::same_as<void>;

  /** \brief Must have get_value() member function (returns a const ref to the current value).
   */
  { scanner.get_value() } -> std::convertible_to<const MemTableValueEntry&>;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Concept that describes the type requirements for MemTable
 * Index types.
 *
 * A MemTable _Index_ is a type providing in-memory key/value lookup.  The underlying data
 * structures must be thread-safe.
 */
template <typename T, typename NormalInserter>
concept MemTableIndex = requires(T index,
                                 const KeyView& key,
                                 NormalInserter inserter,
                                 MemTableRecoveryInserter recovery_inserter) {
  requires MemTableEntryInserter<NormalInserter>;

  /** \brief Must have a member type RecoveryInserter, which satisfies the requirements of
   * MemTableIndexInserter.
   */
  requires MemTableIndexScanner<typename T::Scanner, T>;
  requires MemTableIndexScanner<typename T::UnsynchronizedScanner, T>;

  { index.insert(key, inserter) } -> std::same_as<Status>;

  { index.insert(key, recovery_inserter) } -> std::same_as<Status>;

  { index.find(key) } -> std::convertible_to<Optional<MemTableValueEntry>>;

  { index.unsynchronized_find(key) } -> std::convertible_to<Optional<MemTableValueEntry>>;
};

}  // namespace turtle_kv
