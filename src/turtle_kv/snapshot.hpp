//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SNAPSHOT_HPP

#include <turtle_kv/checkpoint.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>

namespace turtle_kv {

class Snapshot
{
 public:
  Snapshot() = default;

  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;
  Snapshot(Snapshot&&) = default;
  Snapshot& operator=(Snapshot&&) = default;

  /** \brief Looks up a key in this snapshot's checkpoint tree.
   */
  StatusOr<ValueView> get(const KeyView& key);

  /** \brief Returns the EditOffset upper bound of this snapshot.
   */
  EditOffset edit_offset() const noexcept;

  /** \brief Returns true iff the underlying checkpoint is empty.
   */
  bool is_empty() const noexcept;

 private:
  friend class KVStore;
  friend class KVStoreScanner;

  explicit Snapshot(Checkpoint&& checkpoint,
                    EditOffset edit_offset,
                    llfs::PageCache* page_cache,
                    const TreeOptions* tree_options) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Checkpoint checkpoint_;
  llfs::PageCache* page_cache_ = nullptr;
  const TreeOptions* tree_options_ = nullptr;
};

}  // namespace turtle_kv
