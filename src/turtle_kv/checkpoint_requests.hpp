#pragma once

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/optional.hpp>

namespace turtle_kv {

/** \brief Changes to the set of pinned checkpoints requested by `KVStore::create_snapshot` and
 * `KVStore::delete_snapshot`.  These are attached to a MemTable (see MemTable::request_pin and
 * MemTable::request_unpin) and carried out when the checkpoint created from that MemTable is
 * finalized (see CheckpointGenerator::finalize_checkpoint).
 *
 * Because snapshot creations/deletions are serialized and each waits for its checkpoint to be
 * committed, a MemTable carries at most one request (pin or unpin).
 */
struct CheckpointRequests {
  // Whether to pin the new checkpoint.
  //
  bool pin = false;

  // The edit offset of a pinned checkpoint to unpin.
  //
  Optional<EditOffset> unpin;

  bool empty() const noexcept
  {
    return !this->pin && !this->unpin;
  }
};

}  // namespace turtle_kv
