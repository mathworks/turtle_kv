#pragma once

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/import/optional.hpp>

#include <variant>
#include <vector>

namespace turtle_kv {

/** \brief Requests that the new checkpoint be pinned (see `KVStore::create_snapshot`).
 */
struct PinRequest {
};

/** \brief Requests that the pinned checkpoints at the given edit offsets be unpinned (see
 * `KVStore::delete_snapshots`).
 */
struct UnpinRequest {
  std::vector<EditOffset> edit_offsets;
};

/** \brief A change to the set of pinned checkpoints: either a pin or an unpin (mutually exclusive).
 */
using CheckpointRequest = std::variant<PinRequest, UnpinRequest>;

/** \brief Changes to the set of pinned checkpoints requested by `KVStore::create_snapshot` and
 * `KVStore::delete_snapshots`.  These are attached to a MemTable (see MemTable::request_pin and
 * MemTable::request_unpin) and carried out when the checkpoint created from that MemTable is
 * finalized (see CheckpointGenerator::finalize_checkpoint).
 *
 * Because snapshot creations/deletions are serialized and each waits for its checkpoint to be
 * committed, a MemTable carries at most one request (pin or unpin).
 */
struct CheckpointRequests {
  // The requested change, or None if no request was made.
  //
  Optional<CheckpointRequest> request;

  // Set when the MemTable is finalized; once true, no more requests can be attached.  Always true
  // by the time the checkpoint pipeline reads the requests.
  //
  bool closed = false;

  // Returns true if no request was made (ignores `closed`).
  //
  bool empty() const noexcept
  {
    return !this->request;
  }
};

}  // namespace turtle_kv
