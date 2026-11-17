#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/seq.hpp>
#include <llfs/simple_packed_type.hpp>

#include <array>

namespace turtle_kv {

struct PackedCheckpoint {
  // EditOffset upper bound for the latest batch group used to create this checkpoint
  //
  little_i64 edit_offset_upper_bound;

  llfs::PackedPageId new_tree_root;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedCheckpoint), 16);

LLFS_SIMPLE_PACKED_TYPE(PackedCheckpoint);

std::ostream& operator<<(std::ostream& out, const PackedCheckpoint& t);

llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedCheckpoint& checkpoint);

//+++++++++++-+-+--+----- --- -- -  -  -   -

const u8 MAX_PINNED_CHECKPOINTS = 8;

/** \brief The set of active (retained) checkpoints: the base checkpoint (the latest checkpoint
 * produced by the checkpoint pipeline) plus any pinned checkpoints (created via
 * `KVStore::create_snapshot`).  A pinned checkpoint may also be the base checkpoint, in which case
 * it appears in both places.
 */
struct PackedActiveCheckpoints {
  // The latest checkpoint; only valid if `has_base_checkpoint` is true.
  //
  PackedCheckpoint base_checkpoint;

  // Pinned checkpoints, in sorted order by edit_offset_upper_bound.
  //
  std::array<PackedCheckpoint, MAX_PINNED_CHECKPOINTS> pinned_checkpoints;

  little_u8 num_pinned_checkpoints;

  // False only before the first checkpoint is created.
  //
  // TODO: [Gabe Bornstein 10/28/26] Do I need this? Or can I just check if base_checkpoint is
  // nullptr or something instead?
  //
  little_u8 has_base_checkpoint;

  // Replaces the base checkpoint.  Returns kFailedPrecondition (and changes nothing) if
  // `checkpoint` is not newer than the current base checkpoint.
  //
  Status set_base_checkpoint(const PackedCheckpoint& checkpoint);

  // Returns the base checkpoint, or kNotFound if there is none.
  //
  StatusOr<PackedCheckpoint> base() const;

  // Returns the pinned checkpoints, oldest to newest.
  //
  Slice<const PackedCheckpoint> pinned() const;

  bool is_pin_limit_reached() const;

  // Pins `checkpoint`.  Returns kFailedPrecondition (and pins nothing) if the pin limit has been
  // reached, or if `checkpoint` is not newer than all currently pinned checkpoints.
  //
  Status pin(const PackedCheckpoint& checkpoint);

  // Unpins the checkpoint with the given `edit_offset`, shifting subsequent pinned checkpoints
  // down.  Returns kNotFound if the checkpoint is not pinned.
  //
  Status unpin(i64 edit_offset);

  // Linear search over the pinned checkpoints that looks for an exact match for `edit_offset`.
  // Returns kNotFound if not found.
  //
  StatusOr<PackedCheckpoint> find_pinned(i64 edit_offset) const;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedActiveCheckpoints),
                      sizeof(PackedCheckpoint) * (1 + MAX_PINNED_CHECKPOINTS) +
                          sizeof(little_u8) * 2);

LLFS_SIMPLE_PACKED_TYPE(PackedActiveCheckpoints);

llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedActiveCheckpoints& active);

}  // namespace turtle_kv
