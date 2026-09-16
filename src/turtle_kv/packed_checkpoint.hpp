#pragma once

#include <turtle_kv/import/int_types.hpp>

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

const u8 MAX_ACTIVE_CHECKPOINTS = 8;

struct PackedActiveCheckpoints {
  std::array<PackedCheckpoint, MAX_ACTIVE_CHECKPOINTS> checkpoints;
  little_u8 num_active_checkpoints;

  // Appends a checkpoint to the active set, maintaining sorted order by edit_offset_upper_bound.
  // If the set is already at capacity, the oldest (index 0) is evicted.
  //
  void push_back(const PackedCheckpoint& checkpoint);

  const PackedCheckpoint& newest() const;

  const PackedCheckpoint& oldest() const;

  // Linear search that looks for an exact match for `edit_offset`. Returns `nullptr` if not found.
  //
  const PackedCheckpoint* find(i64 edit_offset) const;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedActiveCheckpoints),
                      sizeof(PackedCheckpoint) * MAX_ACTIVE_CHECKPOINTS + sizeof(little_u8));

LLFS_SIMPLE_PACKED_TYPE(PackedActiveCheckpoints);

llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedActiveCheckpoints& active);

}  // namespace turtle_kv
