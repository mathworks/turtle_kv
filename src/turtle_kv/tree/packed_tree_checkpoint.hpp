#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/seq.hpp>
#include <llfs/simple_packed_type.hpp>

namespace turtle_kv {

struct PackedTreeCheckpoint {
  little_u64 batch_upper_bound;
  llfs::PackedPageId new_tree_root;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedTreeCheckpoint), 16);

LLFS_SIMPLE_PACKED_TYPE(PackedTreeCheckpoint);

std::ostream& operator<<(std::ostream& out, const PackedTreeCheckpoint& t);

llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedTreeCheckpoint& checkpoint);

}  // namespace turtle_kv
