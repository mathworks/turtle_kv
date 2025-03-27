#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/seq.hpp>
#include <llfs/simple_packed_type.hpp>

namespace turtle_kv {

struct PackedCheckpoint {
  little_u64 batch_upper_bound;
  llfs::PackedPageId new_tree_root;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedCheckpoint), 16);

LLFS_SIMPLE_PACKED_TYPE(PackedCheckpoint);

std::ostream& operator<<(std::ostream& out, const PackedCheckpoint& t);

llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedCheckpoint& checkpoint);

}  // namespace turtle_kv
