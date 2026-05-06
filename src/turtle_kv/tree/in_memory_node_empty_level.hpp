#pragma once

#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/update_buffer_levels.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/small_fn.hpp>

#include <llfs/page_loader.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief An empty update buffer level.
 */
struct InMemoryNodeEmptyLevel {
  using Self = InMemoryNodeEmptyLevel;

  void drop_after_pivot(i32 split_pivot_i [[maybe_unused]],
                        const KeyView& split_pivot_key [[maybe_unused]],
                        llfs::PageLoader& page_loader [[maybe_unused]],
                        const TreeOptions& tree_options [[maybe_unused]])
  {
    // Nothing to do!
  }

  void drop_before_pivot(i32 split_pivot_i [[maybe_unused]],
                         const KeyView& split_pivot_key [[maybe_unused]],
                         llfs::PageLoader& page_loader [[maybe_unused]],
                         const TreeOptions& tree_options [[maybe_unused]])
  {
    // Nothing to do!
  }

  InMemoryNodeLevel merge(InMemoryNodeLevel&& sibling_level, usize node_pivot_count) &&;

  SmallFn<void(std::ostream&)> dump() const;
};

}  // namespace turtle_kv
