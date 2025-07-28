#include <turtle_kv/tree/storage_config.hpp>
//
#include <turtle_kv/import/constants.hpp>

#include <llfs/page_size.hpp>

#include <set>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageCacheOptions page_cache_options_from(const TreeOptions& tree_options,
                                               usize cache_size_bytes)
{
  auto page_cache_options = llfs::PageCacheOptions::with_default_values();

  std::set<llfs::PageSize> sharded_leaf_views;

  sharded_leaf_views.insert(llfs::PageSize{4 * kKiB});
  sharded_leaf_views.insert(tree_options.trie_index_sharded_view_size());

  for (llfs::PageSize view_size : sharded_leaf_views) {
    if (tree_options.leaf_size() != view_size) {
      page_cache_options.add_sharded_view(tree_options.leaf_size(), view_size);
    }
  }

  VLOG(1) << BATT_INSPECT_RANGE_PRETTY(sharded_leaf_views);

  page_cache_options.set_byte_size(cache_size_bytes,
                                   /*default_page_size=*/llfs::PageSize{4 * kKiB});

  return page_cache_options;
}

}  // namespace turtle_kv
