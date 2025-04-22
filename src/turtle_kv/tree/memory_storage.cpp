#include <turtle_kv/tree/memory_storage.hpp>
//

#include <turtle_kv/import/logging.hpp>
#include <turtle_kv/tree/config.hpp>

#include <llfs/config.hpp>
#include <llfs/memory_page_arena.hpp>

#include <batteries/assert.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

std::shared_ptr<llfs::PageCache> make_memory_page_cache(batt::TaskScheduler& scheduler,
                                                        const TreeOptions& opts,
                                                        usize byte_capacity)
{
  const auto n_leaf_pages =
      llfs::PageCount{(byte_capacity + opts.leaf_size() - 1) / opts.leaf_size()};

  const auto n_node_pages = llfs::PageCount{
      ((n_leaf_pages + opts.max_page_refs_per_node() - 1) / opts.max_page_refs_per_node()) *
          kMaxNodeUtilizationRatio * /*slack_factor=*/16 +
      kMaxTreeHeight * 2};

  VLOG(1) << "n_leaf_pages=" << n_leaf_pages;
  VLOG(1) << "n_node_pages=" << n_node_pages;

  std::vector<llfs::PageArena> arenas;

  arenas.emplace_back(llfs::make_memory_page_arena(scheduler,
                                                   n_leaf_pages,
                                                   opts.leaf_size(),
                                                   /*name=*/"Leaf",
                                                   /*device_id=*/0));

  arenas.emplace_back(llfs::make_memory_page_arena(scheduler,
                                                   n_node_pages,
                                                   opts.node_size(),
                                                   /*name=*/"Node",
                                                   /*device_id=*/1));

  arenas.emplace_back(llfs::make_memory_page_arena(scheduler,
                                                   n_leaf_pages,
                                                   opts.filter_page_size(),
                                                   /*name=*/"Filter",
                                                   /*device_id=*/2));

  auto cache_options = llfs::PageCacheOptions::with_default_values();

  cache_options.set_byte_size((byte_capacity * 5 + 1) / 4);

  return BATT_OK_RESULT_OR_PANIC(llfs::PageCache::make_shared(
      /*storage_pool=*/std::move(arenas),
      /*options=*/cache_options));
}

}  // namespace turtle_kv
