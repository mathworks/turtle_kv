#include <turtle_kv/on_page_cache_overcommit.hpp>
//
#include <turtle_kv/util/memory_stats.hpp>

#include <turtle_kv/import/logging.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void on_page_cache_overcommit(const std::function<void(std::ostream& out)>& context_fn,
                              llfs::PageCache& cache,
                              OvercommitMetrics& metrics)
{
  metrics.trigger_count.add(1);

  auto& cache_slots = llfs::PageCacheSlot::Pool::Metrics::instance();

  const auto print_page_alloc_info = [&](std::ostream& out) {
    for (const llfs::PageDeviceEntry* entry : cache.all_devices()) {
      if (!entry->can_alloc || !entry->arena.has_allocator()) {
        continue;
      }
      out << entry->arena.allocator().debug_info() << "\n";
    }
  };

  // TODO [tastolfi 2025-11-28] remove? silence?
  //
  LOG(INFO) << context_fn << std::endl
            << BATT_INSPECT(cache_slots.admit_count) << BATT_INSPECT(cache_slots.insert_count)
            << BATT_INSPECT(cache_slots.erase_count) << BATT_INSPECT(cache_slots.evict_count)
            << std::endl
            << BATT_INSPECT(cache_slots.estimate_cache_bytes()) << "("
            << batt::dump_size(cache_slots.estimate_cache_bytes()) << ")" << std::endl
            << BATT_INSPECT(cache_slots.estimate_pinned_bytes()) << "("
            << batt::dump_size(cache_slots.estimate_pinned_bytes()) << ")" << std::endl
            << print_page_alloc_info << dump_memory_stats();
}

}  // namespace turtle_kv
