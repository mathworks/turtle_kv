#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/bloom_filter.hpp>
#include <llfs/bloom_filter_page.hpp>
#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/async/worker_pool.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace turtle_kv {

template <typename ItemsT>
Status build_bloom_filter_for_leaf(llfs::PageCache& page_cache,
                                   usize filter_bits_per_key,
                                   llfs::PageId leaf_page_id,
                                   const ItemsT& items)
{
  Optional<llfs::PageId> filter_page_id = page_cache.filter_page_id_for(leaf_page_id);
  if (!filter_page_id) {
    return {batt::StatusCode::kUnavailable};
  }

  llfs::PageDeviceEntry* const filter_device_entry =
      page_cache.get_device_for_page(*filter_page_id);

  BATT_CHECK_NOT_NULLPTR(filter_device_entry);

  llfs::PageDevice& filter_page_device = filter_device_entry->arena.device();

  BATT_ASSIGN_OK_RESULT(std::shared_ptr<llfs::PageBuffer> filter_buffer,
                        filter_page_device.prepare(*filter_page_id));

  BATT_REQUIRE_OK(llfs::build_bloom_filter_page(batt::WorkerPool::null_pool(),
                                                items,
                                                BATT_OVERLOADS_OF(get_key),
                                                llfs::BloomFilterLayout::kBlocked512,
                                                filter_bits_per_key,
                                                leaf_page_id,
                                                filter_buffer.get()));

  BATT_REQUIRE_OK(page_cache.put_view(
      std::make_shared<llfs::BloomFilterPageView>(batt::make_copy(filter_buffer)),
      /*callers=*/0,
      /*job_id=*/0));

  // Start writing the page asynchronously.
  //
  filter_page_device.write(std::move(filter_buffer), [](batt::Status /*ignored_for_now*/) mutable {
  });

  return OkStatus();
}

}  // namespace turtle_kv
