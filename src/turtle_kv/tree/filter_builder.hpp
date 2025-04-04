#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/util/pipeline_channel.hpp>

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
#include <mutex>
#include <thread>
#include <vector>

namespace turtle_kv {

struct BloomFilterMetrics {
  using Self = BloomFilterMetrics;

  static Self& instance()
  {
    static Self self_;
    return self_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatsMetric<u64> word_count_stats;
  StatsMetric<u64> byte_size_stats;
  StatsMetric<u64> bit_size_stats;
  StatsMetric<u64> bit_count_stats;
  StatsMetric<u64> item_count_stats;
};

struct BuildFilterJob {
  std::function<StatusOr<bool>()> work_fn;
  batt::Promise<bool> result;
};

template <typename ItemsT>
Status build_bloom_filter_for_leaf(llfs::PageCache& page_cache,
                                   usize filter_bits_per_key,
                                   llfs::PageId leaf_page_id,
                                   const ItemsT& items)
{
  static std::mutex channel_mutex;

  static batt::ScopedWorkerThreadPool* filter_pool =
      new batt::ScopedWorkerThreadPool{BATT_OK_RESULT_OR_PANIC(
          batt::parse_thread_spec("8 9 10 11 12 13 14 15 40 41 42 43 44 45 46 47"))};

  static PipelineChannel<BuildFilterJob> channel;

  [[maybe_unused]] static std::thread* filter_thread = new std::thread{[&] {
    for (;;) {
      BuildFilterJob job = BATT_OK_RESULT_OR_PANIC(channel.read());
      job.result.set_value(job.work_fn());
    }
  }};

  BuildFilterJob job;
  job.work_fn = [&]() -> StatusOr<bool> {
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

    BATT_ASSIGN_OK_RESULT(
        const llfs::PackedBloomFilterPage* packed_filter,
        llfs::build_bloom_filter_page(filter_pool->worker_pool(),  // batt::WorkerPool::null_pool(),
                                      items,
                                      BATT_OVERLOADS_OF(get_key),
                                      llfs::BloomFilterLayout::kBlocked512,
                                      filter_bits_per_key,
                                      leaf_page_id,
                                      filter_buffer.get()));

    {
      auto& metrics = BloomFilterMetrics::instance();

      const usize item_count = std::distance(items.begin(), items.end());

      metrics.word_count_stats.update(packed_filter->bloom_filter.word_count());
      metrics.byte_size_stats.update(packed_filter->bloom_filter.word_count() * 8);
      metrics.bit_size_stats.update(packed_filter->bloom_filter.word_count() * 64);
      metrics.bit_count_stats.update(packed_filter->bit_count);
      metrics.item_count_stats.update(item_count);
    }

    BATT_REQUIRE_OK(page_cache.put_view(
        std::make_shared<llfs::BloomFilterPageView>(batt::make_copy(filter_buffer)),
        /*callers=*/0,
        /*job_id=*/0));

    // Start writing the page asynchronously.
    //
    filter_page_device.write(std::move(filter_buffer),
                             [](batt::Status /*ignored_for_now*/) mutable {
                             });

    return true;
  };

  {
    std::unique_lock<std::mutex> lock{channel_mutex};
    BATT_CHECK_OK(channel.write(job));
    BATT_REQUIRE_OK(job.result.get_future().await());
  }

  return OkStatus();
}

}  // namespace turtle_kv
