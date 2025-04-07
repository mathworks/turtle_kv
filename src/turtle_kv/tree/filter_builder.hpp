#pragma once

#include <turtle_kv/config.hpp>

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/util/pipeline_channel.hpp>

#include <turtle_kv/vqf_filter_page_view.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/bloom_filter.hpp>
#include <llfs/bloom_filter_page.hpp>
#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/pinned_page.hpp>

#include <vqf/vqf_filter.h>

#include <batteries/async/debug_info.hpp>
#include <batteries/async/worker_pool.hpp>

#include <atomic>
#include <bitset>
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

struct FilterPageAlloc {
  llfs::PageCache& page_cache_;
  llfs::PageId leaf_page_id_;
  Status status;
  Optional<llfs::PageId> filter_page_id;
  llfs::PageDeviceEntry* filter_device_entry = nullptr;
  llfs::PageDevice* filter_page_device = nullptr;
  std::shared_ptr<llfs::PageBuffer> filter_buffer;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit FilterPageAlloc(llfs::PageCache& page_cache, llfs::PageId leaf_page_id) noexcept
      : page_cache_{page_cache}
      , leaf_page_id_{leaf_page_id}
  {
    this->status = [&]() -> Status {
      this->filter_page_id = page_cache.filter_page_id_for(leaf_page_id);
      if (!this->filter_page_id) {
        return batt::StatusCode::kUnavailable;
      }

      this->filter_device_entry = page_cache.get_device_for_page(*filter_page_id);
      BATT_CHECK_NOT_NULLPTR(this->filter_device_entry);

      this->filter_page_device = std::addressof(this->filter_device_entry->arena.device());

      BATT_ASSIGN_OK_RESULT(this->filter_buffer,
                            this->filter_page_device->prepare(*this->filter_page_id));

      return OkStatus();
    }();
  }

  void start_write()
  {
    this->filter_page_device->write(std::move(this->filter_buffer),
                                    [](batt::Status /*ignored_for_now*/) mutable {
                                    });
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsT>
Status build_bloom_filter_for_leaf(llfs::PageCache& page_cache,
                                   usize filter_bits_per_key,
                                   llfs::PageId leaf_page_id,
                                   const ItemsT& items)
{
  FilterPageAlloc alloc{page_cache, leaf_page_id};
  BATT_REQUIRE_OK(alloc.status);

  BATT_ASSIGN_OK_RESULT(const llfs::PackedBloomFilterPage* packed_filter,
                        llfs::build_bloom_filter_page(batt::WorkerPool::null_pool(),
                                                      items,
                                                      BATT_OVERLOADS_OF(get_key),
                                                      llfs::BloomFilterLayout::kBlocked512,
                                                      filter_bits_per_key,
                                                      leaf_page_id,
                                                      alloc.filter_buffer.get()));

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
      std::make_shared<llfs::BloomFilterPageView>(batt::make_copy(alloc.filter_buffer)),
      /*callers=*/0,
      /*job_id=*/0));

  // Start writing the page asynchronously.
  //
  alloc.start_write();

  return OkStatus();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct QuotientFilterMetrics {
  using Self = QuotientFilterMetrics;

  static Self& instance()
  {
    static Self self_;
    return self_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatsMetric<u64> byte_size_stats;
  StatsMetric<u64> bit_size_stats;
  StatsMetric<u64> item_count_stats;
  StatsMetric<u64> bits_per_key_stats;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <int TAG_BITS, typename ItemsT>
inline Status build_vqf_filter(const MutableBuffer& filter_buffer, const ItemsT& items)
{
  auto* packed_filter = static_cast<PackedVqfFilter*>(filter_buffer.data());

  usize shift = 0;
  u64 mask = ~u64{0};

  vqf_filter<TAG_BITS>* filter = packed_filter->get_impl<TAG_BITS>();
  const u64 nslots = vqf_nslots_for_size(TAG_BITS, filter_buffer.size());

  while ((items.size() >> shift) > nslots * kVqfFilterMaxLoadFactorPercent / 100) {
    BATT_CHECK_LT(shift, 8);
    shift += 1;
    mask <<= 1;
  }

  packed_filter->hash_seed = kVqfHashSeed;
  packed_filter->hash_mask = mask;

  vqf_init_in_place(filter, nslots);

  const u64 actual_size = vqf_filter_size(filter);

  auto& metrics = QuotientFilterMetrics::instance();

  metrics.byte_size_stats.update(actual_size);
  metrics.bit_size_stats.update(actual_size * 8);
  metrics.item_count_stats.update(items.size());
  metrics.bits_per_key_stats.update((actual_size * 8 + 4) / items.size());

  for (const auto& item : items) {
    u64 hash_val = vqf_hash_val(get_key(item));
    if ((hash_val & mask) == hash_val) {
      BATT_CHECK(vqf_insert(filter, hash_val))
          << BATT_INSPECT(shift) << BATT_INSPECT(std::bitset<64>{mask});
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsT>
Status build_quotient_filter_for_leaf(llfs::PageCache& page_cache,
                                      usize filter_bits_per_key,
                                      llfs::PageId leaf_page_id,
                                      const ItemsT& items)
{
  BATT_DEBUG_INFO(BATT_INSPECT(filter_bits_per_key));

  FilterPageAlloc alloc{page_cache, leaf_page_id};
  BATT_REQUIRE_OK(alloc.status);

  llfs::PackedPageHeader* const filter_page_header = mutable_page_header(alloc.filter_buffer.get());
  MutableBuffer payload_buffer = alloc.filter_buffer->mutable_payload();

  filter_page_header->layout_id = VqfFilterPageView::page_layout_id();

  const u64 max_size = payload_buffer.size() - 16;
  const u64 target_size = (items.size() * filter_bits_per_key + 7) / 8;
  const u64 effective_size = std::min(max_size, target_size);

  const MutableBuffer filter_buffer{payload_buffer.data(), effective_size};

  if (filter_bits_per_key < 21) {
    BATT_REQUIRE_OK(build_vqf_filter<8>(filter_buffer, items));
  } else {
    BATT_REQUIRE_OK(build_vqf_filter<16>(filter_buffer, items));
  }

  filter_page_header->unused_begin =
      byte_distance(filter_page_header, filter_buffer.data()) + filter_buffer.size();

  filter_page_header->unused_end = filter_page_header->size;

  BATT_REQUIRE_OK(
      page_cache.put_view(std::make_shared<VqfFilterPageView>(batt::make_copy(alloc.filter_buffer)),
                          /*callers=*/0,
                          /*job_id=*/0));

  alloc.start_write();

  return OkStatus();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsT>
auto build_filter_for_leaf_in_job(llfs::PageCache& page_cache,
                                  usize filter_bits_per_key,
                                  llfs::PageId leaf_page_id,
                                  const ItemsT& items)
{
  Status filter_status =
#if TURTLE_KV_USE_BLOOM_FILTER
      build_bloom_filter_for_leaf(page_cache, filter_bits_per_key, leaf_page_id, items)
#endif
#if TURTLE_KV_USE_QUOTIENT_FILTER
          build_quotient_filter_for_leaf(page_cache, filter_bits_per_key, leaf_page_id, items)
#endif
      ;

  if (!filter_status.ok()) {
    LOG_FIRST_N(WARNING, 10) << "Failed to build filter: " << filter_status;
  }

  return
      [](llfs::PageCacheJob&, std::shared_ptr<llfs::PageBuffer>&&) -> StatusOr<llfs::PinnedPage> {
        return {llfs::PinnedPage{}};
      };
}

}  // namespace turtle_kv
