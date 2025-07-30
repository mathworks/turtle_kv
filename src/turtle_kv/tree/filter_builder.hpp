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
#include <llfs/packed_bloom_filter_page.hpp>
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
  LatencyMetric build_page_latency;
};

struct FilterPageAlloc {
  llfs::PageCache& page_cache_;
  llfs::PageId leaf_page_id_;
  Status status;
  llfs::PinnedPage pinned_filter_page;
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
      BATT_ASSIGN_OK_RESULT(
          this->pinned_filter_page,
          page_cache.allocate_filter_page_for(leaf_page_id,
                                              llfs::LruPriority{kNewFilterLruPriority}));

      this->filter_page_id = this->pinned_filter_page.page_id();
      BATT_CHECK(this->filter_page_id);

      BATT_ASSIGN_OK_RESULT(this->filter_buffer, this->pinned_filter_page->get_new_page_buffer());

      return OkStatus();
    }();
  }

  template <typename ViewT, typename... ExtraViewArgs>
  Status commit_page(batt::StaticType<ViewT>, ExtraViewArgs&&... extra_args)
  {
    BATT_REQUIRE_OK(this->pinned_filter_page->set_new_page_view(
        std::make_shared<ViewT>(batt::make_copy(this->filter_buffer),
                                BATT_FORWARD(extra_args)...)));

    this->page_cache_.async_write_filter_page(this->pinned_filter_page,
                                              [](batt::Status /*ignored_for_now*/) mutable {
                                              });

    return OkStatus();
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
  if (filter_bits_per_key == 0) {
    return OkStatus();
  }

  auto& metrics = BloomFilterMetrics::instance();

  FilterPageAlloc alloc{page_cache, leaf_page_id};
  BATT_REQUIRE_OK(alloc.status);

  LatencyTimer timer{Every2ToTheConst<8>{}, metrics.build_page_latency};

  BATT_ASSIGN_OK_RESULT(const llfs::PackedBloomFilterPage* packed_filter,
                        llfs::build_bloom_filter_page(batt::WorkerPool::null_pool(),
                                                      items,
                                                      BATT_OVERLOADS_OF(get_key),
                                                      llfs::BloomFilterLayout::kBlocked512,
                                                      filter_bits_per_key,
                                                      /*opt_hash_count=*/None,
                                                      leaf_page_id,
                                                      alloc.filter_buffer));

  timer.stop();

  {
    const usize item_count = std::distance(items.begin(), items.end());

    metrics.word_count_stats.update(packed_filter->bloom_filter.word_count());
    metrics.byte_size_stats.update(packed_filter->bloom_filter.word_count() * 8);
    metrics.bit_size_stats.update(packed_filter->bloom_filter.word_count() * 64);
    metrics.bit_count_stats.update(packed_filter->bit_count);
    metrics.item_count_stats.update(item_count);
  }

  BATT_REQUIRE_OK(alloc.commit_page(batt::StaticType<llfs::BloomFilterPageView>{}));

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
  LatencyMetric build_page_latency;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <int TAG_BITS, typename ItemsT>
inline Status build_vqf_filter(const MutableBuffer& filter_buffer,
                               const ItemsT& items,
                               u64 nslots,
                               usize hash_val_shift = 0)
{
  auto& metrics = QuotientFilterMetrics::instance();

  LatencyTimer timer{Every2ToTheConst<8>{}, metrics.build_page_latency};

  auto* const packed_filter_header = static_cast<PackedVqfFilter*>(filter_buffer.data());
  vqf_filter<TAG_BITS>* packed_filter = packed_filter_header->get_impl<TAG_BITS>();
  const u64 mask = ~u64{0} << hash_val_shift;

  packed_filter_header->hash_seed = kVqfHashSeed;
  packed_filter_header->hash_mask = mask;

  vqf_init_in_place(packed_filter, nslots);

  const u64 actual_size = vqf_filter_size(packed_filter);
  BATT_CHECK_LE(actual_size,
                filter_buffer.size() - (sizeof(PackedVqfFilter) - sizeof(vqf_metadata)));

  metrics.byte_size_stats.update(actual_size);
  metrics.bit_size_stats.update(actual_size * 8);
  metrics.item_count_stats.update(items.size());
  metrics.bits_per_key_stats.update((actual_size * 8 + 4) / items.size());

  for (const auto& item : items) {
    const u64 hash_val = vqf_hash_val(get_key(item));

    // If `hash_val_shift` is non-zero, then we randomly drop some portion of the input to save
    // space; when answering queries, we always is_present=true for dropped hash values.
    //
    if ((hash_val & mask) == hash_val) {
      BATT_CHECK(vqf_insert(packed_filter, hash_val))
          << BATT_INSPECT(hash_val_shift) << BATT_INSPECT(std::bitset<64>{mask});
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
  if (filter_bits_per_key == 0) {
    return OkStatus();
  }

  FilterPageAlloc alloc{page_cache, leaf_page_id};
  BATT_REQUIRE_OK(alloc.status);

  llfs::PackedPageHeader* const filter_page_header = mutable_page_header(alloc.filter_buffer.get());
  MutableBuffer payload_buffer = alloc.filter_buffer->mutable_payload();

  filter_page_header->layout_id = VqfFilterPageView::page_layout_id();

  const MutableBuffer filter_buffer = payload_buffer;

  const usize kFilterHeaderSize = sizeof(PackedVqfFilter) - sizeof(vqf_metadata);

  const usize max_slots_8bit = vqf_nslots_for_size(8, filter_buffer.size() - kFilterHeaderSize);
  const usize max_slots_16bit = vqf_nslots_for_size(16, filter_buffer.size() - kFilterHeaderSize);

  const double n_keys = items.size();
  const double load_factor_8bit = vqf_filter_load_factor<8>(filter_bits_per_key);
  const double load_factor_16bit = vqf_filter_load_factor<16>(filter_bits_per_key);

  const usize n_slots_8bit = std::floor(n_keys / load_factor_8bit);
  const usize n_slots_16bit = std::floor(n_keys / load_factor_16bit);

  BATT_DEBUG_INFO(std::endl
                  << BATT_INSPECT(filter_bits_per_key) << std::endl
                  << BATT_INSPECT(n_keys) << std::endl
                  << BATT_INSPECT(load_factor_8bit) << std::endl
                  << BATT_INSPECT(n_slots_8bit) << std::endl
                  << BATT_INSPECT(max_slots_8bit) << std::endl
                  << BATT_INSPECT(load_factor_16bit) << std::endl
                  << BATT_INSPECT(n_slots_16bit) << std::endl
                  << BATT_INSPECT(max_slots_16bit) << std::endl
                  << BATT_INSPECT(filter_bits_per_key) << std::endl
                  << BATT_INSPECT(filter_page_header->size));

  BATT_CHECK_LE(load_factor_8bit, kMaxQuotientFilterLoadFactor);

  usize filter_size = 0;

  if (load_factor_16bit <= kMaxQuotientFilterLoadFactor && n_slots_16bit <= max_slots_16bit) {
    filter_size = vqf_required_size<16>(n_slots_16bit);
    BATT_REQUIRE_OK(build_vqf_filter<16>(filter_buffer, items, n_slots_16bit));

  } else if (n_slots_8bit <= max_slots_8bit) {
    filter_size = vqf_required_size<8>(n_slots_8bit);
    BATT_REQUIRE_OK(build_vqf_filter<8>(filter_buffer, items, n_slots_8bit));

  } else {
    BATT_CHECK_GT(max_slots_8bit, max_slots_16bit);

    usize hash_val_shift = 1;
    while ((items.size() >> hash_val_shift) / load_factor_8bit > max_slots_8bit) {
      ++hash_val_shift;
    }
    LOG_FIRST_N(WARNING, 10) << "Truncating hash values to fit filter!"
                             << BATT_INSPECT(hash_val_shift);

    filter_size = vqf_required_size<8>(max_slots_8bit);
    BATT_REQUIRE_OK(build_vqf_filter<8>(filter_buffer, items, max_slots_8bit, hash_val_shift));
  }
  BATT_CHECK_NE(filter_size, 0);

  filter_page_header->unused_begin =
      byte_distance(filter_page_header, filter_buffer.data()) + kFilterHeaderSize + filter_size;

  filter_page_header->unused_end = filter_page_header->size;

  BATT_REQUIRE_OK(alloc.commit_page(batt::StaticType<VqfFilterPageView>{}));

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
#if TURTLE_KV_USE_BLOOM_FILTER
  Status filter_status =
      build_bloom_filter_for_leaf(page_cache, filter_bits_per_key, leaf_page_id, items);

#elif TURTLE_KV_USE_QUOTIENT_FILTER
  Status filter_status =
      build_quotient_filter_for_leaf(page_cache, filter_bits_per_key, leaf_page_id, items);

#endif  //----- --- -- -  -  -   -

  if (!filter_status.ok()) {
    LOG_FIRST_N(WARNING, 10) << "Failed to build filter: " << filter_status;
  }

  return
      [](llfs::PageCacheJob&, std::shared_ptr<llfs::PageBuffer>&&) -> StatusOr<llfs::PinnedPage> {
        return {llfs::PinnedPage{}};
      };
}

}  // namespace turtle_kv
