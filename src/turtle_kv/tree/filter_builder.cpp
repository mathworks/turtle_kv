#include <turtle_kv/tree/filter_builder.hpp>
//

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <llfs/bloom_filter_page_view.hpp>
#include <llfs/buffer.hpp>
#include <llfs/packed_bloom_filter_page.hpp>

#include <batteries/async/task.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ FilterBuilder::FilterBuilder(const TreeOptions& tree_options,
                                          batt::WorkerPool& worker_pool) noexcept
    : tree_options_{tree_options}
    , worker_pool_{worker_pool}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool FilterBuilder::accepts_page(const llfs::PinnedPage& pinned_page) const noexcept /*override*/
{
  return LeafPageView::layout_used_by_page(pinned_page);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status FilterBuilder::build_filter(
    const llfs::PinnedPage& src_pinned_page,
    const std::shared_ptr<llfs::PageBuffer>& dst_filter_page_buffer) noexcept /*override*/
{
  // Sanity check.
  //
  BATT_CHECK(LeafPageView::layout_used_by_page(src_pinned_page));

  // Unpack the cache slot to get the leaf page buffer.
  //
  const llfs::PageView& page_view = *src_pinned_page;

  // Set the page layout in the destination page buffer.
  //
  llfs::PackedPageHeader* const filter_page_header =
      llfs::mutable_page_header(dst_filter_page_buffer.get());

  filter_page_header->layout_id = llfs::BloomFilterPageView::page_layout_id();

  // Pack the filter page header metadata.
  //
  MutableBuffer payload_buffer = dst_filter_page_buffer->mutable_payload();
  auto* const packed_filter_page = static_cast<llfs::PackedBloomFilterPage*>(payload_buffer.data());

  std::memset(packed_filter_page, 0, sizeof(llfs::PackedBloomFilterPage));

  packed_filter_page->src_page_id = llfs::PackedPageId::from(src_pinned_page.page_id());

  const u64 max_word_count =
      (payload_buffer.size() - sizeof(llfs::PackedBloomFilterPage)) / sizeof(little_u64);

  // Calculate the optimal number of hash functions to use.
  //
  const PackedLeafPage& packed_leaf = PackedLeafPage::view_of(page_view);

  // Grab the item count to figure out how to size the filter.
  //
  const u64 item_count = packed_leaf.key_count;

  // Select the filter layout (flat, blocked-64, or blocked-512).
  //
  const auto layout = llfs::BloomFilterLayout::kBlocked512;

  // Calculate the total filter bits to use.
  //
  const u64 filter_page_capacity_in_bits = max_word_count * 64;
  const u64 filter_target_bits = item_count * this->tree_options_.filter_bits_per_key();
  const u64 filter_size_in_bits =
      std::max<u64>(512, std::min(filter_page_capacity_in_bits, filter_target_bits));

  u64 word_count = batt::round_up_bits(9, filter_size_in_bits) / 64;
  if (word_count * 64 > filter_page_capacity_in_bits) {
    word_count -= 8;
    BATT_CHECK_LE(word_count * 64, filter_page_capacity_in_bits);
  }

  // Configure the filter.
  //
  auto config = llfs::BloomFilterConfig::from(layout,
                                              llfs::Word64Count{word_count},
                                              llfs::ItemCount{item_count});

  BATT_CHECK_EQ(config.filter_size, word_count);

  // Initialize the filter header.
  //
  packed_filter_page->bloom_filter.initialize(config);

  // If the actual filter size is less than the page capacity, then don't write the whole page.
  //
  const void* const filter_data_end = &packed_filter_page->bloom_filter.words[word_count];
  filter_page_header->unused_end = filter_page_header->size;
  filter_page_header->unused_begin = llfs::byte_distance(filter_page_header,  //
                                                         filter_data_end);

  BATT_CHECK_LE(filter_page_header->unused_begin, dst_filter_page_buffer->size());

  // Now the header is full initialized; build the full filter from the packed edits.
  //
  llfs::parallel_build_bloom_filter(this->worker_pool_,
                                    packed_leaf.items_begin(),
                                    packed_leaf.items_end(),
                                    BATT_OVERLOADS_OF(get_key),
                                    &packed_filter_page->bloom_filter);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // PARANOID CHECK - remove in release builds! TODO [tastolfi 2025-02-02]
  //
  if (false) {
    std::for_each(packed_leaf.items_begin(), packed_leaf.items_end(), [&](const auto& packed_item) {
      const KeyView& key = get_key(packed_item);
      BATT_CHECK(packed_filter_page->bloom_filter.might_contain(key));
    });
    LOG_EVERY_N(INFO, 250) << "filter paranoid check PASSED";
    if (false) {
      LOG(INFO) << BATT_INSPECT(config) << " filter_page=" << dst_filter_page_buffer->page_id()
                << packed_filter_page->bloom_filter.dump();

      std::for_each(packed_leaf.items_begin(),
                    packed_leaf.items_end(),
                    [&](const auto& packed_item) {
                      const KeyView& key = get_key(packed_item);
                      auto& filter = packed_filter_page->bloom_filter;
                      llfs::BloomFilterQuery<const KeyView&> query{key};
                      query.update_mask(config.hash_count, 8);
                      LOG(INFO) << BATT_INSPECT_STR(key) << BATT_INSPECT(query)
                                << BATT_INSPECT(config.hash_count)
                                << BATT_INSPECT(filter.hash_count());
                    });
    }
  }
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  return OkStatus();
}

}  // namespace turtle_kv
