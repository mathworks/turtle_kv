#include <turtle_kv/tree/key_query.hpp>
//

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/import/env.hpp>

namespace turtle_kv {

namespace {

bool try_full_page_query_first();

bool require_sharded_views();

StatusOr<ValueView> find_key_in_pinned_leaf(llfs::PinnedPage& pinned_leaf,
                                            KeyQuery& query,
                                            usize& item_index_out);

StatusOr<ValueView> find_key_in_leaf_using_sharded_views(llfs::PageId leaf_page_id,
                                                         KeyQuery& query,
                                                         usize& item_index_out);

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf(llfs::PageId leaf_page_id,
                                     KeyQuery& query,
                                     usize& item_index_out)
{
  if (query.reject_page(leaf_page_id)) {
    return {batt::StatusCode::kNotFound};
  }

  if (try_full_page_query_first()) {
    KeyQuery::metrics().try_pin_leaf_count.add(1);

    StatusOr<llfs::PinnedPage> full_leaf_page =
        query.page_loader->try_pin_cached_page(leaf_page_id,
                                               LeafPageView::page_layout_id(),
                                               llfs::PinPageToJob::kDefault);

    if (full_leaf_page.ok()) {
      KeyQuery::metrics().try_pin_leaf_success_count.add(1);
      return find_key_in_pinned_leaf(*full_leaf_page, query, item_index_out);
    }
  }

  KeyQuery::metrics().sharded_view_find_count.add(1);

  StatusOr<ValueView> result =
      find_key_in_leaf_using_sharded_views(leaf_page_id, query, item_index_out);

  if (!require_sharded_views() && result.status() == batt::StatusCode::kUnavailable) {
    BATT_ASSIGN_OK_RESULT(
        llfs::PinnedPage full_leaf_page,
        query.page_loader->get_page_with_layout_in_job(leaf_page_id,
                                                       LeafPageView::page_layout_id(),
                                                       llfs::PinPageToJob::kDefault,
                                                       llfs::OkIfNotFound{false}));

    return find_key_in_pinned_leaf(full_leaf_page, query, item_index_out);

  } else if (result.ok()) {
    KeyQuery::metrics().sharded_view_find_success_count.add(1);
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf(const llfs::PageIdSlot& leaf_page_id_slot,
                                     KeyQuery& query,
                                     usize& item_index_out)
{
  if (query.reject_page(leaf_page_id_slot)) {
    return {batt::StatusCode::kNotFound};
  }

  if (try_full_page_query_first()) {
    KeyQuery::metrics().try_pin_leaf_count.add(1);

    StatusOr<llfs::PinnedPage> full_leaf_page =
        leaf_page_id_slot.try_pin_through(*query.page_loader,
                                          LeafPageView::page_layout_id(),
                                          llfs::PinPageToJob::kDefault);

    if (full_leaf_page.ok()) {
      KeyQuery::metrics().try_pin_leaf_success_count.add(1);
      return find_key_in_pinned_leaf(*full_leaf_page, query, item_index_out);
    }
  }

  KeyQuery::metrics().sharded_view_find_count.add(1);

  StatusOr<ValueView> result =
      find_key_in_leaf_using_sharded_views(leaf_page_id_slot, query, item_index_out);

  if (!require_sharded_views() && result.status() == batt::StatusCode::kUnavailable) {
    BATT_ASSIGN_OK_RESULT(llfs::PinnedPage full_leaf_page,
                          leaf_page_id_slot.load_through(*query.page_loader,
                                                         LeafPageView::page_layout_id(),
                                                         llfs::PinPageToJob::kDefault,
                                                         llfs::OkIfNotFound{false}));

    return find_key_in_pinned_leaf(full_leaf_page, query, item_index_out);

  } else if (result.ok()) {
    KeyQuery::metrics().sharded_view_find_success_count.add(1);
  }

  return result;
}

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool try_full_page_query_first()
{
  static const bool b_ = [] {
    const bool turtlekv_enable_full_page_query =
        getenv_as<bool>("turtlekv_enable_full_page_query").value_or(true);

    LOG(INFO) << BATT_INSPECT(turtlekv_enable_full_page_query);

    return turtlekv_enable_full_page_query;
  }();

  return b_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
[[maybe_unused]] bool require_sharded_views()
{
  static const bool b_ = [] {
    const bool turtlekv_require_sharded_views =
        getenv_as<bool>("turtlekv_require_sharded_views").value_or(false);

    LOG(INFO) << BATT_INSPECT(turtlekv_require_sharded_views);

    return turtlekv_require_sharded_views;
  }();

  return b_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_pinned_leaf(llfs::PinnedPage& pinned_leaf,
                                            KeyQuery& query,
                                            usize& item_index_out)
{
  auto& packed_leaf = PackedLeafPage::view_of(pinned_leaf);

  const PackedKeyValue* found = packed_leaf.find_key(query.key());
  if (!found) {
    return {batt::StatusCode::kNotFound};
  }

  query.page_slice_storage->pinned_pages.emplace_back(std::move(pinned_leaf));

  item_index_out = std::distance(packed_leaf.items_begin(), found);

  VLOG(1) << "Found key " << batt::c_str_literal(query.key()) << BATT_INSPECT(item_index_out)
          << " Reading value";

  return get_value(*found);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> find_key_in_leaf_using_sharded_views(llfs::PageId leaf_page_id,
                                                         KeyQuery& query,
                                                         usize& item_index_out)
{
  const auto default_shard_size = llfs::PageSize{kDefaultLeafShardedViewSize};

  PageSliceReader slice_reader{*query.page_loader, leaf_page_id, default_shard_size};

  const llfs::PageSize head_shard_size = query.tree_options->trie_index_sharded_view_size();

  PageSliceStorage head_storage;
  BATT_ASSIGN_OK_RESULT(
      ConstBuffer head_buffer,
      slice_reader.read_slice(head_shard_size, Interval<usize>{0, head_shard_size}, head_storage));

  const void* page_start = head_buffer.data();
  const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
  const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

  // Sanity check; make sure this is a leaf!
  //
  packed_leaf_page.check_magic();

  // There should be a Trie index, and it should have fit inside the head_buffer; sanity
  // check these assertions.
  //
  const void* trie_begin = packed_leaf_page.trie_index.get();
  const void* trie_end = advance_pointer(trie_begin, packed_leaf_page.trie_index_size);
  BATT_CHECK_LE(byte_distance(page_start, trie_end), head_shard_size);

  // We have the Trie index in memory; query it to find the range containing our query key.
  //
  usize key_prefix_match = 0;
  const Interval<usize> search_range =
      packed_leaf_page.calculate_search_range(query.key(), key_prefix_match);

  // Calculate the leaf page slice containing the range of PackedKeyValue objects we will need
  // to binary search.  We add two to the end of the range; one because we only know how long
  // a key is by comparing its offset to the offset of the next key, and two because we must
  // know the size of the key after that in order to know the value size.
  //
  const PackedKeyValue* head_items = packed_leaf_page.items->data();
  const Interval<usize> items_slice{
      (usize)byte_distance(page_start, head_items + search_range.lower_bound),
      (usize)byte_distance(page_start, head_items + (search_range.upper_bound + 2)),
  };

  // To binary-search the keys, we must pin *both* the portion of the items array we will
  // access (items_slice) *and* the key data pointed to by those items.  Once both are pinned,
  // we calculate a single offset delta to add to the packed offsets inside our KeyOrder
  // comparator.
  //
  PageSliceStorage items_storage;
  BATT_ASSIGN_OK_RESULT(ConstBuffer items_buffer,
                        slice_reader.read_slice(items_slice, items_storage));

  const auto items_begin = (const PackedKeyValue*)items_buffer.data();
  const auto items_end = items_begin + search_range.size();

  // We must include the data of the two keys beyond the nominal search range; since
  // `items_end` is already one past, we must add one to that to get the final upper bound.
  //
  Interval<usize> key_data_slice{
      (usize)(items_slice.lower_bound + items_begin->key_offset),
      (usize)(items_slice.upper_bound + (items_end + 1)->key_offset),
  };

  PageSliceStorage key_data_storage;
  BATT_ASSIGN_OK_RESULT(ConstBuffer key_data_buffer,
                        slice_reader.read_slice(key_data_slice, key_data_storage));

  // `items_begin + items_begin->key_offset` corresponds to the start of the key data buffer;
  // calculate their (signed) difference now that both shards are pinned.
  //
  const isize offset_base = items_begin->key_offset;
  const isize offset_target = byte_distance(items_begin, key_data_buffer.data());
  const isize offset_delta = offset_target - offset_base;

  if (items_begin != items_end) {
    VLOG(1) << "Searching range: "
            << batt::c_str_literal(items_begin->shifted_key_view(offset_delta)) << ".."
            << batt::c_str_literal(std::prev(items_end)->shifted_key_view(offset_delta));
  }

  // Binary search to find the query key.
  //
  const PackedKeyValue* found_item = std::lower_bound(items_begin,  //
                                                      items_end,
                                                      query.key(),
                                                      ShiftedPackedKeyOrder{offset_delta});
  if (found_item == items_end) {
    VLOG(1) << " -- lower bound is items_end; Not Found";
    return {batt::StatusCode::kNotFound};
  }

  VLOG(1) << " -- found_item: " << batt::c_str_literal(found_item->shifted_key_view(offset_delta))
          << " (next=" <<
      [&](std::ostream& out) {
        if (std::next(found_item) == items_end) {
          out << "(end)";
        } else {
          out << batt::c_str_literal(std::next(found_item)->shifted_key_view(offset_delta));
        }
      } << ")";

  // IMPORTANT: we must use `shifted_key_view` and not `key_view` here, or the referenced data
  // will be invalid!
  //
  if (found_item->shifted_key_view(offset_delta) != query.key()) {
    return {batt::StatusCode::kNotFound};
  }

  // Emit the index of the found key within the leaf.
  //
  item_index_out = search_range.lower_bound + std::distance(items_begin, found_item);

  VLOG(1) << "Key matches!" << BATT_INSPECT(item_index_out) << " Reading value";

  // Calculate the location within the page containing the value data.
  //
  Interval<usize> value_data_slice;

  value_data_slice.lower_bound =
      key_data_slice.lower_bound +
      (usize)byte_distance(key_data_buffer.data(), found_item->shifted_value_data(offset_delta));

  value_data_slice.upper_bound =
      value_data_slice.lower_bound + found_item->shifted_value_size(offset_delta);

  // Success!  Pin the shard containing the value, unpack it, and return.
  //
  BATT_ASSIGN_OK_RESULT(ConstBuffer value_data_buffer,
                        slice_reader.read_slice(value_data_slice, *query.page_slice_storage));

  return unpack_value_view(value_data_buffer);
}

}  // namespace

}  // namespace turtle_kv
