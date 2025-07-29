#include <turtle_kv/util/page_slice_reader.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageSliceReader::PageSliceReader(llfs::PageLoader& page_loader,
                                              llfs::PageId page_id,
                                              llfs::PageSize default_shard_size) noexcept
    : page_loader_{page_loader}
    , page_cache_{*this->page_loader_.page_cache()}
    , page_id_{page_id}
    , default_shard_size_{default_shard_size}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> PageSliceReader::read_slice(llfs::PageSize shard_size,
                                                  const Interval<usize>& slice,
                                                  PageSliceStorage& storage_out,
                                                  llfs::PinPageToJob pin_page_to_job,
                                                  llfs::LruPriority lru_priority) const
{
  VLOG(1) << "PageSliceReader::read_slice(shard_size=" << shard_size << ", slice=" << slice << ")";

  BATT_CHECK_GE(slice.size(), 0);

  // Find the alignment unit for our shard size (in bits).
  //
  const i32 shard_size_log2 = batt::log2_ceil(shard_size);

  // Calculate the shard-aligned bounding interval for the requested slice (which is
  // byte-aligned).
  //
  const Interval<usize> shard_aligned_slice{
      batt::round_down_bits(shard_size_log2, slice.lower_bound),
      batt::round_up_bits(shard_size_log2, slice.upper_bound)};

  // How much padding did we need to add to the front of the shard slice?
  //
  const usize offset_in_shard = slice.lower_bound - shard_aligned_slice.lower_bound;

  // There are two cases:
  //   - Base Case: the requested slice fits in a single aligned shard; we can just do a single
  //     cache access and return a sub-range of the cached data
  //   - Recursive Case: the requested slice spans two or more aligned shards; in this case, we
  //     will copy the requested data one shard at a time into a LocalSliceBuffer
  //
  if (shard_aligned_slice.size() == shard_size) {
    BATT_CHECK(this->page_id_);
    Optional<llfs::PageId> shard_page_id =
        this->page_cache_.page_shard_id_for(this->page_id_, shard_aligned_slice);

    // No sharded page view defined for the PageId passed in at construction time.  Fail.
    //
    if (!shard_page_id) {
      return {batt::StatusCode::kUnavailable};
    }

    const void* raw_data = nullptr;

    const llfs::PinnedPage* p_pinned_page = storage_out.find_pinned_page(*shard_page_id);
    if (p_pinned_page) {
      raw_data = p_pinned_page->raw_data();
    } else {
      // Attempt to pin the requested shard in the cache.
      //
      BATT_ASSIGN_OK_RESULT(llfs::PinnedPage newly_pinned_shard,
                            this->page_loader_.load_page(
                                *shard_page_id,
                                llfs::PageLoadOptions{llfs::ShardedPageView::page_layout_id(),
                                                      pin_page_to_job,
                                                      llfs::OkIfNotFound{false},
                                                      lru_priority}));

      raw_data = newly_pinned_shard.raw_data();
      storage_out.insert_pinned_page(std::move(newly_pinned_shard));
    }

    // Success!  Return the requested slice as a ConstBuffer.
    //
    return ConstBuffer{raw_data, offset_in_shard + slice.size()} + offset_in_shard;
  }

  //----- --- -- -  -  -   -
  // Recursive Case.
  //----- --- -- -  -  -   -

  // Size a local buffer to hold the requested data.
  //
  MutableBuffer local_slice_buffer = storage_out.stable_string_store.allocate(slice.size());

  // We need a temporary storage object for the shard loads in the loop below.
  //
  PageSliceStorage shard_tmp_storage;

  // The portion of `local_slice_buffer` that still needs to be filled with  data.
  //
  MutableBuffer dst_buffer{local_slice_buffer.data(), (usize)slice.size()};

  // A subset of `slice`, always guaranteed not to cross the boundary between shards.
  //
  Interval<usize> single_shard_subslice{0, slice.lower_bound};

  // We will iterate pinning one sharded view at a time, copying all data into the local slice
  // buffer.
  //
  while (dst_buffer.size() > 0) {
    // The old upper bound becomes the new lower bound, and the new upper bound is either the end
    // of the next shard or the end of the requested slice, whichever comes first.
    //
    single_shard_subslice.lower_bound = single_shard_subslice.upper_bound;
    single_shard_subslice.upper_bound = std::min<usize>(
        batt::round_down_bits(shard_size_log2, single_shard_subslice.lower_bound) + shard_size,
        slice.upper_bound);

    BATT_CHECK_LT(single_shard_subslice.lower_bound, single_shard_subslice.upper_bound);

    // Pin the next shard.
    //
    BATT_ASSIGN_OK_RESULT(ConstBuffer shard_buffer,
                          this->read_slice(shard_size,
                                           single_shard_subslice,
                                           shard_tmp_storage,
                                           pin_page_to_job,
                                           lru_priority));

    // Copy what we need out of the shard buffer.
    //
    const usize n_to_copy = std::min(dst_buffer.size(), shard_buffer.size());
    std::memcpy(dst_buffer.data(), shard_buffer.data(), n_to_copy);
    dst_buffer += n_to_copy;
  }

  return ConstBuffer{local_slice_buffer.data(), (usize)slice.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ConstBuffer> PageSliceReader::read_slice(const Interval<usize>& slice,
                                                  PageSliceStorage& storage_out,
                                                  llfs::PinPageToJob pin_page_to_job,
                                                  llfs::LruPriority lru_priority) const
{
  return this->read_slice(this->default_shard_size_,
                          slice,
                          storage_out,
                          pin_page_to_job,
                          lru_priority);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const llfs::PinnedPage* PageSliceStorage::find_pinned_page(llfs::PageId page_id) noexcept
{
  for (usize i = this->pinned_pages.size(); i > 0;) {
    --i;
    llfs::PinnedPage& next = this->pinned_pages[i];
    if (next.page_id() == page_id) {
      llfs::PinnedPage& last = this->pinned_pages.back();
      if (&next != &last) {
        std::swap(next, last);
      }
      return &last;
    }
  }
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageSliceStorage::insert_pinned_page(llfs::PinnedPage&& pinned_page) noexcept
{
  this->pinned_pages.emplace_back(std::move(pinned_page));
}

}  // namespace turtle_kv
