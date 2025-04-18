#pragma once

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>

#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id_slot.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/sharded_page_view.hpp>

#include <batteries/case_of.hpp>
#include <batteries/utility.hpp>

#include <type_traits>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn /*= StatusOr<R> (const PackedLeafPage& | const PackedNodePage&) */,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedLeafPage&>>>>
StatusOr<R> visit_tree_page(llfs::PageLoader& page_loader,
                            llfs::PinnedPage& pinned_page_out,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  if (!pinned_page_out || pinned_page_out.page_id() != page_id_slot.page_id) {
    BATT_ASSIGN_OK_RESULT(pinned_page_out,
                          page_id_slot.load_through(page_loader,
                                                    /*required_layout=*/None,
                                                    llfs::PinPageToJob::kDefault,
                                                    llfs::OkIfNotFound{false}));
  }
  const auto& page_header =
      *static_cast<const llfs::PackedPageHeader*>(pinned_page_out.const_buffer().data());

  if (page_header.layout_id == LeafPageView::page_layout_id()) {
    return BATT_FORWARD(visitor_fn)(PackedLeafPage::view_of(pinned_page_out));

  } else if (page_header.layout_id == NodePageView::page_layout_id()) {
    return BATT_FORWARD(visitor_fn)(PackedNodePage::view_of(pinned_page_out));

  } else {
    return {batt::StatusCode::kInvalidArgument};
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedLeafPage&>>>>
StatusOr<R> visit_tree_page(llfs::PageLoader& page_loader,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  llfs::PinnedPage pinned_page;

  return visit_tree_page(page_loader, pinned_page, page_id_slot, BATT_FORWARD(visitor_fn));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... CaseFns>
decltype(auto) visit_tree_page(llfs::PageLoader& page_loader,
                               llfs::PinnedPage& pinned_page_out,
                               const llfs::PageIdSlot& page_id_slot,
                               CaseFns&&... case_fns)
{
  return visit_tree_page(page_loader,
                         pinned_page_out,
                         page_id_slot,
                         batt::make_case_of_visitor(BATT_FORWARD(case_fns)...));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... CaseFns>
decltype(auto) visit_tree_page(llfs::PageLoader& page_loader,
                               const llfs::PageIdSlot& page_id_slot,
                               CaseFns&&... case_fns)
{
  return visit_tree_page(page_loader,
                         page_id_slot,
                         batt::make_case_of_visitor(BATT_FORWARD(case_fns)...));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedLeafPage&>>>>
StatusOr<R> visit_leaf_page(llfs::PageLoader& page_loader,
                            llfs::PinnedPage& pinned_page_out,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  if (!pinned_page_out || pinned_page_out.page_id() != page_id_slot.page_id) {
    BATT_ASSIGN_OK_RESULT(pinned_page_out,
                          page_id_slot.load_through(page_loader,
                                                    LeafPageView::page_layout_id(),
                                                    llfs::PinPageToJob::kDefault,
                                                    llfs::OkIfNotFound{false}));
  }
  return BATT_FORWARD(visitor_fn)(PackedLeafPage::view_of(pinned_page_out));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedLeafPage&>>>>
StatusOr<R> visit_leaf_page(llfs::PageLoader& page_loader,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  llfs::PinnedPage pinned_page;

  return visit_leaf_page(page_loader, pinned_page, page_id_slot, BATT_FORWARD(visitor_fn));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedNodePage&>>>>
StatusOr<R> visit_node_page(llfs::PageLoader& page_loader,
                            llfs::PinnedPage& pinned_page_out,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  if (!pinned_page_out || pinned_page_out.page_id() != page_id_slot.page_id) {
    BATT_ASSIGN_OK_RESULT(pinned_page_out,
                          page_id_slot.load_through(page_loader,
                                                    NodePageView::page_layout_id(),
                                                    llfs::PinPageToJob::kDefault,
                                                    llfs::OkIfNotFound{false}));
  }
  return BATT_FORWARD(visitor_fn)(PackedNodePage::view_of(pinned_page_out));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const PackedNodePage&>>>>
StatusOr<R> visit_node_page(llfs::PageLoader& page_loader,
                            const llfs::PageIdSlot& page_id_slot,
                            VisitorFn&& visitor_fn)
{
  llfs::PinnedPage pinned_page;

  return visit_node_page(page_loader, pinned_page, page_id_slot, BATT_FORWARD(visitor_fn));
}

namespace detail {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CopyBufferSlice {
  MutableBuffer* dst;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize operator()(const ConstBuffer& src) const
  {
    usize n_to_copy = std::min(this->dst->size(), src.size());
    std::memcpy(this->dst->data(), src.data(), n_to_copy);
    *dst += n_to_copy;
    return n_to_copy;
  }
};

}  // namespace detail

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <
    typename VisitorFn,
    typename R = StatusOr<RemoveStatusOr<std::invoke_result_t<VisitorFn, const ConstBuffer&>>>>
StatusOr<R> visit_page_slice(llfs::PageCache& page_cache,
                             llfs::PageLoader& page_loader,
                             const llfs::PageIdSlot& page_id_slot,
                             llfs::PageSize shard_size,
                             const Interval<usize>& slice,
                             VisitorFn&& visitor_fn)
{
  const i32 shard_size_log2 = batt::log2_ceil(shard_size);

  const Interval<usize> shard_aligned_slice{
      batt::round_down_bits(shard_size_log2, slice.lower_bound),
      batt::round_up_bits(shard_size_log2, slice.upper_bound)};

  const usize offset_in_shard = slice.lower_bound - shard_aligned_slice.lower_bound;

  if (shard_aligned_slice.size() == shard_size) {
    llfs::PageId shard_page_id = page_loader.page_cache()
                                     ->page_shard_id_for(page_id_slot, shard_aligned_slice)
                                     .value_or_panic();

    llfs::PinnedPage pinned_shard = BATT_OK_RESULT_OR_PANIC(
        page_loader.get_page_with_layout_in_job(shard_page_id,
                                                llfs::ShardedPageView::page_layout_id(),
                                                llfs::PinPageToJob::kDefault,
                                                llfs::OkIfNotFound{false}));

    ConstBuffer slice_buffer{pinned_shard.raw_data(), offset_in_shard + slice.size()};
    slice_buffer += offset_in_shard;

    return BATT_FORWARD(visitor_fn)(slice_buffer);
  }

  SmallVec<u8, 4096> storage(slice.size());
  MutableBuffer buffer{storage.data(), storage.size()};
  Interval<usize> subslice{0, slice.lower_bound};

  while (buffer.size() > 0) {
    subslice.lower_bound = subslice.upper_bound;
    subslice.upper_bound =
        std::min(batt::round_down_bits(shard_size_log2, subslice.lower_bound) + shard_size,
                 slice.upper_bound);

    BATT_REQUIRE_OK(visit_page_slice(page_cache,
                                     page_loader,
                                     page_id_slot,
                                     shard_size,
                                     subslice,
                                     detail::CopyBufferSlice{&buffer}));
  }

  return BATT_FORWARD(visitor_fn)(ConstBuffer{storage.data(), storage.size()});
}

using LocalSliceBuffer = SmallVec<std::aligned_storage_t<64, 64>, 64>;
using PageSliceStorage = std::variant<NoneType, llfs::PinnedPage, LocalSliceBuffer>;

class PageSliceReader
{
 public:
  explicit PageSliceReader(llfs::PageLoader& page_loader,
                           llfs::PageId page_id,
                           llfs::PageSize default_shard_size) noexcept
      : page_loader_{page_loader}
      , page_cache_{*this->page_loader_.page_cache()}
      , page_id_{page_id}
      , default_shard_size_{default_shard_size}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<ConstBuffer> read_slice(
      llfs::PageSize shard_size,
      const Interval<usize>& slice,
      PageSliceStorage& storage_out,
      llfs::PinPageToJob pin_page_to_job = llfs::PinPageToJob::kDefault) const
  {
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
      Optional<llfs::PageId> shard_page_id =
          this->page_cache_.page_shard_id_for(this->page_id_, shard_aligned_slice);

      // No sharded page view defined for the PageId passed in at construction time.  Fail.
      //
      if (!shard_page_id) {
        return {batt::StatusCode::kUnavailable};
      }

      // Attempt to pin the requested shard in the cache.
      //
      llfs::PinnedPage& pinned_shard = storage_out.emplace<llfs::PinnedPage>();
      BATT_ASSIGN_OK_RESULT(
          pinned_shard,
          this->page_loader_.get_page_with_layout_in_job(*shard_page_id,
                                                         llfs::ShardedPageView::page_layout_id(),
                                                         pin_page_to_job,
                                                         llfs::OkIfNotFound{false}));

      // Success!  Return the requested slice as a ConstBuffer.
      //
      return ConstBuffer{pinned_shard.raw_data(), offset_in_shard + slice.size()} + offset_in_shard;
    }

    //----- --- -- -  -  -   -
    // Recursive Case.
    //----- --- -- -  -  -   -

    // Size a LocalSliceBuffer to hold the requested data.
    //
    const usize cache_lines = (slice.size() + 63) / 64;
    LocalSliceBuffer& local_slice_buffer = storage_out.emplace<LocalSliceBuffer>(cache_lines);

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
      BATT_ASSIGN_OK_RESULT(
          ConstBuffer shard_buffer,
          this->read_slice(shard_size, single_shard_subslice, shard_tmp_storage, pin_page_to_job));

      // Copy what we need out of the shard buffer.
      //
      const usize n_to_copy = std::min(dst_buffer.size(), shard_buffer.size());
      std::memcpy(dst_buffer.data(), shard_buffer.data(), n_to_copy);
      dst_buffer += n_to_copy;
    }

    return ConstBuffer{local_slice_buffer.data(), (usize)slice.size()};
  }

  StatusOr<ConstBuffer> read_slice(
      const Interval<usize>& slice,
      PageSliceStorage& storage_out,
      llfs::PinPageToJob pin_page_to_job = llfs::PinPageToJob::kDefault) const
  {
    return this->read_slice(this->default_shard_size_, slice, storage_out, pin_page_to_job);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& page_loader_;
  llfs::PageCache& page_cache_;
  llfs::PageId page_id_;
  llfs::PageSize default_shard_size_;
};

}  // namespace turtle_kv
