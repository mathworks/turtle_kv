#pragma once

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>

#include <turtle_kv/import/status.hpp>

#include <llfs/page_id_slot.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>

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
                            VisitorFn&& visitor_fn) noexcept
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
                            VisitorFn&& visitor_fn) noexcept
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
                               CaseFns&&... case_fns) noexcept
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
                               CaseFns&&... case_fns) noexcept
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
                            VisitorFn&& visitor_fn) noexcept
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
                            VisitorFn&& visitor_fn) noexcept
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
                            VisitorFn&& visitor_fn) noexcept
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
                            VisitorFn&& visitor_fn) noexcept
{
  llfs::PinnedPage pinned_page;

  return visit_node_page(page_loader, pinned_page, page_id_slot, BATT_FORWARD(visitor_fn));
}

}  // namespace turtle_kv
