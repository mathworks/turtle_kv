#include <turtle_kv/tree/leaf_page_view.hpp>
//

#include <llfs/packed_page_header.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageLayoutId LeafPageView::page_layout_id()
{
  static const llfs::PageLayoutId id = llfs::PageLayoutId::from_str("kv_leaf_");
  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageReader LeafPageView::page_reader()
{
  return [](std::shared_ptr<const llfs::PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const llfs::PageView>> {
    return {std::make_shared<LeafPageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Status LeafPageView::register_layout(llfs::PageCache& cache)
{
  LOG_FIRST_N(INFO, 1) << "Registering page layout: " << LeafPageView::page_layout_id();
  return cache.register_page_reader(LeafPageView::page_layout_id(),
                                    __FILE__,
                                    __LINE__,
                                    LeafPageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ bool LeafPageView::layout_used_by_page(const llfs::PinnedPage& pinned_page)
{
  return pinned_page && (llfs::get_page_header(pinned_page.page_buffer()).layout_id ==
                         LeafPageView::page_layout_id());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ LeafPageView::LeafPageView(
    std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept
    : PageView{std::move(page_buffer)}
    , packed_leaf_page_{std::addressof(PackedLeafPage::view_of(*this))}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<llfs::PageId> LeafPageView::trace_refs() const /*override*/
{
  return batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> LeafPageView::min_key() const /*override*/
{
  if (this->packed_leaf_page_->items->empty()) {
    return None;
  }
  return this->packed_leaf_page_->min_key();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> LeafPageView::max_key() const /*override*/
{
  if (this->packed_leaf_page_->items->empty()) {
    return None;
  }
  return this->packed_leaf_page_->max_key();
}

}  // namespace turtle_kv
