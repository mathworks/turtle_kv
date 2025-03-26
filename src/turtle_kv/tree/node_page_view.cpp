#include <turtle_kv/tree/node_page_view.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageLayoutId NodePageView::page_layout_id()
{
  static const llfs::PageLayoutId id = llfs::PageLayoutId::from_str("kv_node_");
  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageReader NodePageView::page_reader()
{
  return [](std::shared_ptr<const llfs::PageBuffer> page_buffer)
             -> StatusOr<std::shared_ptr<const llfs::PageView>> {
    return {std::make_shared<NodePageView>(std::move(page_buffer))};
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ batt::Status NodePageView::register_layout(llfs::PageCache& cache)
{
  LOG_FIRST_N(INFO, 1) << "Registering page layout: " << NodePageView::page_layout_id();
  return cache.register_page_reader(NodePageView::page_layout_id(),
                                    __FILE__,
                                    __LINE__,
                                    NodePageView::page_reader());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ NodePageView::NodePageView(
    std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept
    : PageView{std::move(page_buffer)}
    , packed_node_page_{std::addressof(PackedNodePage::view_of(*this))}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<llfs::PageId> NodePageView::trace_refs() const /*override*/
{
  using Segment = PackedNodePage::UpdateBuffer::Segment;

  return as_seq(this->packed_node_page_->children_slice())                           //
         | seq::chain(                                                               //
               as_seq(this->packed_node_page_->update_buffer.segments_slice())       //
               | seq::map([](const Segment& segment) -> const llfs::PackedPageId& {  //
                   return segment.leaf_page_id;                                      //
                 }))                                                                 //
         | seq::map([](const llfs::PackedPageId& packed_page_id) -> llfs::PageId {   //
             return packed_page_id.unpack();                                         //
           })                                                                        //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> NodePageView::min_key() const /*override*/
{
  return this->packed_node_page_->min_key();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> NodePageView::max_key() const /*override*/
{
  return this->packed_node_page_->max_key();
}

}  // namespace turtle_kv
