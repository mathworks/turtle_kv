#include <turtle_kv/tree/packed_leaf_page.hpp>
//

#include <turtle_kv/tree/leaf_page_view.hpp>

#include <llfs/packed_page_header.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageLayoutId packed_leaf_page_layout_id()
{
  return LeafPageView::page_layout_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> pin_leaf_page_to_job(llfs::PageCacheJob& page_job,
                                                std::shared_ptr<llfs::PageBuffer>&& page_buffer)
{
  BATT_CHECK_OK(LeafPageView::register_layout(page_job.cache()));

  return page_job.pin_new(std::make_shared<LeafPageView>(std::move(page_buffer)),
                          /*callers=*/0);
}

}  // namespace turtle_kv
