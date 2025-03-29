#pragma once

#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_buffer.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_filter.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <memory>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class LeafPageView : public llfs::PageView
{
 public:
  /** \brief The page layout id for all instances of this class.
   */
  static llfs::PageLayoutId page_layout_id();

  /** \brief Returns the PageReader for this layout.
   */
  static llfs::PageReader page_reader();

  /** \brief Registers this page layout with the passed cache, so that pages using the layout can be
   * correctly loaded and parsed by the PageCache.
   */
  static Status register_layout(llfs::PageCache& cache);

  /** \brief Returns true iff the passed page is valid and its header specifies layout
   * LeafPageView::page_layout_id().
   */
  static bool layout_used_by_page(const llfs::PinnedPage& pinned_page);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit LeafPageView(std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept;

  llfs::PageLayoutId get_page_layout_id() const override
  {
    return LeafPageView::page_layout_id();
  }

  BoxedSeq<llfs::PageId> trace_refs() const override;

  Optional<KeyView> min_key() const override;

  Optional<KeyView> max_key() const override;

  std::shared_ptr<llfs::PageFilter> build_filter() const override
  {
    return std::make_shared<llfs::NullPageFilter>(this->page_id());
  }

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "LeafPageView";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedLeafPage& packed_leaf_page() const
  {
    return *this->packed_leaf_page_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedLeafPage* packed_leaf_page_;
};

}  // namespace turtle_kv
