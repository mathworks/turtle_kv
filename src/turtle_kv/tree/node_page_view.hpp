#pragma once

#include <turtle_kv/tree/packed_node_page.hpp>

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
#include <ostream>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class NodePageView : public llfs::PageView
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit NodePageView(std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept;

  llfs::PageLayoutId get_page_layout_id() const override
  {
    return NodePageView::page_layout_id();
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
    out << "NodePageView";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PackedNodePage& packed_node_page() const noexcept
  {
    return *this->packed_node_page_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedNodePage* packed_node_page_;
};

}  // namespace turtle_kv
