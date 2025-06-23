#include <turtle_kv/tree/tree_scanner.hpp>
//
#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/import/small_vec.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TreeScanGenerator::TreeScanGenerator(llfs::PageLoader& page_loader,
                                     MergeScanner::GeneratorContext& context,
                                     const Subtree* root,
                                     const KeyView& min_key) noexcept
    : page_loader_{page_loader}
    , context_{context}
    , root_{root}
    , min_key_{min_key}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status TreeScanGenerator::operator()()
{
  this->visit_subtree(*(this->root_));
  return this->status_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_subtree(const Subtree& root)
{
  batt::case_of(
      root.impl,
      [&](const llfs::PageIdSlot& page_id_slot) {
        this->visit_page(page_id_slot);
      },
      [&](const std::unique_ptr<InMemoryLeaf>& leaf) {
        this->visit_in_memory_leaf(*leaf);
      },
      [&](const std::unique_ptr<InMemoryNode>& node) {
        this->visit_in_memory_node(*node);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_page(const llfs::PageIdSlot& page_id_slot)
{
  StatusOr<llfs::PinnedPage> pinned_page =
      page_id_slot.load_through(this->page_loader_.get(),
                                llfs::PageLoadOptions{
                                    llfs::PinPageToJob::kFalse,
                                    llfs::OkIfNotFound{false},
                                    llfs::LruPriority{kNodeLruPriority},
                                });
  BATT_CHECK(pinned_page.ok());

  const auto& page_header =
      *static_cast<const llfs::PackedPageHeader*>(pinned_page->const_buffer().data());

  if (page_header.layout_id == LeafPageView::page_layout_id()) {
    this->visit_packed_leaf(PackedLeafPage::view_of(*pinned_page));

  } else if (page_header.layout_id == NodePageView::page_layout_id()) {
    this->visit_packed_node(PackedNodePage::view_of(*pinned_page));

  } else {
    BATT_PANIC() << "Reached a page that is not a leaf or node page.";
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_packed_leaf(const PackedLeafPage& leaf_page)
{
  MergeFrame frame;

  Slice<const PackedKeyValue> edit_slice =
      as_slice(leaf_page.lower_bound(this->min_key_), leaf_page.items_end());

  frame.push_line(seq::single_item(EditSlice{edit_slice}) | seq::boxed());

  this->context_.push_frame(&frame);
  this->status_.Update(this->context_.await_frame_consumed(&frame));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_packed_node(const PackedNodePage& node)
{
  using PackedLevel = PackedNodePage::UpdateBuffer::SegmentedLevel;

  using PackedLevelScanner =
      SegmentedLevelScanner<const PackedNodePage, const PackedLevel, llfs::PageLoader>;

  //----- --- -- -  -  -   -

  SmallVec<PackedLevel, 64> levels;
  MergeFrame frame;

  {
    const usize level_count = node.get_level_count();
    for (usize level_i = 0; level_i < level_count; ++level_i) {
      if (node.is_size_tiered()) {
        levels.emplace_back(node.get_tier(level_i));
      } else {
        levels.emplace_back(node.get_level(level_i));
      }
    }
  }

  Status segment_load_status;
  const usize min_key_pivot = in_node(node).find_pivot_containing(this->min_key_);

  for (const PackedLevel& level : levels) {
    frame.push_line(PackedLevelScanner{node,
                                       level,
                                       this->page_loader_,
                                       llfs::PinPageToJob::kFalse,
                                       segment_load_status,
                                       (i32)min_key_pivot,
                                       this->min_key_} |
                    seq::boxed());
  }

  this->context_.push_frame(&frame);

  //----- --- -- -  -  -   -

  this->visit_child_subtrees(node, min_key_pivot);

  //----- --- -- -  -  -   -

  this->status_.Update(this->context_.await_frame_consumed(&frame));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_in_memory_leaf(InMemoryLeaf& leaf)
{
  MergeFrame frame;

  frame.push_line(leaf.result_set.live_edit_slices(this->min_key_));

  this->context_.push_frame(&frame);
  this->status_.Update(this->context_.await_frame_consumed(&frame));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanGenerator::visit_in_memory_node(InMemoryNode& node)
{
  Status segment_load_status;
  HasPageRefs has_page_refs{false};
  const usize min_key_pivot = in_node(node).find_pivot_containing(this->min_key_);

  //----- --- -- -  -  -   -

  MergeFrame frame;

  node.push_levels_to_merge(frame,
                            this->page_loader_,
                            segment_load_status,
                            has_page_refs,
                            as_slice(node.update_buffer.levels),
                            min_key_pivot,
                            /*only_pivot=*/false,
                            this->min_key_);

  this->context_.push_frame(&frame);

  //----- --- -- -  -  -   -

  this->visit_child_subtrees(node, min_key_pivot);

  //----- --- -- -  -  -   -

  this->status_.Update(this->context_.await_frame_consumed(&frame));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT>
void TreeScanGenerator::visit_child_subtrees(NodeT& node, usize min_key_pivot)
{
  const usize n_pivots = node.pivot_count();

  for (usize pivot_i = min_key_pivot; pivot_i < n_pivots; ++pivot_i) {
    this->visit_subtree(node.get_child(pivot_i));
    if (!this->status_.ok()) {
      break;
    }
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class TreeScanner

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TreeScanner::TreeScanner(llfs::PageLoader& page_loader,
                         std::shared_ptr<const Subtree>&& root) noexcept
    : page_loader_{page_loader}
    , root_{std::move(root)}
    , min_key_{global_min_key()}
    , merge_scanner_{None}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<usize> TreeScanner::scan_items(const Slice<std::pair<KeyView, ValueView>>& items_out)
{
  this->prepare();

  usize n = 0;

  for (; n < items_out.size(); ++n) {
    Optional<EditView> next = this->merge_scanner_->next();
    if (!next) {
      break;
    }
    auto& item_out = items_out[n];
    item_out.first = next->key;
    item_out.second = next->value;
  }

  return n;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanner::seek_to(const KeyView& key)
{
  this->min_key_ = key;
  this->rewind();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanner::rewind()
{
  this->merge_scanner_ = None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeScanner::prepare()
{
  if (!this->merge_scanner_) {
    this->merge_scanner_.emplace();
    this->merge_scanner_->set_generator([this](MergeScanner::GeneratorContext& context) {
      Status status;
      {
        TreeScanGenerator scan_tree{this->page_loader_, context, this->root_.get(), this->min_key_};
        status = scan_tree();
      }
      return status;
    });
  }
}

}  // namespace turtle_kv
