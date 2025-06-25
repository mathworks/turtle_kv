#pragma once

#include <turtle_kv/tree/in_memory_leaf.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/core/merge_scanner.hpp>

#include <llfs/page_loader.hpp>

#include <memory>

namespace turtle_kv {

class TreeScanGenerator
{
 public:
  explicit TreeScanGenerator(llfs::PageLoader& page_loader,
                             MergeScanner::GeneratorContext& context,
                             const Subtree* root,
                             const KeyView& min_key) noexcept;

  batt::Status operator()();

  void visit_subtree(const Subtree& root);

  void visit_page(const llfs::PageIdSlot& page_id_slot);

  void visit_packed_leaf(const PackedLeafPage& leaf_page);

  void visit_packed_node(const PackedNodePage& node_page);

  void visit_in_memory_leaf(InMemoryLeaf& leaf);

  void visit_in_memory_node(InMemoryNode& node);

  template <typename NodeT>
  void visit_child_subtrees(NodeT& node, usize min_key_pivot);

 private:
  Ref<llfs::PageLoader> page_loader_;
  MergeScanner::GeneratorContext context_;
  const Subtree* root_;
  batt::Status status_;
  KeyView min_key_;
};

class TreeScanner
{
 public:
  explicit TreeScanner(llfs::PageLoader& page_loader,
                       std::shared_ptr<const Subtree>&& root) noexcept;

  TreeScanner(const TreeScanner&) = delete;
  TreeScanner& operator=(const TreeScanner&) = delete;

  batt::StatusOr<usize> scan_items(const Slice<std::pair<KeyView, ValueView>>& items_out);

  void seek_to(const KeyView& key);

  void rewind();

  void prepare();

  Optional<EditView> peek()
  {
    return this->merge_scanner_->peek();
  }

  Optional<EditView> next()
  {
    return this->merge_scanner_->next();
  }

 private:
  llfs::PageLoader& page_loader_;
  std::shared_ptr<const Subtree> root_;
  KeyView min_key_;
  Optional<MergeScanner> merge_scanner_;
};

}  // namespace turtle_kv
