#pragma once

#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/filtered_key_query.hpp>
#include <turtle_kv/tree/subtree_viability.hpp>
#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/strong_types.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/pinned_page.hpp>

#include <memory>
#include <type_traits>
#include <variant>

namespace turtle_kv {

struct InMemoryLeaf;
struct InMemoryNode;

class Subtree
{
 public:
  std::variant<llfs::PageIdSlot, std::unique_ptr<InMemoryLeaf>, std::unique_ptr<InMemoryNode>> impl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Subtree make_empty();

  static Subtree from_page_id(const llfs::PageId& page_id);

  static Subtree from_pinned_page(const llfs::PinnedPage& pinned_page);

  static llfs::PageLayoutId expected_layout_for_height(i32 height);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status apply_batch_update(const TreeOptions& tree_options,
                            ParentNodeHeight parent_height,
                            BatchUpdate& update,
                            const KeyView& key_upper_bound,
                            IsRoot is_root);

  StatusOr<i32> get_height(llfs::PageLoader& page_loader) const;

  StatusOr<KeyView> get_min_key(llfs::PageLoader& page_loader,
                                llfs::PinnedPage& pinned_page_out) const;

  StatusOr<KeyView> get_max_key(llfs::PageLoader& page_loader,
                                llfs::PinnedPage& pinned_page_out) const;

  SubtreeViability get_viability() const;

  StatusOr<ValueView> find_key(ParentNodeHeight parent_height,
                               llfs::PageLoader& page_loader,
                               llfs::PinnedPage& pinned_page_out,
                               const KeyView& key) const;

  StatusOr<ValueView> find_key_filtered(ParentNodeHeight parent_height,
                                        FilteredKeyQuery& query) const;

  std::function<void(std::ostream&)> dump(i32 detail_level = 1) const;

  Optional<llfs::PageId> get_page_id() const;

  llfs::PackedPageId packed_page_id_or_panic() const;

  /** \brief Attempts to split the tree at the top level only; if successful, returns the new
   * right-sibling (i.e. key range _after_ this).
   */
  StatusOr<Subtree> try_split(llfs::PageLoader& page_loader);

  /** \brief Attempt to make the root viable by flushing a batch.
   */
  Status try_flush(batt::WorkerPool& worker_pool,
                   llfs::PageLoader& page_loader,
                   const batt::CancelToken& cancel_token);

  bool is_serialized() const;

  /** \brief If this Subtree is serialized, returns a clone of it; otherwise panic.
   */
  Subtree clone_serialized_or_panic() const;

  Status start_serialize(TreeSerializeContext& context);

  StatusOr<llfs::PinnedPage> finish_serialize(TreeSerializeContext& context);

 private:
  Status split_and_grow(llfs::PageLoader& page_loader,
                        const TreeOptions& tree_options,
                        const KeyView& key_upper_bound);
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace turtle_kv
