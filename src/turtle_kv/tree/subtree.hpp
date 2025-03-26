#pragma once

#include <turtle_kv/tree/batch_update.hpp>
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

struct Subtree {
  std::variant<llfs::PageIdSlot, std::unique_ptr<InMemoryLeaf>, std::unique_ptr<InMemoryNode>> impl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Subtree make_empty() noexcept;

  static Subtree from_page_id(const llfs::PageId& page_id) noexcept;

  static Subtree from_pinned_page(const llfs::PinnedPage& pinned_page) noexcept;

  static llfs::PageLayoutId expected_layout_for_height(i32 height) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status apply_batch_update(const TreeOptions& tree_options,  //
                            i32 parent_height,                //
                            BatchUpdate& update,              //
                            const KeyView& key_upper_bound,   //
                            IsRoot is_root) noexcept;

  StatusOr<i32> get_height(llfs::PageLoader& page_loader) const noexcept;

  StatusOr<KeyView> get_min_key(llfs::PageLoader& page_loader,  //
                                llfs::PinnedPage& pinned_page_out) const noexcept;

  StatusOr<KeyView> get_max_key(llfs::PageLoader& page_loader,  //
                                llfs::PinnedPage& pinned_page_out) const noexcept;

  SubtreeViability get_viability() const noexcept;

  StatusOr<ValueView> find_key(llfs::PageLoader& page_loader,      //
                               llfs::PinnedPage& pinned_page_out,  //
                               const KeyView& key) const noexcept;

  std::function<void(std::ostream&)> dump(i32 detail_level = 1) const noexcept;

  Optional<llfs::PageId> get_page_id() const noexcept;

  /** \brief Attempts to split the tree at the top level only; if successful, returns the new
   * right-sibling (i.e. key range _after_ this).
   */
  StatusOr<Subtree> try_split(llfs::PageLoader& page_loader) noexcept;

  llfs::PackedPageId packed_page_id_or_panic() const noexcept;

  bool is_serialized() const noexcept;

  Status start_serialize(TreeSerializeContext& context) noexcept;

  StatusOr<llfs::PinnedPage> finish_serialize(TreeSerializeContext& context) noexcept;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace turtle_kv
