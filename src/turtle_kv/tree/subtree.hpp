#pragma once

#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/key_query.hpp>
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

#include <atomic>
#include <memory>
#include <type_traits>
#include <variant>

namespace turtle_kv {

struct InMemoryLeaf;
struct InMemoryNode;

class TreeScanGenerator;

/** \brief A Turtle Tree.
 */
class Subtree
{
 public:
  friend class TreeScanGenerator;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a new empty tree.
   */
  static Subtree make_empty();

  /** \brief Returns a tree with the given page as root.
   */
  static Subtree from_page_id(const llfs::PageId& page_id);

  /** \brief Returns a tree with the given page (unpacked) as root.
   */
  static Subtree from_packed_page_id(const llfs::PackedPageId& packed_page_id);

  /** \brief Returns a tree with the given pinned page as root.
   */
  static Subtree from_pinned_page(const llfs::PinnedPage& pinned_page);

  /** \brief Returns the LLFS page layout id of the root of a subtree with the given height.
   */
  static llfs::PageLayoutId expected_layout_for_height(i32 height);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Subtree() = default;

  explicit Subtree(const llfs::PageIdSlot& page_id_slot) noexcept;

  explicit Subtree(llfs::PageIdSlot&& page_id_slot) noexcept;

  explicit Subtree(std::unique_ptr<InMemoryLeaf>&& in_memory_leaf) noexcept;

  explicit Subtree(std::unique_ptr<InMemoryNode>&& in_memory_node) noexcept;

  Subtree(Subtree&& other) noexcept;

  Subtree(const Subtree&) = delete;

  Subtree& operator=(Subtree&& other) noexcept;

  Subtree& operator=(const Subtree&) = delete;

  ~Subtree() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Applies the passed BatchUpdate to the subtree in memory; panics if this Subtree is
   * locked against modification.
   */
  Status apply_batch_update(const TreeOptions& tree_options,
                            ParentNodeHeight parent_height,
                            BatchUpdate& update,
                            const KeyView& key_upper_bound,
                            IsRoot is_root);

  /** \brief Returns the current height of the tree.
   */
  StatusOr<i32> get_height(llfs::PageLoader& page_loader) const;

  /** \brief Returns the min-ordered key in the range of this subtree.
   */
  StatusOr<KeyView> get_min_key(llfs::PageLoader& page_loader,
                                llfs::PinnedPage& pinned_page_out) const;

  /** \brief Returns the max-ordered key in the range of this subtree.
   */
  StatusOr<KeyView> get_max_key(llfs::PageLoader& page_loader,
                                llfs::PinnedPage& pinned_page_out) const;

  /** \brief Evaluates whether this Subtree is viable to be serialized in its present state.  This
   * is called during batch update to determine when to merge, split, grow, or shrink nodes of the
   * tree.
   */
  SubtreeViability get_viability() const;

  /** \brief Performs a point query in the tree, returning the value associated with the query key,
   * if found.
   */
  StatusOr<ValueView> find_key(ParentNodeHeight parent_height, KeyQuery& query) const;

  /** \brief Returns a function which prints a human-readable representation of this tree.
   */
  std::function<void(std::ostream&)> dump(i32 detail_level = 1) const;

  /** \brief If this tree is in a serialized state, returns the PageId of the root; otherwise
   * returns None.
   */
  Optional<llfs::PageId> get_page_id() const;

  /** \brief Returns the packed root page id, if this tree is serialized; panics otherwise.
   */
  llfs::PackedPageId packed_page_id_or_panic() const;

  /** \brief Attempts to split the tree at the top level only; if successful, returns the new
   * right-sibling (i.e. key range _after_ this).
   *
   * If no split is necessary, returns None.
   */
  StatusOr<Optional<Subtree>> try_split(BatchUpdateContext& context);

  /** \brief Attempt to make the root viable by flushing a batch.
   */
  Status try_flush(BatchUpdateContext& context);

  /** \brief Returns true iff this Subtree has no in-memory modifications.
   */
  bool is_serialized() const;

  /** \brief If this Subtree is serialized, returns a clone of it; otherwise panic.
   *
   * The returned Subtree is *not* locked by default, even if `this` is.
   */
  Subtree clone_serialized_or_panic() const;

  /** \brief Adds page serialization tasks to the passed `context`.  These will run concurrently
   * once they are all collected.  Afterward, `this->finish_serialize` should be called to finish
   * the serialization process.
   */
  Status start_serialize(TreeSerializeContext& context);

  /** \brief Completes a previously initiated serialization of the Subtree.
   */
  StatusOr<llfs::PageId> finish_serialize(TreeSerializeContext& context);

  /** \brief Locks this Subtree against modification, ensuring that it is safe to access
   * concurrently on multiple threads.  The Subtree must be serialized to be locked; this operation
   * can not be undone.
   */
  void lock();

  /** \brief Returns true iff `this->lock()` has been called.
   */
  bool is_locked() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Status split_and_grow(BatchUpdateContext& context,
                        const TreeOptions& tree_options,
                        const KeyView& key_upper_bound);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::variant<llfs::PageIdSlot, std::unique_ptr<InMemoryLeaf>, std::unique_ptr<InMemoryNode>>
      impl_;

  std::atomic<bool> locked_{false};
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace turtle_kv
