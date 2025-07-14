#include <turtle_kv/tree/subtree.hpp>
//

#include <turtle_kv/tree/in_memory_leaf.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/the_key.hpp>
#include <turtle_kv/tree/visit_tree_page.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>

#include <llfs/sharded_page_view.hpp>

#include <batteries/bool_status.hpp>
#include <batteries/stream_util.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Subtree Subtree::make_empty()
{
  return Subtree::from_page_id(llfs::PageId{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Subtree Subtree::from_page_id(const llfs::PageId& page_id)
{
  return Subtree{llfs::PageIdSlot::from_page_id(page_id)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Subtree Subtree::from_packed_page_id(const llfs::PackedPageId& packed_page_id)
{
  return Subtree::from_page_id(packed_page_id.unpack());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Subtree Subtree::from_pinned_page(const llfs::PinnedPage& pinned_page)
{
  return Subtree::from_page_id(pinned_page.page_id());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ llfs::PageLayoutId Subtree::expected_layout_for_height(i32 height)
{
  if (height == 1) {
    return LeafPageView::page_layout_id();
  }
  return NodePageView::page_layout_id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Subtree::Subtree(const llfs::PageIdSlot& page_id_slot) noexcept : impl_{page_id_slot}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Subtree::Subtree(llfs::PageIdSlot&& page_id_slot) noexcept
    : impl_{std::move(page_id_slot)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Subtree::Subtree(std::unique_ptr<InMemoryLeaf>&& in_memory_leaf) noexcept
    : impl_{std::move(in_memory_leaf)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ Subtree::Subtree(std::unique_ptr<InMemoryNode>&& in_memory_node) noexcept
    : impl_{std::move(in_memory_node)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree::Subtree(Subtree&& other) noexcept
    : impl_{std::move(other.impl_)}
    , locked_{other.locked_.load()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree::~Subtree() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree& Subtree::operator=(Subtree&& other) noexcept
{
  if (this != &other) {
    this->impl_ = std::move(other.impl_);
    this->locked_.store(other.locked_.load());
  }
  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::apply_batch_update(const TreeOptions& tree_options,
                                   ParentNodeHeight parent_height,
                                   BatchUpdate& update,
                                   const KeyView& key_upper_bound,
                                   IsRoot is_root)
{
  BATT_CHECK_GT(parent_height, 0);
  BATT_CHECK(!this->locked_.load());

  Subtree& subtree = *this;

  StatusOr<Subtree> new_subtree = batt::case_of(  //
      subtree.impl_,

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](llfs::PageIdSlot& page_id_slot) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: merge {update} + {empty} => InMemoryLeaf

        // Check for empty tree (page id with special invalid value ~u64{0})
        //
        if (!page_id_slot.is_valid()) {
          BATT_CHECK_EQ(parent_height, 1);

          auto new_leaf = std::make_unique<InMemoryLeaf>(llfs::PinnedPage{}, tree_options);

          new_leaf->result_set = update.result_set;

          if (!update.edit_size_totals) {
            update.update_edit_size_totals();
          }

          new_leaf->set_edit_size_totals(std::move(*update.edit_size_totals));
          update.edit_size_totals = None;

          return Subtree{std::move(new_leaf)};
        }
        BATT_CHECK_GT(parent_height, 1);

        llfs::PageLayoutId expected_layout = Subtree::expected_layout_for_height(parent_height - 1);

        StatusOr<llfs::PinnedPage> status_or_pinned_page = page_id_slot.load_through(
            update.context.page_loader,
            llfs::PageLoadOptions{
                expected_layout,
                llfs::PinPageToJob::kDefault,
                llfs::OkIfNotFound{false},
                llfs::LruPriority{(parent_height > 2) ? kNodeLruPriority : kLeafLruPriority},
            });

        BATT_REQUIRE_OK(status_or_pinned_page) << BATT_INSPECT(parent_height);

        llfs::PinnedPage& pinned_page = *status_or_pinned_page;

        if (parent_height == 2) {
          //+++++++++++-+-+--+----- --- -- -  -  -   -
          // Case: {BatchUpdate} + {PackedLeafPage} => InMemoryLeaf

          const PackedLeafPage& packed_leaf = PackedLeafPage::view_of(pinned_page);
          auto new_leaf =
              std::make_unique<InMemoryLeaf>(batt::make_copy(pinned_page), tree_options);

          BATT_ASSIGN_OK_RESULT(  //
              new_leaf->result_set,
              update.context.merge_compact_edits(  //
                  global_max_key(),
                  [&](MergeCompactor& compactor) -> Status {
                    compactor.push_level(update.result_set.live_edit_slices());
                    compactor.push_level(packed_leaf.as_edit_slice_seq());
                    return OkStatus();
                  }));

          new_leaf->set_edit_size_totals(
              update.context.compute_running_total(new_leaf->result_set));

          return Subtree{std::move(new_leaf)};

        } else {
          //+++++++++++-+-+--+----- --- -- -  -  -   -
          // Case: {BatchUpdate} + {PackedNodePage} => InMemoryNode

          const PackedNodePage& packed_node = PackedNodePage::view_of(pinned_page);

          BATT_ASSIGN_OK_RESULT(
              std::unique_ptr<InMemoryNode> node,
              InMemoryNode::unpack(batt::make_copy(pinned_page), tree_options, packed_node));

          BATT_REQUIRE_OK(node->apply_batch_update(update, key_upper_bound, is_root));

          return Subtree{std::move(node)};
        }
      },

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](std::unique_ptr<InMemoryLeaf>& in_memory_leaf) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: {BatchUpdate} + {InMemoryLeaf} => InMemoryLeaf

        BATT_CHECK_EQ(parent_height, 2);

        BATT_ASSIGN_OK_RESULT(
            in_memory_leaf->result_set,
            update.context.merge_compact_edits(
                global_max_key(),
                [&](MergeCompactor& compactor) -> Status {
                  compactor.push_level(update.result_set.live_edit_slices());
                  compactor.push_level(in_memory_leaf->result_set.live_edit_slices());
                  return OkStatus();
                }));

        in_memory_leaf->result_set.update_has_page_refs(update.result_set.has_page_refs());
        in_memory_leaf->set_edit_size_totals(
            update.context.compute_running_total(in_memory_leaf->result_set));

        return Subtree{std::move(in_memory_leaf)};
      },

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](std::unique_ptr<InMemoryNode>& in_memory_node) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: {BatchUpdate} + {InMemoryNode} => InMemoryNode

        BATT_CHECK_GT(parent_height, 2);

        BATT_REQUIRE_OK(in_memory_node->apply_batch_update(update, key_upper_bound, is_root));

        return Subtree{std::move(in_memory_node)};
      });

  BATT_REQUIRE_OK(new_subtree);

  // If this is the root level and tree needs to grow/shrink in height, do so now.
  //
  if (is_root) {
    BATT_REQUIRE_OK(batt::case_of(
        new_subtree->get_viability(),
        [](const Viable&) -> Status {
          // Nothing to fix; tree is viable!
          return OkStatus();
        },
        [&](NeedsSplit needs_split) {
          Status status =
              new_subtree->split_and_grow(update.context, tree_options, key_upper_bound);

          if (!status.ok()) {
            LOG(INFO) << "split_and_grow failed;" << BATT_INSPECT(needs_split);
          }
          return status;
        },
        [&](const NeedsMerge& needs_merge) {
          BATT_CHECK(!needs_merge.single_pivot)
              << "TODO [tastolfi 2025-03-26] implement flush and shrink";
          return OkStatus();
        }));
  }

  subtree = std::move(*new_subtree);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::split_and_grow(BatchUpdateContext& context,
                               const TreeOptions& tree_options,
                               const KeyView& key_upper_bound)
{
  BATT_CHECK(!this->locked_.load());

  StatusOr<Optional<Subtree>> upper_half_subtree = this->try_split(context);
  if (upper_half_subtree.ok() && !*upper_half_subtree) {
    return OkStatus();
  }
  BATT_REQUIRE_OK(upper_half_subtree);

  Subtree* lower_half_subtree = this;

  BATT_ASSIGN_OK_RESULT(  //
      std::unique_ptr<InMemoryNode> new_root,
      InMemoryNode::from_subtrees(context.page_loader,
                                  tree_options,
                                  std::move(*lower_half_subtree),
                                  std::move(**upper_half_subtree),
                                  key_upper_bound,
                                  IsRoot{true}));

  this->impl_ = std::move(new_root);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> Subtree::get_height(llfs::PageLoader& page_loader) const
{
  return batt::case_of(
      this->impl_,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<i32> {
        if (!page_id_slot.page_id) {
          return 0;
        }
        return visit_tree_page(  //
            page_loader,
            page_id_slot,

            [](const PackedLeafPage&) -> StatusOr<i32> {
              return 1;
            },
            [](const PackedNodePage& packed_node) -> StatusOr<i32> {
              return (i32)packed_node.height;
            });
      },
      [](const std::unique_ptr<InMemoryLeaf>& in_memory_leaf) -> StatusOr<i32> {
        return 1;
      },
      [](const std::unique_ptr<InMemoryNode>& in_memory_node) -> StatusOr<i32> {
        return in_memory_node->height;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<KeyView> Subtree::get_min_key(llfs::PageLoader& page_loader,
                                       llfs::PinnedPage& pinned_page_out) const
{
  return batt::case_of(
      this->impl_,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<KeyView> {
        return visit_tree_page(  //
            page_loader,
            pinned_page_out,
            page_id_slot,

            [](const PackedLeafPage& packed_leaf) -> StatusOr<KeyView> {
              return packed_leaf.min_key();
            },

            [](const PackedNodePage& packed_node) -> StatusOr<KeyView> {
              return packed_node.min_key();
            });
      },
      [&](const std::unique_ptr<InMemoryLeaf>& in_memory_leaf) -> StatusOr<KeyView> {
        return in_memory_leaf->get_min_key();
      },
      [&](const std::unique_ptr<InMemoryNode>& in_memory_node) -> StatusOr<KeyView> {
        return in_memory_node->get_min_key();
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<KeyView> Subtree::get_max_key(llfs::PageLoader& page_loader,
                                       llfs::PinnedPage& pinned_page_out) const
{
  return batt::case_of(
      this->impl_,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<KeyView> {
        return visit_tree_page(  //
            page_loader,
            pinned_page_out,
            page_id_slot,

            [](const PackedLeafPage& packed_leaf) -> StatusOr<KeyView> {
              return packed_leaf.max_key();
            },

            [](const PackedNodePage& packed_node) -> StatusOr<KeyView> {
              return packed_node.max_key();
            });
      },
      [&](const std::unique_ptr<InMemoryLeaf>& in_memory_leaf) -> StatusOr<KeyView> {
        return in_memory_leaf->get_max_key();
      },
      [&](const std::unique_ptr<InMemoryNode>& in_memory_node) -> StatusOr<KeyView> {
        return in_memory_node->get_max_key();
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SubtreeViability Subtree::get_viability() const
{
  return batt::case_of(
      this->impl_,
      [&](const llfs::PageIdSlot& page_id_slot [[maybe_unused]]) -> SubtreeViability {
        return Viable{};
      },
      [&](const auto& in_memory) -> SubtreeViability {
        return in_memory->get_viability();
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> Subtree::find_key(ParentNodeHeight parent_height, KeyQuery& query) const
{
  if (parent_height < 2) {
    return {batt::StatusCode::kNotFound};
  }

  return batt::case_of(
      this->impl_,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<ValueView> {
        if (parent_height != 2) {
          llfs::PinnedPage pinned_node_page;
          return visit_node_page(*query.page_loader,
                                 pinned_node_page,
                                 page_id_slot,
                                 [&](const PackedNodePage& packed_node) -> StatusOr<ValueView> {
                                   return packed_node.find_key(query);
                                 });
        }
        BATT_CHECK_EQ(parent_height, 2);

        usize key_index_if_found = 0;
        return find_key_in_leaf(page_id_slot, query, key_index_if_found);
      },
      [&](const std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<ValueView> {
        return leaf->find_key(query.key());
      },
      [&](const std::unique_ptr<InMemoryNode>& node) -> StatusOr<ValueView> {
        return node->find_key(query);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> Subtree::dump(i32 detail_level) const
{
  return [this](std::ostream& out) {
    batt::case_of(
        this->impl_,
        [&](const llfs::PageIdSlot& page_id_slot) {
          if (!page_id_slot.page_id) {
            out << "Empty{}";
          } else {
            out << page_id_slot.page_id;
          }
        },
        [&](const std::unique_ptr<InMemoryLeaf>& leaf) {
          out << "Leaf{"
              << "n_items=" << leaf->get_item_count()     //
              << ", size=" << leaf->get_items_size()      //
              << ", height=" << 1                         //
              << ", viability=" << leaf->get_viability()  //
              << ",}";
        },
        [&](const std::unique_ptr<InMemoryNode>& node) {
          out << "Node{"                    //
              << "height=" << node->height  //
              << ", pivots[" << node->pivot_count() << "]="
              << batt::dump_range(as_slice(node->pivot_keys_.data(), node->pivot_count()))  //
              << ", pending=" << batt::dump_range(node->pending_bytes)                      //
              << ", viability=" << node->get_viability()                                    //
              << ",}";
        });
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<llfs::PageId> Subtree::get_page_id() const
{
  return batt::case_of(
      this->impl_,
      [](const llfs::PageIdSlot& page_id_slot) -> Optional<llfs::PageId> {
        return page_id_slot.page_id;
      },
      [](const auto&) -> Optional<llfs::PageId> {
        return None;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Optional<Subtree>> Subtree::try_split(BatchUpdateContext& context)
{
  BATT_CHECK(!this->locked_.load());

  return batt::case_of(
      this->impl_,

      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<Optional<Subtree>> {
        BATT_PANIC() << "Splitting a serialized subtree is not supported! (Should have been split "
                        "*before* serialization)";

        return {batt::StatusCode::kUnimplemented};
      },

      [&](const std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<Optional<Subtree>> {
        BATT_ASSIGN_OK_RESULT(std::unique_ptr<InMemoryLeaf> leaf_upper_half,  //
                              leaf->try_split());

        if (leaf_upper_half == nullptr) {
          return Optional<Subtree>{None};
        }
        return {Subtree{std::move(leaf_upper_half)}};
      },

      [&](const std::unique_ptr<InMemoryNode>& node) -> StatusOr<Optional<Subtree>> {
        BATT_ASSIGN_OK_RESULT(std::unique_ptr<InMemoryNode> node_upper_half,  //
                              node->try_split(context));

        if (node_upper_half == nullptr) {
          return Optional<Subtree>{None};
        }

        return {Subtree{std::move(node_upper_half)}};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::try_flush(BatchUpdateContext& context)
{
  BATT_CHECK(!this->locked_.load());

  return batt::case_of(
      this->impl_,

      [&](const llfs::PageIdSlot& page_id_slot [[maybe_unused]]) -> Status {
        return {batt::StatusCode::kUnimplemented};
      },

      [&](const std::unique_ptr<InMemoryLeaf>& leaf [[maybe_unused]]) -> Status {
        return OkStatus();
      },

      [&](const std::unique_ptr<InMemoryNode>& node) -> Status {
        return node->try_flush(context);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PackedPageId Subtree::packed_page_id_or_panic() const
{
  BATT_CHECK((batt::is_case<llfs::PageIdSlot>(this->impl_)));

  return llfs::PackedPageId::from(std::get<llfs::PageIdSlot>(this->impl_).page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageIdSlot Subtree::page_id_slot_or_panic() const
{
  BATT_CHECK((batt::is_case<llfs::PageIdSlot>(this->impl_)));

  return std::get<llfs::PageIdSlot>(this->impl_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool Subtree::is_serialized() const
{
  return batt::is_case<llfs::PageIdSlot>(this->impl_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree Subtree::clone_serialized_or_panic() const
{
  BATT_CHECK((batt::is_case<llfs::PageIdSlot>(this->impl_)));

  return Subtree{std::get<llfs::PageIdSlot>(this->impl_)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::start_serialize(TreeSerializeContext& context)
{
  return batt::case_of(
      this->impl_,
      [](const llfs::PageIdSlot&) -> Status {
        return OkStatus();
      },
      [&context, this](std::unique_ptr<InMemoryLeaf>& leaf) -> Status {
        BATT_CHECK(!this->locked_.load());
        return leaf->start_serialize(context);
      },
      [&context, this](std::unique_ptr<InMemoryNode>& node) -> Status {
        BATT_CHECK(!this->locked_.load());
        return node->start_serialize(context);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PageId> Subtree::finish_serialize(TreeSerializeContext& context)
{
  batt::BoolStatus newly_serialized = batt::BoolStatus::kUnknown;

  StatusOr<llfs::PageId> page_id = batt::case_of(
      this->impl_,
      [&context,
       &newly_serialized](const llfs::PageIdSlot& page_id_slot) -> StatusOr<llfs::PageId> {
        newly_serialized = batt::BoolStatus::kFalse;
        return page_id_slot.page_id;
      },
      [&context, &newly_serialized, this](
          std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<llfs::PageId> {
        BATT_CHECK(!this->locked_.load());
        newly_serialized = batt::BoolStatus::kTrue;
        return leaf->finish_serialize(context);
      },
      [&context, &newly_serialized, this](
          std::unique_ptr<InMemoryNode>& node) -> StatusOr<llfs::PageId> {
        BATT_CHECK(!this->locked_.load());
        newly_serialized = batt::BoolStatus::kTrue;
        return node->finish_serialize(context);
      });

  BATT_REQUIRE_OK(page_id);

  BATT_CHECK_NE(newly_serialized, batt::BoolStatus::kUnknown);
  if (newly_serialized == batt::BoolStatus::kTrue) {
    this->impl_.emplace<llfs::PageIdSlot>(llfs::PageIdSlot::from_page_id(*page_id));
  }

  return page_id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void Subtree::lock()
{
  BATT_CHECK(this->is_serialized());
  this->locked_.store(true);
}

}  // namespace turtle_kv
