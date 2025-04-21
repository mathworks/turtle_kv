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
  return Subtree{
      .impl = llfs::PageIdSlot::from_page_id(page_id),
  };
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
Status Subtree::apply_batch_update(const TreeOptions& tree_options,
                                   ParentNodeHeight parent_height,
                                   BatchUpdate& update,
                                   const KeyView& key_upper_bound,
                                   IsRoot is_root)
{
  BATT_CHECK_GT(parent_height, 0);

  Subtree& subtree = *this;

  StatusOr<Subtree> new_subtree = batt::case_of(  //
      subtree.impl,

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](llfs::PageIdSlot& page_id_slot) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: merge {update} + {empty} => InMemoryLeaf

        // Check for empty tree (page id with special invalid value ~u64{0})
        //
        if (!page_id_slot.is_valid()) {
          BATT_CHECK_EQ(parent_height, 1);

          auto new_leaf = std::make_unique<InMemoryLeaf>(tree_options);

          new_leaf->result_set = update.result_set;

          if (!update.edit_size_totals) {
            update.update_edit_size_totals();
          }

          new_leaf->set_edit_size_totals(std::move(*update.edit_size_totals));
          update.edit_size_totals = None;

          return Subtree{.impl = std::move(new_leaf)};
        }
        BATT_CHECK_GT(parent_height, 1);

        llfs::PageLayoutId expected_layout = Subtree::expected_layout_for_height(parent_height - 1);

        StatusOr<llfs::PinnedPage> status_or_pinned_page =
            page_id_slot.load_through(update.page_loader,
                                      expected_layout,
                                      llfs::PinPageToJob::kDefault,
                                      llfs::OkIfNotFound{false});

        BATT_REQUIRE_OK(status_or_pinned_page) << BATT_INSPECT(parent_height);

        llfs::PinnedPage& pinned_page = *status_or_pinned_page;

        if (parent_height == 2) {
          //+++++++++++-+-+--+----- --- -- -  -  -   -
          // Case: {BatchUpdate} + {PackedLeafPage} => InMemoryLeaf

          const PackedLeafPage& packed_leaf = PackedLeafPage::view_of(pinned_page);
          auto new_leaf = std::make_unique<InMemoryLeaf>(tree_options);

          BATT_ASSIGN_OK_RESULT(
              new_leaf->result_set,
              update.merge_compact_edits(global_max_key(),
                                         [&](MergeCompactor::GeneratorContext& context) -> Status {
                                           MergeFrame frame;
                                           frame.push_line(update.result_set.live_edit_slices());
                                           frame.push_line(packed_leaf.as_edit_slice_seq());
                                           context.push_frame(&frame);
                                           return context.await_frame_consumed(&frame);
                                         }));

          new_leaf->set_edit_size_totals(
              compute_running_total(update.worker_pool, new_leaf->result_set));

          return Subtree{.impl = std::move(new_leaf)};

        } else {
          //+++++++++++-+-+--+----- --- -- -  -  -   -
          // Case: {BatchUpdate} + {PackedNodePage} => InMemoryNode

          const PackedNodePage& packed_node = PackedNodePage::view_of(pinned_page);

          BATT_ASSIGN_OK_RESULT(std::unique_ptr<InMemoryNode> node,
                                InMemoryNode::unpack(tree_options, packed_node));

          BATT_REQUIRE_OK(node->apply_batch_update(update, key_upper_bound, is_root));

          return Subtree{.impl = std::move(node)};
        }
      },

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](std::unique_ptr<InMemoryLeaf>& in_memory_leaf) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: {BatchUpdate} + {InMemoryLeaf} => InMemoryLeaf

        BATT_CHECK_EQ(parent_height, 2);

        BATT_ASSIGN_OK_RESULT(in_memory_leaf->result_set,
                              update.merge_compact_edits(
                                  global_max_key(),
                                  [&](MergeCompactor::GeneratorContext& context) -> Status {
                                    MergeFrame frame;
                                    frame.push_line(update.result_set.live_edit_slices());
                                    frame.push_line(in_memory_leaf->result_set.live_edit_slices());
                                    context.push_frame(&frame);
                                    return context.await_frame_consumed(&frame);
                                  }));

        in_memory_leaf->result_set.update_has_page_refs(update.result_set.has_page_refs());
        in_memory_leaf->set_edit_size_totals(
            compute_running_total(update.worker_pool, in_memory_leaf->result_set));

        return Subtree{.impl = std::move(in_memory_leaf)};
      },

      //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
      //
      [&](std::unique_ptr<InMemoryNode>& in_memory_node) -> StatusOr<Subtree> {
        //+++++++++++-+-+--+----- --- -- -  -  -   -
        // Case: {BatchUpdate} + {InMemoryNode} => InMemoryNode

        BATT_CHECK_GT(parent_height, 2);

        BATT_REQUIRE_OK(in_memory_node->apply_batch_update(update, key_upper_bound, is_root));

        return Subtree{.impl = std::move(in_memory_node)};
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
        [&](const NeedsSplit&) {
          return new_subtree->split_and_grow(update.page_loader, tree_options, key_upper_bound);
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
Status Subtree::split_and_grow(llfs::PageLoader& page_loader,
                               const TreeOptions& tree_options,
                               const KeyView& key_upper_bound)
{
  BATT_ASSIGN_OK_RESULT(  //
      Subtree upper_half_subtree,
      this->try_split(page_loader));

  Subtree& lower_half_subtree = *this;

  BATT_ASSIGN_OK_RESULT(  //
      std::unique_ptr<InMemoryNode> new_root,
      InMemoryNode::from_subtrees(page_loader,
                                  tree_options,
                                  std::move(lower_half_subtree),
                                  std::move(upper_half_subtree),
                                  key_upper_bound,
                                  IsRoot{true}));

  this->impl = std::move(new_root);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> Subtree::get_height(llfs::PageLoader& page_loader) const
{
  return batt::case_of(
      this->impl,
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
      this->impl,
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
      this->impl,
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
      this->impl,
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
      this->impl,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<ValueView> {
        if (query.reject_page(page_id_slot.page_id)) {
          return {batt::StatusCode::kNotFound};
        }

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
        this->impl,
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
      this->impl,
      [](const llfs::PageIdSlot& page_id_slot) -> Optional<llfs::PageId> {
        return page_id_slot.page_id;
      },
      [](const auto&) -> Optional<llfs::PageId> {
        return None;
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Subtree> Subtree::try_split(llfs::PageLoader& page_loader)
{
  return batt::case_of(
      this->impl,

      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<Subtree> {
        BATT_PANIC() << "TODO [tastolfi 2025-03-16] implement me!";

        return {batt::StatusCode::kUnimplemented};
      },

      [&](const std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<Subtree> {
        BATT_ASSIGN_OK_RESULT(std::unique_ptr<InMemoryLeaf> leaf_upper_half,  //
                              leaf->try_split());

        return Subtree{.impl = std::move(leaf_upper_half)};
      },

      [&](const std::unique_ptr<InMemoryNode>& node) -> StatusOr<Subtree> {
        BATT_ASSIGN_OK_RESULT(std::unique_ptr<InMemoryNode> node_upper_half,  //
                              node->try_split(page_loader));

        return Subtree{.impl = std::move(node_upper_half)};
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::try_flush(batt::WorkerPool& worker_pool,
                          llfs::PageLoader& page_loader,
                          const batt::CancelToken& cancel_token)
{
  return batt::case_of(
      this->impl,

      [&](const llfs::PageIdSlot& page_id_slot [[maybe_unused]]) -> Status {
        return {batt::StatusCode::kUnimplemented};
      },

      [&](const std::unique_ptr<InMemoryLeaf>& leaf [[maybe_unused]]) -> Status {
        return OkStatus();
      },

      [&](const std::unique_ptr<InMemoryNode>& node) -> Status {
        return node->try_flush(worker_pool, page_loader, cancel_token);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PackedPageId Subtree::packed_page_id_or_panic() const
{
  BATT_CHECK((batt::is_case<llfs::PageIdSlot>(this->impl)));

  return llfs::PackedPageId::from(std::get<llfs::PageIdSlot>(this->impl).page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool Subtree::is_serialized() const
{
  return batt::is_case<llfs::PageIdSlot>(this->impl);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree Subtree::clone_serialized_or_panic() const
{
  BATT_CHECK((batt::is_case<llfs::PageIdSlot>(this->impl)));

  Subtree clone;
  clone.impl.emplace<llfs::PageIdSlot>() = *this->get_page_id();
  return clone;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status Subtree::start_serialize(TreeSerializeContext& context)
{
  return batt::case_of(
      this->impl,
      [](const llfs::PageIdSlot&) -> Status {
        return OkStatus();
      },
      [&context](std::unique_ptr<InMemoryLeaf>& leaf) -> Status {
        return leaf->start_serialize(context);
      },
      [&context](std::unique_ptr<InMemoryNode>& node) -> Status {
        return node->start_serialize(context);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PageId> Subtree::finish_serialize(TreeSerializeContext& context)
{
  batt::BoolStatus newly_serialized = batt::BoolStatus::kUnknown;

  StatusOr<llfs::PageId> page_id = batt::case_of(
      this->impl,
      [&context,
       &newly_serialized](const llfs::PageIdSlot& page_id_slot) -> StatusOr<llfs::PageId> {
        newly_serialized = batt::BoolStatus::kFalse;
        return page_id_slot.page_id;
      },
      [&context, &newly_serialized](std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<llfs::PageId> {
        newly_serialized = batt::BoolStatus::kTrue;
        return leaf->finish_serialize(context);
      },
      [&context, &newly_serialized](std::unique_ptr<InMemoryNode>& node) -> StatusOr<llfs::PageId> {
        newly_serialized = batt::BoolStatus::kTrue;
        return node->finish_serialize(context);
      });

  BATT_REQUIRE_OK(page_id);

  BATT_CHECK_NE(newly_serialized, batt::BoolStatus::kUnknown);
  if (newly_serialized == batt::BoolStatus::kTrue) {
    this->impl.emplace<llfs::PageIdSlot>(llfs::PageIdSlot::from_page_id(*page_id));
  }

  return page_id;
}

}  // namespace turtle_kv
