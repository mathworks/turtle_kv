#include <turtle_kv/tree/subtree.hpp>
//

#include <turtle_kv/tree/in_memory_leaf.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/packed_node_page.hpp>
#include <turtle_kv/tree/the_key.hpp>
#include <turtle_kv/tree/visit_tree_page.hpp>

#include <llfs/sharded_page_view.hpp>

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
StatusOr<ValueView> Subtree::find_key(ParentNodeHeight parent_height,
                                      llfs::PageLoader& page_loader,
                                      llfs::PinnedPage& pinned_page_out,
                                      const KeyView& key) const
{
  return batt::case_of(
      this->impl,
      [&](const llfs::PageIdSlot& page_id_slot) -> StatusOr<ValueView> {
        return visit_tree_page(  //
            page_loader,
            pinned_page_out,
            page_id_slot,

            [&](const PackedLeafPage& packed_leaf) -> StatusOr<ValueView> {
              const PackedKeyValue* found = packed_leaf.find_key(key);
              if (!found) {
                return {batt::StatusCode::kNotFound};
              }
              return get_value(*found);
            },

            [&](const PackedNodePage& packed_node) -> StatusOr<ValueView> {
              return packed_node.find_key(page_loader, pinned_page_out, key);
            });
      },
      [&](const std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<ValueView> {
        return leaf->find_key(key);
      },
      [&](const std::unique_ptr<InMemoryNode>& node) -> StatusOr<ValueView> {
        return node->find_key(page_loader, pinned_page_out, key);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> Subtree::find_key_filtered(ParentNodeHeight parent_height,
                                               FilteredKeyQuery& query) const
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
          return visit_node_page(*query.page_loader,
                                 *query.pinned_page_out,
                                 page_id_slot,
                                 [&](const PackedNodePage& packed_node) -> StatusOr<ValueView> {
                                   return packed_node.find_key_filtered(query);
                                 });
        }
        BATT_CHECK_EQ(parent_height, 2);

        PageSliceReader slice_reader{*query.page_loader, page_id_slot, llfs::PageSize{4096}};

        const auto head_shard_size =
            llfs::PageSize{(u32)query.tree_options->trie_index_sharded_view_size()};

        PageSliceStorage head_storage;
        BATT_ASSIGN_OK_RESULT(ConstBuffer head_buffer,
                              slice_reader.read_slice(head_shard_size,
                                                      Interval<usize>{0, head_shard_size},
                                                      head_storage));

        const void* page_start = head_buffer.data();
        const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
        const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

        // Sanity check; make sure this is a leaf!
        //
        packed_leaf_page.check_magic();

        // There should be a Trie index, and it should have fit inside the head_buffer; sanity
        // check these assertions.
        //
        const void* trie_begin = packed_leaf_page.trie_index.get();
        const void* trie_end = advance_pointer(trie_begin, packed_leaf_page.trie_index_size);
        BATT_CHECK_LE(byte_distance(page_start, trie_end), head_shard_size);

        // We have the Trie index in memory; query it to find the range containing our query key.
        //
        usize key_prefix_match = 0;
        const Interval<usize> search_range =
            packed_leaf_page.calculate_search_range(query.key(), key_prefix_match);

        // Calculate the leaf page slice containing the range of PackedKeyValue objects we will need
        // to binary search.  We add two to the end of the range; one because we only know how long
        // a key is by comparing its offset to the offset of the next key, and two because we must
        // know the size of the key after that in order to know the value size.
        //
        const PackedKeyValue* head_items = packed_leaf_page.items->data();
        const Interval<usize> items_slice{
            (usize)byte_distance(page_start, head_items + search_range.lower_bound),
            (usize)byte_distance(page_start, head_items + (search_range.upper_bound + 2)),
        };

        // To binary-search the keys, we must pin *both* the portion of the items array we will
        // access (items_slice) *and* the key data pointed to by those items.  Once both are pinned,
        // we calculate a single offset delta to add to the packed offsets inside our KeyOrder
        // comparator.
        //
        PageSliceStorage items_storage;
        BATT_ASSIGN_OK_RESULT(ConstBuffer items_buffer,
                              slice_reader.read_slice(items_slice, items_storage));

        const auto items_begin = (const PackedKeyValue*)items_buffer.data();
        const auto items_end = items_begin + search_range.size();

        // We must include the data of the two keys beyond the nominal search range; since
        // `items_end` is already one past, we must add one to that to get the final upper bound.
        //
        Interval<usize> key_data_slice{
            (usize)(items_slice.lower_bound + items_begin->key_offset),
            (usize)(items_slice.upper_bound + (items_end + 1)->key_offset),
        };

        PageSliceStorage key_data_storage;
        BATT_ASSIGN_OK_RESULT(ConstBuffer key_data_buffer,
                              slice_reader.read_slice(key_data_slice, key_data_storage));

        // `items_begin + items_begin->key_offset` corresponds to the start of the key data buffer;
        // calculate their (signed) difference now that both shards are pinned.
        //
        const isize offset_base = items_begin->key_offset;
        const isize offset_target = byte_distance(items_begin, key_data_buffer.data());
        const isize offset_delta = offset_target - offset_base;

        // Binary search to find the query key.
        //
        auto it = std::lower_bound(items_begin,
                                   items_end,
                                   query.key(),
                                   ShiftedPackedKeyOrder{offset_delta});
        if (it == items_end) {
          return {batt::StatusCode::kNotFound};
        }

        // IMPORTANT: we must use `shifted_key_view` and not `key_view` here, or the referenced data
        // will be invalid!
        //
        const KeyView found_lower_bound = it->shifted_key_view(offset_delta);
        if (found_lower_bound != query.key()) {
          return {batt::StatusCode::kNotFound};
        }

        // Calculate the location within the page containing the value data.
        //
        Interval<usize> value_data_slice;

        value_data_slice.lower_bound =
            key_data_slice.lower_bound +
            (usize)byte_distance(key_data_buffer.data(), it->shifted_value_data(offset_delta));

        value_data_slice.upper_bound =
            value_data_slice.lower_bound + it->shifted_value_size(offset_delta);

        // TODO [tastolfi 2025-04-18] REMOVE ME!
        //  Do a whole-page query and compare the result with the sharded view query result.
        //
        StatusOr<ValueView> whole_page_result = visit_leaf_page(  //
            *query.page_loader,
            *query.pinned_page_out,
            page_id_slot,

            [&](const PackedLeafPage& packed_leaf) -> StatusOr<ValueView> {
              const PackedKeyValue* found = packed_leaf.find_key(query.key());
              if (!found) {
                return {batt::StatusCode::kNotFound};
              }
              return get_value(*found);
            });

        BATT_CHECK_OK(whole_page_result);

        llfs::PinnedPage& pinned_whole_leaf = *query.pinned_page_out;
        Interval<usize> expected_value_data_slice;

        expected_value_data_slice.lower_bound =
            byte_distance(pinned_whole_leaf.raw_data(), whole_page_result->as_str().data()) -
            (1 /*for the op_code byte*/);

        expected_value_data_slice.upper_bound = expected_value_data_slice.lower_bound +
                                                whole_page_result->size() +
                                                (1 /*for the op_code byte*/);

        static std::atomic<usize> success_count{0};
        static std::atomic<usize> failure_count{0};

        if (value_data_slice == expected_value_data_slice) {
          success_count.fetch_add(1);
        } else {
          failure_count.fetch_add(1);
          LOG(INFO) << BATT_INSPECT(success_count) << BATT_INSPECT(failure_count);
        }
        //        BATT_CHECK_EQ(value_data_slice, expected_value_data_slice) <<
        //        BATT_INSPECT(success_count);

        return whole_page_result;
      },
      [&](const std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<ValueView> {
        return leaf->find_key(query.key());
      },
      [&](const std::unique_ptr<InMemoryNode>& node) -> StatusOr<ValueView> {
        return node->find_key(*query.page_loader, *query.pinned_page_out, query.key());
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
StatusOr<llfs::PinnedPage> Subtree::finish_serialize(TreeSerializeContext& context)
{
  StatusOr<llfs::PinnedPage> pinned_page = batt::case_of(
      this->impl,
      [&context](llfs::PageIdSlot& page_id_slot) -> StatusOr<llfs::PinnedPage> {
        return page_id_slot.load_through(context.page_job(),
                                         /*required_layout=*/None,
                                         llfs::PinPageToJob::kDefault,
                                         llfs::OkIfNotFound{false});
      },
      [&context](std::unique_ptr<InMemoryLeaf>& leaf) -> StatusOr<llfs::PinnedPage> {
        return leaf->finish_serialize(context);
      },
      [&context](std::unique_ptr<InMemoryNode>& node) -> StatusOr<llfs::PinnedPage> {
        return node->finish_serialize(context);
      });

  BATT_REQUIRE_OK(pinned_page);

  this->impl.emplace<llfs::PageIdSlot>(llfs::PageIdSlot::from_page_id(pinned_page->page_id()));

  return pinned_page;
}

}  // namespace turtle_kv
