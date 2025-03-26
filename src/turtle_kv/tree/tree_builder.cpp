#include <turtle_kv/tree/tree_builder.hpp>
//

#include <turtle_kv/tree/in_memory_node.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ TreeBuilder::TreeBuilder(batt::WorkerPool& worker_pool,  //
                                      llfs::PageCache& page_cache,    //
                                      const TreeOptions& tree_options) noexcept
    : worker_pool_{worker_pool}
    , page_cache_{page_cache}
    , page_loader_{page_cache}
    , tree_options_{tree_options}
    , tree_{Subtree::make_empty()}
    , height_{0}
    , cancel_token_{nullptr}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status TreeBuilder::apply_batch_update(
    MergeCompactor::ResultSet</*decay_to_items=*/false>&& result_set) noexcept
{
  //----- --- -- -  -  -   -
  // Create and install a CancelToken.
  //
  batt::CancelToken cancel_token;
  BATT_CHECK_EQ(this->cancel_token_.exchange(&cancel_token), nullptr);
  auto on_scope_exit = batt::finally([&] {
    BATT_CHECK_EQ(this->cancel_token_.exchange(nullptr), &cancel_token);
  });

  //----- --- -- -  -  -   -
  // Create a BatchUpdate to apply to the tree.
  //
  BatchUpdate batch_update{
      .worker_pool = this->worker_pool_,
      .page_loader = *this->page_loader_,
      .cancel_token = cancel_token,
      .result_set = std::move(result_set),
      .edit_size_totals = None,
  };

  //----- --- -- -  -  -   -
  // Apply the batch update to the tree.
  //
  BATT_REQUIRE_OK(this->tree_.apply_batch_update(  //
      this->tree_options_,                         //
      /*parent_height=*/this->height_ + 1,         //
      batch_update,                                //
      /*key_upper_bound=*/global_max_key(),        //
      IsRoot{true}));

  //----- --- -- -  -  -   -
  // Update the height.
  //
  BATT_ASSIGN_OK_RESULT(this->height_, this->tree_.get_height(*this->page_loader_));

  //----- --- -- -  -  -   -
  // If the tree needs to grow/shrink in height, do so now.
  //
  BATT_REQUIRE_OK(batt::case_of(
      this->tree_.get_viability(),
      [](const Viable&) -> Status {
        // Nothing to fix; tree is viable!
        return OkStatus();
      },
      [&](const NeedsSplit&) {
        return this->grow_tree();
      },
      [&](const NeedsMerge& needs_merge) {
        BATT_CHECK(!needs_merge.single_pivot);
        return OkStatus();
      }));

  // Success!
  //
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<llfs::PageCacheJob>> TreeBuilder::serialize(
    llfs::PageCache& page_cache) noexcept
{
  //----- --- -- -  -  -   -
  // Create a new PageCacheJob for the newly serialized tree pages.
  //
  std::unique_ptr<llfs::PageCacheJob> page_job = page_cache.new_job();

  //----- --- -- -  -  -   -
  // Create a context object to pass to start_serialize/finish_serialize.
  //
  TreeSerializeContext context{
      this->tree_options_,
      *page_job,
      this->worker_pool_,
  };

  //----- --- -- -  -  -   -
  // Start the serialization.
  //
  // This will cause a bunch of tree building jobs to be added to the TreeSerializeContext; we will
  // run them all in parallel in the worker pool once we have all of them.
  //
  BATT_REQUIRE_OK(this->tree_.start_serialize(context));

  //----- --- -- -  -  -   -
  // Build all new pages.
  //
  // This farms out work to the WorkerPool.
  //
  BATT_REQUIRE_OK(context.build_all_pages());

  //----- --- -- -  -  -   -
  // Finish the serialization by making another pass through the tree.
  //
  // This allows each node/leaf to retrieve any pages for which it created jobs during
  // start_serialize.
  //
  BATT_ASSIGN_OK_RESULT(llfs::PinnedPage pinned_root_page, this->tree_.finish_serialize(context));

  //----- --- -- -  -  -   -
  // Tell the PageCacheJob about the root page so it can trace references from there.
  //
  Optional<llfs::PageId> new_root_page_id = this->tree_.get_page_id();

  BATT_CHECK(new_root_page_id);
  BATT_CHECK_EQ(*new_root_page_id, pinned_root_page.page_id());

  page_job->new_root(*new_root_page_id);

  // Success!
  //
  return {std::move(page_job)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status TreeBuilder::grow_tree() noexcept
{
  BATT_ASSIGN_OK_RESULT(  //
      Subtree upper_half_subtree, this->tree_.try_split(*this->page_loader_));

  Subtree lower_half_subtree = std::move(this->tree_);

  BATT_ASSIGN_OK_RESULT(  //
      std::unique_ptr<InMemoryNode> new_root,
      InMemoryNode::from_subtrees(*this->page_loader_,            //
                                  this->tree_options_,            //
                                  std::move(lower_half_subtree),  //
                                  std::move(upper_half_subtree),  //
                                  global_max_key(),               //
                                  IsRoot{true}));

  this->tree_ = Subtree{
      .impl = std::move(new_root),
  };

  this->height_ += 1;

  return OkStatus();
}

}  // namespace turtle_kv
