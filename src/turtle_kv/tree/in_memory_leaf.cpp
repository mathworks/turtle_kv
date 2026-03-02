#include <turtle_kv/tree/in_memory_leaf.hpp>
//

#include <turtle_kv/tree/filter_builder.hpp>
#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/the_key.hpp>

#include <batteries/algo/parallel_transform.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ std::unique_ptr<InMemoryLeaf> InMemoryLeaf::unpack(
    llfs::PinnedPage&& pinned_leaf_page,
    const TreeOptions& tree_options,
    const PackedLeafPage& packed_leaf,
    batt::WorkerPool& worker_pool) noexcept
{
  std::unique_ptr<InMemoryLeaf> new_leaf =
      std::make_unique<InMemoryLeaf>(batt::make_copy(pinned_leaf_page), tree_options);

  Slice<const PackedKeyValue> packed_items = packed_leaf.items_slice();
  std::vector<EditView> buffer;
  buffer.reserve(packed_items.size());

  {
    batt::ScopedWorkContext context{worker_pool};

    const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();
    const batt::TaskCount max_tasks{worker_pool.size() + 1};

    batt::parallel_transform(
        context,
        packed_items.begin(),
        packed_items.end(),
        buffer.data(),
        [](const PackedKeyValue& pkv) -> EditView {
          return to_edit_view(pkv);
        },
        /*min_task_size = */ algo_defaults.copy_edits.min_task_size,
        /*max_tasks = */ max_tasks);
  }

  MergeCompactor::ResultSet</*decay_to_items=*/true> result_set;
  const ItemView* first_edit = (const ItemView*)buffer.data();
  result_set.append(std::move(buffer), as_slice(first_edit, packed_items.size()));
  new_leaf->result_set = std::move(result_set);

  new_leaf->set_edit_size_totals(compute_running_total(worker_pool, *(new_leaf->result_set)));

  return {std::move(new_leaf)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SubtreeViability InMemoryLeaf::get_viability()
{
  const usize total_size = this->get_items_size();

  if (total_size < this->tree_options.flush_size() / 4) {
    return NeedsMerge{};
  } else if (total_size > this->tree_options.flush_size()) {
    return NeedsSplit{};
  } else {
    return Viable{};
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<InMemoryLeaf>> InMemoryLeaf::try_split()
{
  BATT_CHECK(this->edit_size_totals);
  BATT_CHECK(!this->edit_size_totals->empty());
  BATT_CHECK(this->result_set);
  BATT_CHECK_EQ(this->result_set->size() + 1,  //
                this->edit_size_totals->size());

  BATT_ASSIGN_OK_RESULT(SplitPlan plan, this->make_split_plan());

  // Sanity checks.
  //
  BATT_CHECK_LT(0, plan.split_point);
  BATT_CHECK_LT(plan.split_point, this->result_set->size());

  auto new_sibling =
      std::make_unique<InMemoryLeaf>(batt::make_copy(this->pinned_leaf_page_), this->tree_options);

  new_sibling->result_set = this->result_set;
  {
    const usize pre_drop_size = new_sibling->result_set->size();
    new_sibling->result_set->drop_before_n(plan.split_point);
    const usize post_drop_size = new_sibling->result_set->size();

    BATT_CHECK_EQ(post_drop_size, pre_drop_size - plan.split_point)
        << BATT_INSPECT(pre_drop_size) << BATT_INSPECT(plan);
  }
  new_sibling->shared_edit_size_totals_ = this->shared_edit_size_totals_;
  new_sibling->edit_size_totals = this->edit_size_totals;
  new_sibling->edit_size_totals->drop_front(plan.split_point);

  this->result_set->drop_after_n(plan.split_point);
  this->edit_size_totals->drop_back(this->edit_size_totals->size() - plan.split_point - 1);

  BATT_CHECK_EQ(this->result_set->size() + 1,  //
                this->edit_size_totals->size());

  BATT_CHECK_EQ(new_sibling->result_set->size() + 1,  //
                new_sibling->edit_size_totals->size());

  BATT_CHECK(!batt::is_case<NeedsSplit>(this->get_viability()))
      << BATT_INSPECT(this->get_viability()) << BATT_INSPECT(plan);

  BATT_CHECK(!batt::is_case<NeedsSplit>(new_sibling->get_viability()))
      << BATT_INSPECT(new_sibling->get_viability()) << BATT_INSPECT(plan);

  return {std::move(new_sibling)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto InMemoryLeaf::make_split_plan() const -> StatusOr<SplitPlan>
{
  BATT_CHECK(this->edit_size_totals);

  if (this->edit_size_totals->size() < 2) {
    return {batt::StatusCode::kFailedPrecondition};
  }

  SplitPlan plan;

  plan.min_viable_size = this->tree_options.flush_size() / 4;
  plan.max_viable_size = this->tree_options.flush_size();
  plan.total_size_before = this->get_items_size();
  plan.half_size = plan.total_size_before / 2;

  i32 direction = 0;

  const usize min_split_point = 1;
  const usize max_split_point = this->edit_size_totals->size() - 2;

  // Binary search for a starting split point, and then adjust it to make the plan viable.
  //
  plan.split_point = std::distance(this->edit_size_totals->begin(),                   //
                                   std::lower_bound(this->edit_size_totals->begin(),  //
                                                    this->edit_size_totals->end(),    //
                                                    plan.half_size));
  for (;;) {
    plan.first_size =
        (*this->edit_size_totals)[plan.split_point] - (*this->edit_size_totals).front();

    plan.second_size =
        (*this->edit_size_totals).back() - (*this->edit_size_totals)[plan.split_point];

    BATT_CHECK_EQ(plan.first_size + plan.second_size, plan.total_size_before) << BATT_INSPECT(plan);

    if (plan.first_size > plan.max_viable_size) {
      if (plan.split_point <= min_split_point || plan.second_size <= plan.min_viable_size) {
        return {batt::StatusCode::kOutOfRange};
      }
      if (direction == 1) {
        LOG(ERROR) << "Failed to create a leaf split plan:" << BATT_INSPECT(plan);
        return {batt::StatusCode::kInternal};
      }
      direction = -1;
      --plan.split_point;
      continue;
    }

    if (plan.second_size > plan.max_viable_size) {
      if (plan.split_point >= max_split_point || plan.first_size <= plan.min_viable_size) {
        return {batt::StatusCode::kOutOfRange};
      }
      if (direction == -1) {
        LOG(ERROR) << "Failed to create a leaf split plan:" << BATT_INSPECT(plan);
        return {batt::StatusCode::kInternal};
      }
      direction = 1;
      ++plan.split_point;
      continue;
    }

    break;
  }

  if (plan.first_size < plan.min_viable_size || plan.second_size < plan.min_viable_size) {
    return {batt::StatusCode::kOutOfRange};
  }

  BATT_CHECK_LE(plan.first_size, plan.max_viable_size);
  BATT_CHECK_LE(plan.second_size, plan.max_viable_size);

  return plan;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryLeaf::try_merge(BatchUpdateContext& context,
                               std::unique_ptr<InMemoryLeaf> sibling) noexcept
{
  BATT_CHECK(this->result_set);
  BATT_CHECK(sibling->result_set);

  if (sibling->result_set->empty()) {
    BATT_CHECK(batt::is_case<Viable>(this->get_viability()));
    return OkStatus();
  }

  if (this->result_set->empty()) {
    BATT_CHECK(batt::is_case<Viable>(sibling->get_viability()));
    this->pinned_leaf_page_ = std::move(sibling->pinned_leaf_page_);
    this->result_set = std::move(sibling->result_set);
    this->shared_edit_size_totals_ = sibling->shared_edit_size_totals_;
    this->edit_size_totals = std::move(sibling->edit_size_totals);
    return OkStatus();
  }

  BATT_CHECK_LT(this->get_max_key(), sibling->get_min_key());

  this->result_set = MergeCompactor::ResultSet<true>::concat(std::move(*this->result_set),
                                                             std::move(*(sibling->result_set)));

  this->set_edit_size_totals(context.compute_running_total(*this->result_set));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryLeaf::apply_batch_update(BatchUpdate& update) noexcept
{
  Optional<BoxedSeq<EditSlice>> current_edits = None;

  if (this->pinned_leaf_page_ && !this->result_set) {
    // In this case, we have initialized a new InMemoryLeaf from a PackedLeaf. Use the
    // items from the PackedLeaf to merge with the incoming update.
    //
    const PackedLeafPage& packed_leaf = PackedLeafPage::view_of(this->pinned_leaf_page_);
    current_edits = packed_leaf.as_edit_slice_seq();

  } else if (this->result_set) {
    // In this case, we have an existing InMemoryLeaf that we are applying updates to.
    // Use the existing ResultSet to merge with the incoming update.
    //
    current_edits = this->result_set->live_edit_slices();
  }

  // If we didn't enter either of the above two cases, we must have an empty tree that we are
  // applying updates to.
  //
  BATT_CHECK_IMPLIES(!current_edits, !this->pinned_leaf_page_ && !this->result_set);

  BATT_ASSIGN_OK_RESULT(this->result_set,
                        update.context.merge_compact_edits</*decay_to_items=*/true>(
                            global_max_key(),
                            [&](MergeCompactor& compactor) -> Status {
                              compactor.push_level(update.result_set.live_edit_slices());
                              if (current_edits) {
                                compactor.push_level(std::move(*current_edits));
                              }
                              return OkStatus();
                            }));

  this->result_set->update_has_page_refs(update.result_set.has_page_refs());
  this->set_edit_size_totals(update.context.compute_running_total(*this->result_set));

  return OkStatus();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryLeaf::start_serialize(TreeSerializeContext& context)
{
  BATT_CHECK(this->result_set);

  BATT_CHECK(!batt::is_case<NeedsSplit>(this->get_viability()))
      << BATT_INSPECT(this->get_viability()) << BATT_INSPECT(this->get_items_size())
      << BATT_INSPECT(this->tree_options.flush_size());

  auto filter_bits_per_key = context.tree_options().filter_bits_per_key();
  const bool overcommit_triggered = context.overcommit().is_triggered();
  llfs::PageSize filter_page_size = context.tree_options().filter_page_size();

  BATT_ASSIGN_OK_RESULT(
      const u64 future_id,
      context.async_build_page(
          this->tree_options.leaf_size(),
          packed_leaf_page_layout_id(),
          llfs::LruPriority{kNewLeafLruPriority},
          /*task_count=*/2,
          [this, filter_bits_per_key, filter_page_size, overcommit_triggered](
              usize task_i,
              llfs::PageCache& page_cache,
              llfs::PageBuffer& page_buffer) -> StatusOr<TreeSerializeContext::PinPageToJobFn>  //
          {
            if (task_i == 0) {
              return build_leaf_page_in_job(this->tree_options.trie_index_reserve_size(),
                                            page_buffer,
                                            this->result_set->get());
            }
            BATT_CHECK_EQ(task_i, 1);

            return build_filter_for_leaf_in_job(page_cache,
                                                overcommit_triggered,
                                                filter_bits_per_key,
                                                filter_page_size,
                                                page_buffer.page_id(),
                                                this->result_set->get());
          }));

  BATT_CHECK_EQ(this->future_id_.exchange(future_id), ~u64{0});

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PageId> InMemoryLeaf::finish_serialize(TreeSerializeContext& context)
{
  BATT_CHECK_EQ(this->tree_options.filter_bits_per_key(),
                context.tree_options().filter_bits_per_key());
  BATT_CHECK_EQ(this->tree_options.expected_items_per_leaf(),
                context.tree_options().expected_items_per_leaf());

  u64 observed_id = this->future_id_.load();

  if (observed_id == ~u64{1}) {
    return {batt::StatusCode::kFailedPrecondition};
  }

  StatusOr<llfs::PinnedPage> pinned_leaf_page =
      context.get_build_page_result(TreeSerializeContext::BuildPageJobId{observed_id});

  return pinned_leaf_page->page_id();
}

}  // namespace turtle_kv
