#include <turtle_kv/tree/in_memory_leaf.hpp>
//

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/the_key.hpp>

namespace turtle_kv {

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
  BATT_CHECK_EQ(this->result_set.size() + 1,  //
                this->edit_size_totals->size());

  BATT_ASSIGN_OK_RESULT(SplitPlan plan, this->make_split_plan());

  // Sanity checks.
  //
  BATT_CHECK_LT(0, plan.split_point);
  BATT_CHECK_LT(plan.split_point, this->result_set.size());

  auto new_sibling = std::make_unique<InMemoryLeaf>(this->tree_options);

  new_sibling->result_set = this->result_set;
  {
    const usize pre_drop_size = new_sibling->result_set.size();
    new_sibling->result_set.drop_before_n(plan.split_point);
    const usize post_drop_size = new_sibling->result_set.size();

    BATT_CHECK_EQ(post_drop_size, pre_drop_size - plan.split_point)
        << BATT_INSPECT(pre_drop_size) << BATT_INSPECT(plan);
  }
  new_sibling->shared_edit_size_totals_ = this->shared_edit_size_totals_;
  new_sibling->edit_size_totals = this->edit_size_totals;
  new_sibling->edit_size_totals->drop_front(plan.split_point);

  this->result_set.drop_after_n(plan.split_point);
  this->edit_size_totals->drop_back(this->edit_size_totals->size() - plan.split_point - 1);

  BATT_CHECK_EQ(this->result_set.size() + 1,  //
                this->edit_size_totals->size());

  BATT_CHECK_EQ(new_sibling->result_set.size() + 1,  //
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
        LOG(ERROR) << BATT_INSPECT(plan);
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
        LOG(ERROR) << BATT_INSPECT(plan);
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status InMemoryLeaf::start_serialize(TreeSerializeContext& context)
{
  BATT_CHECK(!batt::is_case<NeedsSplit>(this->get_viability()))
      << BATT_INSPECT(this->get_viability()) << BATT_INSPECT(this->get_items_size())
      << BATT_INSPECT(this->tree_options.flush_size());

  BATT_ASSIGN_OK_RESULT(
      const u64 future_id,
      context.async_build_page(
          this->tree_options.leaf_size(),
          packed_leaf_page_layout_id(),
          [this](llfs::PageBuffer& page_buffer) -> StatusOr<TreeSerializeContext::PinPageToJobFn> {
            // TODO [tastolfi 2025-03-27] decay items
            //
            return build_leaf_page_in_job(page_buffer, this->result_set.get());
          }));

  BATT_CHECK_EQ(this->future_id_.exchange(future_id), ~u64{0});

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> InMemoryLeaf::finish_serialize(TreeSerializeContext& context)
{
  u64 observed_id = this->future_id_.load();

  if (observed_id == ~u64{1}) {
    return {batt::StatusCode::kFailedPrecondition};
  }

  StatusOr<llfs::PinnedPage> pinned_leaf_page =
      context.get_build_page_result(TreeSerializeContext::BuildPageJobId{observed_id});

  return pinned_leaf_page;
}

}  // namespace turtle_kv
