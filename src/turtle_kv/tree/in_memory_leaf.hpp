#pragma once

#include <turtle_kv/tree/batch_update.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/subtree_viability.hpp>
#include <turtle_kv/tree/tree_options.hpp>
#include <turtle_kv/tree/tree_serialize_context.hpp>

#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/optional.hpp>

#include <batteries/algo/running_total.hpp>

#include <memory>
#include <ostream>

namespace turtle_kv {

struct InMemoryLeaf {
  struct SplitPlan {
    usize min_viable_size = 0;
    usize max_viable_size = 0;
    usize total_size_before = 0;
    usize half_size = 0;
    usize split_point = 0;
    usize first_size = 0;
    usize second_size = 0;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PinnedPage pinned_leaf_page_;
  TreeOptions tree_options;
  Optional<MergeCompactor::ResultSet</*decay_to_items=*/true>> result_set;
  std::shared_ptr<const batt::RunningTotal> shared_edit_size_totals_;
  Optional<batt::RunningTotal::slice_type> edit_size_totals;
  mutable std::atomic<u64> future_id_{~u64{0}};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static std::unique_ptr<InMemoryLeaf> unpack(llfs::PinnedPage&& pinned_leaf_page,
                                              const TreeOptions& tree_options,
                                              const PackedLeafPage& packed_leaf,
                                              batt::WorkerPool& worker_pool) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit InMemoryLeaf(llfs::PinnedPage&& pinned_leaf_page,
                        const TreeOptions& tree_options_arg) noexcept
      : pinned_leaf_page_{std::move(pinned_leaf_page)}
      , tree_options{tree_options_arg}
      , result_set{None}
  {
  }

  void set_edit_size_totals(batt::RunningTotal&& running_total)
  {
    this->shared_edit_size_totals_ = std::make_shared<batt::RunningTotal>(std::move(running_total));

    this->edit_size_totals.emplace(this->shared_edit_size_totals_->begin(),
                                   this->shared_edit_size_totals_->end());
  }

  usize get_item_count() const
  {
    BATT_CHECK(this->result_set);
    return this->result_set->size();
  }

  usize get_items_size() const
  {
    BATT_CHECK(this->edit_size_totals);
    BATT_CHECK(this->result_set);
    BATT_CHECK_EQ(this->edit_size_totals->size(), this->result_set->size() + 1);

    if (this->edit_size_totals->empty()) {
      return 0;
    }
    return this->edit_size_totals->back() - this->edit_size_totals->front();
  }

  KeyView get_min_key() const
  {
    BATT_CHECK(this->result_set);
    return this->result_set->get_min_key();
  }

  KeyView get_max_key() const
  {
    BATT_CHECK(this->result_set);
    return this->result_set->get_max_key();
  }

  StatusOr<ValueView> find_key(const KeyView& key) const
  {
    BATT_CHECK(this->result_set);
    return this->result_set->find_key(key);
  }

  SubtreeViability get_viability();

  StatusOr<std::unique_ptr<InMemoryLeaf>> try_split();

  StatusOr<SplitPlan> make_split_plan() const;

  Status try_merge(BatchUpdateContext& context, std::unique_ptr<InMemoryLeaf> sibling) noexcept;

  Status apply_batch_update(BatchUpdate& update) noexcept;

  Status start_serialize(TreeSerializeContext& context);

  StatusOr<llfs::PageId> finish_serialize(TreeSerializeContext& context);
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

inline std::ostream& operator<<(std::ostream& out, const InMemoryLeaf::SplitPlan& t)
{
  return out << "InMemoryLeaf::SplitPlan{.total_size_before=" << t.total_size_before
             << ", .half_size=" << t.half_size << ", .split_point=" << t.split_point
             << ", .first_size=" << t.first_size << ", .second_size=" << t.second_size
             << ", .min_viable_size=" << t.min_viable_size << ",}";
}

}  // namespace turtle_kv
