#pragma once

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

  TreeOptions tree_options;
  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  std::shared_ptr<const batt::RunningTotal> shared_edit_size_totals_;
  Optional<batt::RunningTotal::slice_type> edit_size_totals;
  mutable std::atomic<u64> future_id_{~u64{0}};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit InMemoryLeaf(const TreeOptions& tree_options_arg) noexcept
      : tree_options{tree_options_arg}
  {
  }

  void set_edit_size_totals(batt::RunningTotal&& running_total) noexcept
  {
    this->shared_edit_size_totals_ = std::make_shared<batt::RunningTotal>(std::move(running_total));

    this->edit_size_totals.emplace(this->shared_edit_size_totals_->begin(),
                                   this->shared_edit_size_totals_->end());
  }

  usize get_item_count() const noexcept
  {
    return this->result_set.size();
  }

  usize get_items_size() const noexcept
  {
    BATT_CHECK(this->edit_size_totals);
    BATT_CHECK_EQ(this->edit_size_totals->size(), this->result_set.size() + 1);

    if (this->edit_size_totals->empty()) {
      return 0;
    }
    return this->edit_size_totals->back() - this->edit_size_totals->front();
  }

  KeyView get_min_key() const noexcept
  {
    return this->result_set.get_min_key();
  }

  KeyView get_max_key() const noexcept
  {
    return this->result_set.get_max_key();
  }

  StatusOr<ValueView> find_key(const KeyView& key) const noexcept
  {
    return this->result_set.find_key(key);
  }

  SubtreeViability get_viability() noexcept;

  StatusOr<std::unique_ptr<InMemoryLeaf>> try_split() noexcept;

  StatusOr<SplitPlan> make_split_plan() const noexcept;

  Status start_serialize(TreeSerializeContext& context) noexcept;

  StatusOr<llfs::PinnedPage> finish_serialize(TreeSerializeContext& context) noexcept;
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
