#pragma once

#include <turtle_kv/tree/key_query.hpp>

#include <turtle_kv/core/algo/tuning_defaults.hpp>
#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/algo/parallel_running_total.hpp>
#include <batteries/assert.hpp>

#include <algorithm>
#include <iterator>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename NodeT>
struct NodeAlgorithms {
  NodeT& node_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit NodeAlgorithms(NodeT& node) noexcept : node_{node}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the half-open key interval for the given pivot.
   */
  Interval<KeyView> get_pivot_key_range(usize pivot_i) const
  {
    return Interval<KeyView>{
        this->node_.get_pivot_key(pivot_i),
        this->node_.get_pivot_key(pivot_i + 1),
    };
  }

  /** \brief Returns the index of the pivot containing the given key.
   */
  usize find_pivot_containing(const KeyView& key) const
  {
    // pivot_keys:     k0-----k1-----k2-----k3 ... kN
    //                  |  0   |  1   |  2   |  3   |
    //                  +------+------+------+------+
    //                     ^     ^          ^
    //                     |     |          |
    //            +--------+     |          |
    //            |+-------------+          |
    //            ||+-----------------------+
    //            |||
    // where does key go?
    //
    auto pivot_keys = this->node_.get_pivot_keys();

    BATT_CHECK_GE(pivot_keys.size(), 2u);

    pivot_keys.drop_front();
    pivot_keys.drop_back();

    const auto first = pivot_keys.begin();

    return std::distance(first, std::upper_bound(first, pivot_keys.end(), key, KeyOrder{}));
  }

  /** \brief Returns the minimal closed key interval that completely contains the given closed key
   * interval.
   */
  CInterval<usize> find_pivot_crange_containing(const CInterval<KeyView>& key_crange) const
  {
    const usize lower_pivot = this->find_pivot_containing(this->node_, key_crange.lower_bound);
    const usize upper_pivot = this->find_pivot_containing(this->node_, key_crange.upper_bound);

    return CInterval<usize>{lower_pivot, upper_pivot};
  }

  /** \brief Returns the minimal half-open key interval that completely contains the given half-open
   * key interval.
   */
  Interval<usize> find_pivot_range_containing(const Interval<KeyView>& key_range) const
  {
    usize lower_pivot = this->find_pivot_containing(this->node_, key_range.lower_bound);
    usize upper_pivot = this->find_pivot_containing(this->node_, key_range.upper_bound);

    // Check to see whether there may be keys in the range from pivot_keys_[upper_pivot] to
    // key_range.upper_bound; if so, then bump `upper_pivot` by 1 to include these keys.
    //
    if (upper_pivot < this->node_.pivot_count() &&
        this->node_.get_pivot_key(upper_pivot) < key_range.upper_bound) {
      upper_pivot += 1;
    }

    return Interval<usize>{lower_pivot, upper_pivot};
  }

  /** \brief Scans the passed batch to find the total byte size in the range of each pivot; this is
   * used to update the node.
   */
  template <typename Edits, typename SizeOfEditFn /*= usize(const EditView&)*/>
  void update_pending_bytes(batt::WorkerPool& worker_pool,
                            const Edits& edits,
                            const SizeOfEditFn& size_of_edit)
  {
    const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();

    const batt::WorkSliceParams running_total_params{
        .min_task_size = algo_defaults.running_total_edit_size.min_task_size,
        .max_tasks = batt::TaskCount{worker_pool.size() + /*this_thread*/ 1},
    };

    worker_pool.reset();

    auto edits_begin = edits.begin();
    auto edits_end = edits.end();

    // First compute prefix sum of the edits.
    //
    batt::RunningTotal total_item_bytes = batt::parallel_running_total(worker_pool,
                                                                       edits_begin,
                                                                       edits_end,
                                                                       size_of_edit,
                                                                       running_total_params);

    // Now find the lower_bound position of each pivot key in the prefix sum and subtract the
    // difference to get bytes per pivot.
    //
    usize pivot_edits_begin_i = std::distance(
        edits_begin,
        std::lower_bound(edits_begin, edits_end, this->node_.get_pivot_key(0), KeyOrder{}));

    const usize pivot_count = this->node_.pivot_count();
    for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
      const usize pivot_edits_end_i =
          std::distance(edits_begin,
                        std::lower_bound(edits_begin,
                                         edits_end,
                                         this->node_.get_pivot_key(pivot_i + 1),
                                         KeyOrder{}));

      this->node_.add_pending_bytes(
          pivot_i,
          total_item_bytes[pivot_edits_end_i] - total_item_bytes[pivot_edits_begin_i]);

      pivot_edits_begin_i = pivot_edits_end_i;
    }
  }

  /** \brief Executes a point query, using page filters to skip leaf pages where the key is known
   * not to be.
   */
  StatusOr<ValueView> find_key(KeyQuery& query)
  {
    Optional<ValueView> value;

    const i32 key_pivot_i = this->find_pivot_containing(query.key());
    const usize level_count = this->node_.get_level_count();

    for (usize level_i = 0; level_i < level_count; ++level_i) {
      StatusOr<ValueView> found_in_level =
          this->node_.find_key_in_level(level_i, query, key_pivot_i);

      BATT_ASSIGN_OK_RESULT(const bool done, combine_in_place(&value, found_in_level));
      if (done) {
        BATT_CHECK(value);
        return *value;
      }
    }

    StatusOr<ValueView> subtree_result = this->node_  //
                                             .get_child(key_pivot_i)
                                             .find_key(ParentNodeHeight{this->node_.height}, query);

    BATT_REQUIRE_OK(combine_in_place(&value, subtree_result));

    if (!value) {
      return {batt::StatusCode::kNotFound};
    }

    return {std::move(*value)};
  }

  /** \brief Splits the given level at the given key, placing the lower and upper halves in
   * `lower_half_level` and `upper_half_level` respectively.
   */
  template <typename LevelCaseT, typename LevelVariantT>
  void split_level(const LevelCaseT& whole_level,
                   i32 split_pivot_i,
                   LevelVariantT& lower_half_level,
                   LevelVariantT& upper_half_level)
  {
    const KeyView split_pivot_key = this->node_.get_pivot_key(split_pivot_i);

    LevelCaseT& lower_impl = lower_half_level.template emplace<LevelCaseT>(whole_level);
    LevelCaseT& upper_impl = upper_half_level.template emplace<LevelCaseT>(whole_level);

    lower_impl.drop_after_pivot(split_pivot_i, split_pivot_key);
    upper_impl.drop_before_pivot(split_pivot_i, split_pivot_key);
  }
};

template <typename NodeT>
NodeAlgorithms<NodeT> in_node(NodeT& node)
{
  return NodeAlgorithms<NodeT>{node};
}

}  // namespace turtle_kv
