#pragma once

#include <turtle_kv/tree/testing/fake_page_loader.hpp>

#include <turtle_kv/tree/active_pivots_set.hpp>

#include <turtle_kv/core/packed_key_value.hpp>

#include <turtle_kv/util/piecewise_filter.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_id.hpp>

#include <boost/range/algorithm/equal_range.hpp>

#include <map>

namespace turtle_kv {
namespace testing {

struct FakeLevel;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct FakeSegment {
  llfs::PageId page_id_;
  ActivePivotsSet128 active_pivots_ = {};
  PiecewiseFilter<u32> filter_;
  std::map<usize, usize> pivot_items_count_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<FakePinnedPage> load_leaf_page(FakePageLoader& loader,
                                          llfs::PinPageToJob pin_page_to_job,
                                          llfs::PageCacheOvercommit& overcommit) const
  {
    return loader.load_page(this->page_id_,
                            llfs::PageLoadOptions{
                                pin_page_to_job,
                                llfs::OkIfNotFound{false},
                                overcommit,
                            });
  }

  ActivePivotsSet128 get_active_pivots() const
  {
    return this->active_pivots_;
  }

  bool is_pivot_active(i32 pivot_i) const
  {
    return this->active_pivots_.get(pivot_i);
  }

  void set_pivot_active(i32 pivot_i, bool active)
  {
    this->active_pivots_.set(pivot_i, active);
  }

  void insert_active_pivot(usize pivot_i, bool is_active = true)
  {
    this->active_pivots_.insert(pivot_i, is_active);
  }

  void set_pivot_items_count(usize pivot_i, usize count)
  {
    this->set_pivot_active(pivot_i, (count > 0));
    if (count > 0) {
      this->pivot_items_count_[pivot_i] = count;
    } else {
      this->pivot_items_count_.erase(pivot_i);
    }
  }

  void set_page_id(llfs::PageId page_id)
  {
    this->page_id_ = page_id;
  }

  llfs::PageId get_leaf_page_id() const
  {
    return this->page_id_;
  }

  void clear_active_pivots()
  {
    this->active_pivots_ = {};
    this->pivot_items_count_.clear();
  }

  bool is_inactive() const
  {
    const bool inactive = this->active_pivots_.is_empty();
    if (inactive) {
      Slice<const Interval<u32>> filter_dropped_ranges = this->filter_.dropped();
      BATT_CHECK_EQ(filter_dropped_ranges.size(), 1);
      BATT_CHECK_EQ(filter_dropped_ranges[0].lower_bound, 0);
    }
    return inactive;
  }

  template <typename Traits>
  void drop_key_range(const BasicInterval<Traits>& key_range,
                      const Slice<const PackedKeyValue>& items)
  {
    drop_item_range(this->filter_, items, key_range, llfs::KeyRangeOrder{});
  }

  void drop_index_range(Interval<u32> i)
  {
    this->filter_.drop_index_range(i);
  }

  bool is_index_filtered(const FakeLevel&, u32 index) const
  {
    return !this->filter_.live_at_index(index);
  }

  bool is_unfiltered() const
  {
    return !this->filter_.dropped_total();
  }

  u32 live_lower_bound(const FakeLevel&, u32 item_i) const
  {
    return this->filter_.live_lower_bound(item_i);
  }

  Interval<u32> get_live_item_range(const FakeLevel&, Interval<u32> i) const
  {
    return this->filter_.find_live_range(i);
  }
};

}  // namespace testing
}  // namespace turtle_kv
