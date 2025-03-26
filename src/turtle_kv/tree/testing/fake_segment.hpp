#pragma once

#include <turtle_kv/tree/testing/fake_page_loader.hpp>

#include <turtle_kv/util/bit_ops.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_id.hpp>

#include <map>

namespace turtle_kv {
namespace testing {

struct FakeLevel;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct FakeSegment {
  llfs::PageId page_id_;
  u64 active_pivots_ = 0;
  u64 flushed_pivots_ = 0;
  std::map<usize, usize> flushed_item_upper_bound_;
  std::map<usize, usize> pivot_items_count_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<FakePinnedPage> load_leaf_page(FakePageLoader& loader,
                                          llfs::PinPageToJob pin_page_to_job) const noexcept
  {
    return loader.get_page_in_job(this->page_id_, pin_page_to_job, llfs::OkIfNotFound{false});
  }

  u64 get_active_pivots() const noexcept
  {
    return this->active_pivots_;
  }

  bool is_pivot_active(usize pivot_i) const noexcept
  {
    return get_bit(this->active_pivots_, pivot_i);
  }

  void set_pivot_active(usize pivot_i, bool active) noexcept
  {
    this->active_pivots_ = set_bit(this->active_pivots_, pivot_i, active);
  }

  void insert_active_pivot(usize pivot_i, bool is_active = true) noexcept
  {
    this->active_pivots_ = insert_bit(this->active_pivots_, pivot_i, is_active);
  }

  u64 get_flushed_pivots() const noexcept
  {
    return this->flushed_pivots_;
  }

  usize get_flushed_item_upper_bound(const FakeLevel&, usize pivot_i) const noexcept
  {
    auto iter = this->flushed_item_upper_bound_.find(pivot_i);
    if (iter == this->flushed_item_upper_bound_.end()) {
      return 0;
    }
    return iter->second;
  }

  void set_flushed_item_upper_bound(usize pivot_i, usize upper_bound) noexcept
  {
    this->flushed_pivots_ = set_bit(this->flushed_pivots_, pivot_i, (upper_bound != 0));
    if (upper_bound != 0) {
      this->flushed_item_upper_bound_[pivot_i] = upper_bound;
    } else {
      this->flushed_item_upper_bound_.erase(pivot_i);
    }
  }

  void set_pivot_items_count(usize pivot_i, usize count) noexcept
  {
    this->active_pivots_ = set_bit(this->active_pivots_, pivot_i, (count > 0));
    if (count > 0) {
      this->pivot_items_count_[pivot_i] = count;
    } else {
      this->pivot_items_count_.erase(pivot_i);
    }
  }

  void insert_flushed_item_upper_bound(usize pivot_i, usize new_upper_bound) noexcept
  {
    this->flushed_pivots_ = insert_bit(this->flushed_pivots_, pivot_i, (new_upper_bound != 0));

    std::map<usize, usize> new_flushed_item_upper_bound;
    for (const auto& [index, upper_bound] : this->flushed_item_upper_bound_) {
      if (index < pivot_i) {
        new_flushed_item_upper_bound[index] = upper_bound;
      } else if (index == pivot_i) {
        new_flushed_item_upper_bound[index] = new_upper_bound;
      } else {
        BATT_CHECK_GT(index, pivot_i);
        new_flushed_item_upper_bound[index + 1] = upper_bound;
      }
    }
    std::swap(this->flushed_item_upper_bound_, new_flushed_item_upper_bound);
  }

  void set_page_id(llfs::PageId page_id) noexcept
  {
    this->page_id_ = page_id;
  }

  void clear_active_pivots() noexcept
  {
    this->active_pivots_ = 0;
    this->pivot_items_count_.clear();
  }

  void clear_flushed_pivots() noexcept
  {
    this->flushed_pivots_ = 0;
    this->flushed_item_upper_bound_.clear();
  }
};

}  // namespace testing
}  // namespace turtle_kv
