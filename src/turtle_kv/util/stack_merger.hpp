#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/utility.hpp>

#include <glog/logging.h>

#include <algorithm>
#include <array>

namespace turtle_kv {

namespace detail {

template <typename T>
struct DefaultStackMergerCompare {
  bool operator()(T* left, T* right) const
  {
    return *left < *right;
  }
};

}  // namespace detail

template <typename T,
          typename CompareFn = detail::DefaultStackMergerCompare<T>,
          usize kStaticSize = 64>
class StackMerger
{
 public:
  using Self = StackMerger;

  using ItemRef = T*;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the index of the parent of `child_i`.
   */
  BATT_ALWAYS_INLINE static usize get_parent(usize child_i)
  {
    return (child_i - 1) / 3;
  }

  /** \brief Returns the index of the `branch_k`-th child of node `parent_i`.
   *
   * `branch_k` must be one of {1, 2, 3}
   */
  BATT_ALWAYS_INLINE static usize get_child(usize parent_i, usize branch_k)
  {
    return parent_i * 3 + branch_k;
  }

  /** \brief Converts a pointer to T to an ItemRef.
   */
  BATT_ALWAYS_INLINE static ItemRef from_pointer(T* ptr)
  {
    return ptr;
  }

  /** \brief Retrieves the original pointer from an ItemRef.
   */
  BATT_ALWAYS_INLINE static T* get_pointer(ItemRef iref)
  {
    return iref;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit StackMerger(usize capacity = 0) noexcept
  {
    this->reserve(capacity);
  }

  explicit StackMerger(const Slice<T>& items) noexcept
  {
    this->initialize(items, /*minimum_capacity=*/0);
  }

  ~StackMerger() noexcept
  {
    this->release_storage();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset(const Slice<T>& items, usize minimum_capacity = 0)
  {
    this->release_storage();
    this->initialize(items, minimum_capacity);
  }

  /** \brief Returns true iff the StackMerger is empty.
   */
  BATT_ALWAYS_INLINE bool empty() const
  {
    return this->begin_ == this->end_;
  }

  /** \brief Returns the number of items remaining.
   */
  BATT_ALWAYS_INLINE usize size() const
  {
    return this->end_ - this->begin_;
  }

  /** \brief Returns a pointer to the current least element (min-heap order).
   */
  BATT_ALWAYS_INLINE T* first() const
  {
    return Self::get_pointer(*this->begin_);
  }

  /** \brief Notifies the StackMerger that the top's priority has changed; causes the top element to
   * be sifted into the new correct position.
   */
  void update_first()
  {
    this->sift_down(0);
  }

  /** \brief Removes the top element from the StackMerger.
   */
  void remove_first()
  {
    if (this->empty()) {
      return;
    }

    --this->end_;
    std::swap(*this->begin_, *this->end_);

    this->sift_down(0);
  }

  /** \brief Inserts a new stack item.
   */
  void insert(T* item)
  {
    const usize child_i = this->size();

    *this->end_ = Self::from_pointer(item);
    ++this->end_;

    this->sift_up(child_i);
  }

  /** \brief Returns a function which prints a human-friendly representation of this object.
   */
  auto dump_items() const
  {
    return [this](std::ostream& out) {
      out << "{" << std::endl;
      usize i = 0;
      for (ItemRef* ptr = this->begin_; ptr != this->end_; ++ptr) {
        out << "    " << i << ": " << batt::make_printable(*Self::get_pointer(*ptr)) << " @"
            << (void*)Self::get_pointer(*ptr) << "," << std::endl;
        ++i;
      }
      out << "}";
    };
  }

  /** \brief Panics if heap invariants do not hold for the internal state of this object.
   */
  void check_invariants()
  {
    const usize n_items = this->size();

    for (usize child_i = n_items; child_i > 1;) {
      --child_i;
      const usize parent_i = Self::get_parent(child_i);

      BATT_CHECK_LT(parent_i, n_items);
      BATT_CHECK_LT(child_i, n_items);
      BATT_CHECK(this->compare(this->begin_[parent_i], this->begin_[child_i]))
          << BATT_INSPECT(parent_i) << BATT_INSPECT(child_i) << " " << this->dump_items();
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void initialize(const Slice<T>& items, usize minimum_capacity)
  {
    const usize n_items = items.size();
    this->reserve(std::max(minimum_capacity, n_items));

    // Initialize item pointers.
    //
    for (usize i = 0; i < n_items; ++i) {
      this->begin_[i] = Self::from_pointer(&items[i]);
    }
    this->end_ = this->begin_ + n_items;

    // Make heap.
    //
    const usize end_i = (n_items + 1) / 3;
    for (usize i = end_i; i > 0;) {
      --i;
      this->sift_down(i);
    }
  }

  void release_storage()
  {
    if (this->begin_ != this->static_array_.data()) {
      delete[] this->begin_;
    }
  }

  /** \brief Initializes internal storage.
   */
  void reserve(usize capacity)
  {
    static_assert(sizeof(T) >= 8);
    static_assert(alignof(T) >= 8);

    // Allocate storage.
    //
    if (capacity > this->static_array_.size()) {
      this->begin_ = new ItemRef[capacity];
    } else {
      this->begin_ = this->static_array_.data();
    }
  }

  /** \brief Returns true iff the left argument comes before the right in min-heap order.
   */
  BATT_ALWAYS_INLINE bool compare(ItemRef left_iptr, ItemRef right_iptr) const
  {
    return this->compare_(Self::get_pointer(left_iptr), Self::get_pointer(right_iptr));
  }

  /** \brief Restores the heap invariant, given that `parent_i`'s priority may have been reduced.
   */
  void sift_down(usize parent_i)
  {
    const usize n_items = this->size();
    for (;;) {
      const usize child1_i = Self::get_child(parent_i, 1);
      if (child1_i >= n_items) {
        return;
      }

      ItemRef& parent = this->begin_[parent_i];
      ItemRef& child1 = this->begin_[child1_i];

      ItemRef* p_min = &parent;
      usize i_min = parent_i;

      if (this->compare(child1, *p_min)) {
        p_min = &child1;
        i_min = child1_i;
      }

      const usize child2_i = child1_i + 1;
      if (child2_i < n_items) {
        ItemRef& child2 = this->begin_[child2_i];
        if (this->compare(child2, *p_min)) {
          p_min = &child2;
          i_min = child2_i;
        }

        const usize child3_i = child2_i + 1;
        if (child3_i < n_items) {
          ItemRef& child3 = this->begin_[child3_i];
          if (this->compare(child3, *p_min)) {
            p_min = &child3;
            i_min = child3_i;
          }
        }
      }

      if (i_min == parent_i) {
        break;
      }

      std::swap(parent, *p_min);
      parent_i = i_min;
    }
  }

  void sift_up(usize child_i)
  {
    DVLOG(1) << "sift_up(" << child_i << ")";

    if (child_i == 0) {
      return;
    }

    ItemRef* child = &(this->begin_[child_i]);

    for (;;) {
      const usize parent_i = Self::get_parent(child_i);
      ItemRef& parent = this->begin_[parent_i];

      DVLOG(1) << BATT_INSPECT(parent_i);

      // child's siblings are known to be not-less-than parent, by the heap invariant; so, we only
      // need to compare with parent and swap if they are out of order.
      //
      if (this->compare(parent, *child)) {
        DVLOG(1) << "parent is less than child; stopping";
        break;
      }

      std::swap(parent, *child);

      if (parent_i == 0) {
        break;
      }
      child = &parent;
      child_i = parent_i;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ItemRef* begin_;
  ItemRef* end_;
  CompareFn compare_;
  std::array<ItemRef, kStaticSize> static_array_;
};

}  // namespace turtle_kv
