//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_UTIL_SMALL_QUEUE_HPP

#include <turtle_kv/util/movable.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/utility.hpp>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
class SmallQueueBase
{
 public:
  using VecBase = SmallVecBase<T>;
  using iterator = typename VecBase::iterator;
  using const_iterator = typename VecBase::const_iterator;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void clear()
  {
    this->vec_.clear();
    this->front_i_ = 0;
  }

  usize size() const
  {
    return this->vec_.size() - this->front_i_;
  }

  const T* data() const noexcept
  {
    return this->vec_.data() + this->front_i_;
  }

  bool empty() const
  {
    return this->size() == 0;
  }

  iterator begin()
  {
    return std::next(this->vec_.begin(), this->front_i_);
  }

  iterator end()
  {
    return this->vec_.end();
  }

  const_iterator begin() const
  {
    return std::next(this->vec_.begin(), this->front_i_);
  }

  const_iterator end() const
  {
    return this->vec_.end();
  }

  T& front()
  {
    return this->vec_[this->front_i_];
  }

  T& back()
  {
    return this->vec_.back();
  }

  const T& front() const
  {
    return this->vec_[this->front_i_];
  }

  const T& back() const
  {
    return this->vec_.back();
  }

  void pop_front()
  {
    BATT_CHECK_LT(this->front_i_, this->vec_.size());
    ++this->front_i_;
    this->try_compact();
  }

  template <typename A>
  void push_back(A&& arg)
  {
    this->vec_.push_back(BATT_FORWARD(arg));
  }

  template <typename... A>
  decltype(auto) emplace_back(A&&... args)
  {
    return this->vec_.emplace_back(BATT_FORWARD(args)...);
  }

  bool try_compact()
  {
    if (this->front_i_ * 2 >= this->vec_.size()) {
      this->vec_.erase(this->vec_.begin(), std::next(this->vec_.begin, this->front_i_));
      this->front_i_ = 0;
      return true;
    }
    return false;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  explicit SmallQueueBase(VecBase& vec) noexcept : vec_{vec}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  VecBase& vec_;
  Movable<usize> front_i_{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T, usize kStaticSize>
class SmallQueueStorage
{
 public:
  using Vec = SmallVec<T, kStaticSize>;

  Vec vec_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T, usize kStaticSize>
class SmallQueue
    : private SmallQueueStorage<T, kStaticSize>
    , public SmallQueueBase<T>
{
 public:
  using Self = SmallQueue;
  using Super = SmallQueueBase<T>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SmallQueue() : Super{this->SmallQueueStorage<T, kStaticSize>::vec_}
  {
  }

  SmallQueue(const SmallQueue&) = default;
  SmallQueue& operator=(const SmallQueue&) = default;

  SmallQueue(SmallQueue&&) = default;
  SmallQueue& operator=(SmallQueue&&) = default;
};

}  // namespace turtle_kv
