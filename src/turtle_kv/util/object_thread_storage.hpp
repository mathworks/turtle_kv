#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <batteries/assert.hpp>
#include <batteries/hint.hpp>

#include <memory>
#include <vector>

namespace turtle_kv {

template <typename T>
class ObjectThreadStorage
{
 public:
  using Self = ObjectThreadStorage;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self& instance()
  {
    static Self* instance_ = new Self{};
    return *instance_;
  }

  template <typename... Args>
  static T& get(usize slot_i, Args&&... args)
  {
    thread_local std::vector<std::unique_ptr<T>> slots_;

    thread_local bool initialized_ = [&] {
      Self& self = Self::instance();
      absl::MutexLock lock{&self.mutex_};
      auto [iter, inserted] = self.thread_states_.emplace(std::addressof(slots_));
      BATT_CHECK(inserted);
      return true;
    }();

    thread_local auto on_scope_exit = batt::finally([&] {
      Self& self = Self::instance();
      absl::MutexLock lock{&self.mutex_};
      self.thread_states_.erase(std::addressof(slots_));
    });

    BATT_ASSERT(initialized_);

    if (BATT_HINT_FALSE(slots_.size() <= slot_i)) {
      slots_.resize(slot_i + 1);
    }
    std::unique_ptr<T>& state = slots_[slot_i];

    if (!state) {
      state = std::make_unique<T>(BATT_FORWARD(args)...);
    }
    return *state;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class ScopedSlot;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize alloc_slot()
  {
    absl::MutexLock lock{&this->mutex_};

    if (!this->slot_pool_.empty()) {
      const usize slot_i = this->slot_pool_.back();
      this->slot_pool_.pop_back();
      return slot_i;
    }

    const usize new_slot_i = this->total_slots_created_;
    ++this->total_slots_created_;
    return new_slot_i;
  }

  void free_slot(usize slot_i)
  {
    absl::MutexLock lock{&this->mutex_};

    for (std::vector<std::unique_ptr<T>>* per_thread : this->thread_states_) {
      BATT_CHECK_NOT_NULLPTR(per_thread);
      if (slot_i < per_thread->size()) {
        (*per_thread)[slot_i] = nullptr;
      }
    }

    this->slot_pool_.emplace_back(slot_i);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  absl::Mutex mutex_;
  std::vector<usize> slot_pool_;
  usize total_slots_created_ = 0;
  absl::flat_hash_set<std::vector<std::unique_ptr<T>>*> thread_states_;
};

template <typename T>
class ObjectThreadStorage<T>::ScopedSlot
{
 public:
  explicit ScopedSlot() noexcept : slot_i_{ObjectThreadStorage<T>::instance().alloc_slot()}
  {
  }

  ~ScopedSlot() noexcept
  {
    ObjectThreadStorage<T>::instance().free_slot(this->slot_i_);
  }

  ScopedSlot(const ScopedSlot&) = delete;
  ScopedSlot& operator=(const ScopedSlot&) = delete;

  template <typename... Args>
  T& get(Args&&... args)
  {
    return ObjectThreadStorage<T>::get(this->slot_i_, BATT_FORWARD(args)...);
  }

 private:
  usize slot_i_;
};

}  // namespace turtle_kv
