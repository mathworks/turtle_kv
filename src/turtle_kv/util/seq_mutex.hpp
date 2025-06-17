#pragma once

#include <atomic>

namespace turtle_kv {

template <typename IntT>
class SeqMutex
{
 public:
  class ReadLock
  {
   public:
    explicit ReadLock(SeqMutex& mutex) noexcept
        : state_{mutex.state_}
        , before_state_{this->state_.load()}
    {
      while ((this->before_state_ & 3) != 0) {
        this->before_state_ = this->state_.load();
      }
    }

    [[nodiscard]] bool changed()
    {
      return this->state_.load() != this->before_state_;
    }

   private:
    std::atomic<IntT>& state_;
    IntT before_state_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class WriteLock
  {
   public:
    explicit WriteLock(SeqMutex& mutex) noexcept : state_{mutex.state_}
    {
      for (;;) {
        const IntT old_state = this->state_.fetch_or(1);
        if ((old_state & 1) == 0) {
          this->state_.fetch_add(2);
          return;
        }
      }
    }

    ~WriteLock() noexcept
    {
      this->state_.fetch_add(1);
    }

   private:
    std::atomic<IntT>& state_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SeqMutex() noexcept : state_{0}
  {
  }

  SeqMutex(const SeqMutex&) = delete;
  SeqMutex& operator=(const SeqMutex&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::atomic<IntT> state_;
};

}  // namespace turtle_kv
