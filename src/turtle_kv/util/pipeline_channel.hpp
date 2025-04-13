#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/assert.hpp>
#include <batteries/async/futex.hpp>

#include <atomic>

namespace turtle_kv {

template <typename T>
class PipelineChannel
{
 public:
  static constexpr u32 kStateEmpty = 0;
  static constexpr u32 kStateWriting = 1;
  static constexpr u32 kStateFull = 2;
  static constexpr u32 kStateReading = 3;
  static constexpr u32 kStateClosed = 4;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename... Args>
  Status write(Args&&... args) noexcept
  {
    u32 observed_state = this->state_.load();

    for (;;) {
      switch (observed_state) {
        case kStateEmpty:
          if (this->state_.compare_exchange_weak(observed_state, kStateWriting)) {
            this->value_.emplace(BATT_FORWARD(args)...);
            BATT_CHECK_EQ(this->state_.exchange(kStateFull), kStateWriting);
            return this->notify();
          }
          break;

        case kStateWriting:  // fall-through
        case kStateFull:     // fall-through
        case kStateReading:
          // Wait for the reader to catch up...
          //
          batt::spin_yield();
          observed_state = this->state_.load();
          break;

        case kStateClosed:
          return batt::StatusCode::kClosed;

        default:
          BATT_PANIC() << "Invalid state!";
          BATT_UNREACHABLE();
      }
    }
  }

  StatusOr<T> read() noexcept
  {
    u32 observed_state = this->state_.load();

    for (;;) {
      switch (observed_state) {
        case kStateEmpty: {
          BATT_REQUIRE_OK(this->wait(kStateEmpty));
          observed_state = this->state_.load();
          break;
        }
        case kStateWriting:
          batt::spin_yield();
          observed_state = this->state_.load();
          break;

        case kStateFull:
          if (this->state_.compare_exchange_weak(observed_state, kStateReading)) {
            StatusOr<T> read_value{std::move(*this->value_)};
            this->value_ = None;
            BATT_CHECK_EQ(this->state_.exchange(kStateEmpty), kStateReading);
            return read_value;
          }
          break;

        case kStateReading:
          BATT_PANIC() << "The reader thread should only try to read one value at a time!";
          BATT_UNREACHABLE();
          break;

        case kStateClosed:
          return {batt::StatusCode::kClosed};

        default:
          BATT_PANIC() << "Invalid state!";
          BATT_UNREACHABLE();
      }
    }
  }

  void close() noexcept
  {
    u32 observed_state = this->state_.load();

    for (;;) {
      switch (observed_state) {
        case kStateClosed:
          return;

        case kStateWriting:  // fall-through
        case kStateReading:
          batt::spin_yield();
          observed_state = this->state_.load();
          break;

        case kStateEmpty:  // fall-through
        case kStateFull:
          if (this->state_.compare_exchange_weak(observed_state, kStateClosed)) {
            this->notify().IgnoreError();
            this->notify().IgnoreError();
            return;
          }
          break;

        default:
          BATT_PANIC() << "Invalid state!";
          BATT_UNREACHABLE();
      }
    }
  }

 private:
  Status wait(u32 last_seen)
  {
    const i32 retval = batt::futex_wait(&this->state_, last_seen);
    if (retval != 0 && errno == EAGAIN) {
      return OkStatus();
    }
    return batt::status_from_retval(retval);
  }

  Status notify()
  {
    return batt::status_from_retval(batt::futex_notify(&this->state_));
  }

  std::atomic<u32> state_;
  Optional<T> value_;
};

}  // namespace turtle_kv
