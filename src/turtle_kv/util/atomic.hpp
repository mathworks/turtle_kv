#pragma once
#define TURTLE_KV_UTIL_ATOMIC_HPP

#include <atomic>

namespace turtle_kv {

/** \brief Atomically updates `value` so that it is at least `min_value`.
 * \return the previously observed value.
 */
template <typename T>
T atomic_clamp_min(std::atomic<T>& value, T min_value)
{
  T observed_value = value.load();

  for (;;) {
    if (observed_value >= min_value) {
      break;
    }
    if (value.compare_exchange_weak(observed_value, min_value)) {
      break;
    }
  }

  return observed_value;
}

/** \brief Atomically updates `value` so that it is at least `min_value`.
 * \return the previously observed value.
 */
template <typename T>
T atomic_clamp_max(std::atomic<T>& value, T max_value)
{
  T observed_value = value.load();

  for (;;) {
    if (observed_value <= max_value) {
      break;
    }
    if (value.compare_exchange_weak(observed_value, max_value)) {
      break;
    }
  }

  return observed_value;
}

}  // namespace turtle_kv
