#pragma once
#define TURTLE_KV_UTIL_ART_MUTEX_HPP

#define ART_USE_ABSEIL_MUTEX 1
#define ART_USE_STD_MUTEX 0

#if ART_USE_ABSEIL_MUTEX
#include <absl/synchronization/mutex.h>
#endif

#if ART_USE_STD_MUTEX
#include <mutex>
#endif

namespace turtle_kv {

#if ART_USE_ABSEIL_MUTEX
using ARTMutex = absl::Mutex;
using ARTMutexLock = absl::MutexLock;
#endif

#if ART_USE_STD_MUTEX
using ARTMutex = std::mutex;
using ARTMutexLock = std::unique_lock<std::mutex>;
#endif

}  // namespace turtle_kv
