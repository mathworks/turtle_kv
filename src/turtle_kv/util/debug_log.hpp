#pragma once

#include <batteries/async/task.hpp>

#include <array>
#include <iostream>
#include <mutex>
#include <sstream>

namespace turtle_kv {

namespace detail {

inline std::mutex& debug_log_mutex()
{
  static std::mutex m_;
  return m_;
}

}  // namespace detail

#define TURTLE_KV_DEBUG_LOG_ON(expr)                                                               \
  [&] {                                                                                            \
    std::ostringstream oss_BUT_NOT_THE_ONE_IN_YOUR_CODE;                                           \
    oss_BUT_NOT_THE_ONE_IN_YOUR_CODE << "[thread:" << ::batt::this_thread_id() << "] " << expr     \
                                     << std::endl;                                                 \
    std::unique_lock<std::mutex> lock_BUT_NOT_THE_ONE_IN_YOUR_CODE{                                \
        ::turtle_kv::detail::debug_log_mutex()};                                                   \
    std::cout << oss_BUT_NOT_THE_ONE_IN_YOUR_CODE.str() << std::flush;                             \
  }()

// #define TURTLE_KV_DEBUG_LOG_ENABLE

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#ifdef TURTLE_KV_DEBUG_LOG_ENABLE

#define TURTLE_KV_DEBUG_LOG TURTLE_KV_DEBUG_LOG_ON

#else  //+++++++++++-+-+--+----- --- -- -  -  -   -

#define TURTLE_KV_DEBUG_LOG(expr)                                                                  \
  if (false)                                                                                       \
  std::cout << ""

#endif
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct HexBinaryView {
  std::string_view str;
};

std::ostream& operator<<(std::ostream& out, const HexBinaryView& t) noexcept;

}  // namespace turtle_kv
