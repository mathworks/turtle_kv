#pragma once

#include <turtle_kv/import/env.hpp>
#include <turtle_kv/import/logging.hpp>

namespace turtle_kv {

template <typename Param, typename T = typename Param::value_type>
inline typename Param::value_type getenv_param() noexcept
{
  static const T value_ = [] {
    T value = getenv_as<bool>(Param::get_name()).value_or(Param::get_default_value());
    LOG(INFO) << Param::get_name() << "=" << value;
    return value;
  }();

  return value_;
}

#define TURTLE_KV_ENV_PARAM(type, name, default_value)                                             \
  struct name {                                                                                    \
    using value_type = type;                                                                       \
    static const char* get_name() noexcept                                                         \
    {                                                                                              \
      return #name;                                                                                \
    }                                                                                              \
    static type get_default_value() noexcept                                                       \
    {                                                                                              \
      return default_value;                                                                        \
    }                                                                                              \
  }

}  // namespace turtle_kv
