#pragma once

#include <filesystem>

namespace turtle_kv {

inline std::filesystem::path data_root() noexcept
{
  return std::filesystem::path{"/mnt/kv-bakeoff/turtle_kv_Test/data_root"};
}

}  // namespace turtle_kv
