#pragma once

#include <batteries/require.hpp>

#include <filesystem>

namespace turtle_kv {

inline batt::StatusOr<std::filesystem::path> data_root() noexcept
{
  const char* var_name = "TURTLE_KV_TEST_DIR";
  const char* var_value = std::getenv(var_name);

  if (var_value == nullptr) {
    LOG(ERROR) << "\nERROR: Environment variable '" << var_name
               << "' is not defined. Define it to resolve this error.";
    BATT_REQUIRE_NE(var_value, nullptr);
  }

  return std::filesystem::path{var_value};
}

}  // namespace turtle_kv
