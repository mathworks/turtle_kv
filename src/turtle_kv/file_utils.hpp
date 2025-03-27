#pragma once

#include <turtle_kv/import/status.hpp>

#include <filesystem>

namespace turtle_kv {

Status remove_existing_path(const std::filesystem::path& path) noexcept;

}  // namespace turtle_kv
