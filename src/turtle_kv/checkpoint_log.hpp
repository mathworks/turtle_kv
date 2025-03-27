#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/status.hpp>

#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <filesystem>

namespace turtle_kv {

Status create_checkpoint_log(llfs::StorageContext& storage_context,
                             const TreeOptions& tree_options,
                             const std::filesystem::path& file_name) noexcept;

StatusOr<std::unique_ptr<llfs::Volume>> open_checkpoint_log(
    llfs::StorageContext& storage_context,
    const std::filesystem::path& file_name) noexcept;

}  // namespace turtle_kv
