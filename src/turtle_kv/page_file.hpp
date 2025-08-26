#pragma once

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_id_factory.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/volume.hpp>

#include <filesystem>

namespace turtle_kv {

struct PageFileSpec {
  std::filesystem::path filename;
  llfs::PageCount initial_page_count;
  Optional<llfs::PageCount> max_page_count = None;
  llfs::PageSize page_size;
};

Status create_page_file(llfs::StorageContext& storage_context,                   //
                        const PageFileSpec& spec,                                //
                        RemoveExisting remove_existing = RemoveExisting{false},  //
                        Optional<llfs::page_device_id_int> device_id = None      //
                        ) noexcept;

inline Status create_page_file(llfs::StorageContext& storage_context,                   //
                               const std::filesystem::path& filename,                   //
                               llfs::PageCount max_page_count,                          //
                               llfs::PageSize page_size,                                //
                               RemoveExisting remove_existing = RemoveExisting{false},  //
                               Optional<llfs::page_device_id_int> device_id = None      //
                               ) noexcept
{
  return create_page_file(storage_context,  //
                          PageFileSpec{
                              .filename = filename,
                              .initial_page_count = llfs::PageCount{0},
                              .max_page_count = max_page_count,
                              .page_size = page_size,
                          },                //
                          remove_existing,  //
                          device_id);
}

}  // namespace turtle_kv
