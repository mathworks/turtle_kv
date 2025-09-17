#include <turtle_kv/page_file.hpp>
//

#include <turtle_kv/import/optional.hpp>

#include <llfs/page_arena_config.hpp>
#include <llfs/page_device_config.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status create_page_file(llfs::StorageContext& storage_context,
                        const PageFileSpec& spec,
                        RemoveExisting remove_existing,
                        Optional<llfs::page_device_id_int> device_id) noexcept
{
  std::error_code ec;
  if (std::filesystem::exists(spec.filename, ec) && !ec) {
    VLOG(1) << "file exists: " << spec.filename;
    if (!remove_existing) {
      return batt::status_from_errno(EEXIST);
    }
    std::filesystem::remove_all(spec.filename, ec);
  } else {
    VLOG(1) << "file does not exist: " << spec.filename << BATT_INSPECT(ec);
  }
  BATT_REQUIRE_OK(ec);

  llfs::PageSizeLog2 page_size_log2 = log2_ceil(spec.page_size);

  return storage_context.add_new_file(
      spec.filename.string(),
      [&](llfs::StorageFileBuilder& builder) -> Status  //
      {
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> arena_config =
            builder.add_object(llfs::PageArenaConfigOptions{
                .uuid = None,
                .page_allocator =
                    llfs::CreateNewPageAllocator{
                        .options =
                            llfs::PageAllocatorConfigOptions{
                                .uuid = None,
                                /* TODO [tastolfi 2022-07-25] add config option */
                                .max_attachments = 32,
                                //  TODO: [Gabe Bornstein 7/14/25] Is there a better way to document
                                //    .page_count should be max_page_count when using Dynamic
                                //    Storage Provisioning (i.e. last_in_file = true for
                                //    PageDevice)? Maybe rename .page_count to initial vs.
                                //    max_page_count.
                                .page_count = spec.max_page_count.value_or(spec.initial_page_count),
                                .log_device =
                                    llfs::CreateNewLogDevice2WithDefaultSize{
                                        .uuid = None,
                                        .device_page_size_log2 = None,
                                        .data_alignment_log2 = None,
                                    },
                                .page_size_log2 = page_size_log2,
                                .page_device = llfs::LinkToNewPageDevice{},
                            },
                    },
                .page_device =
                    llfs::CreateNewPageDevice{
                        .options =
                            llfs::PageDeviceConfigOptions{
                                .uuid = None,
                                .device_id = device_id,
                                .page_count = spec.initial_page_count,
                                .max_page_count = spec.max_page_count,
                                .page_size_log2 = page_size_log2,
                                .last_in_file = true,
                            },
                    },
            });

        BATT_REQUIRE_OK(arena_config);

        return OkStatus();
      });
}

}  // namespace turtle_kv
