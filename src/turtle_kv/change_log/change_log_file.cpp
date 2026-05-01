#include <turtle_kv/change_log/change_log_file.hpp>
//

#include <turtle_kv/import/constants.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status ChangeLogFile::create(const std::filesystem::path& path,  //
                                        const Config& config,               //
                                        RemoveExisting remove_existing) noexcept
{
  if (remove_existing) {
    BATT_REQUIRE_OK(remove_existing_path(path));
  }

  static_assert(sizeof(PackedMetaBlock) == 4096);

  BATT_CHECK_GE(config.block0_offset, 4096) << "block0 must not overlap the 4k meta block!";

  StatusOr<int> fd = llfs::create_file_read_write(path.string(), llfs::OpenForAppend{false});
  BATT_REQUIRE_OK(fd);

  auto on_scope_exit = batt::finally([fd] {
    llfs::close_fd(*fd).IgnoreError();
  });

  const u64 file_size = config.block0_offset + (config.block_size * config.block_count);

  BATT_REQUIRE_OK(llfs::truncate_fd(*fd, file_size));

  PackedMetaBlock meta_block;

  config.pack_to(&meta_block.config);

  ChangeLogMetaState::with_initial_values().pack_to(&meta_block.meta_state);

  BATT_REQUIRE_OK(llfs::write_fd(*fd,
                                 ConstBuffer{
                                     &meta_block,
                                     sizeof(PackedMetaBlock),
                                 },
                                 /*offset=*/kMetaBlockOffset));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<ChangeLogFile>> ChangeLogFile::open(
    const std::filesystem::path& path) noexcept
{
  StatusOr<llfs::ScopedIoRing> new_io_ring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{1});

  BATT_REQUIRE_OK(new_io_ring);

  auto io_ring = std::make_unique<llfs::ScopedIoRing>(std::move(*new_io_ring));

  StatusOr<int> fd =
      llfs::open_file_read_write(path.string(), llfs::OpenForAppend{false}, llfs::OpenRawIO{true});

  PackedMetaBlock meta_block;

  BATT_REQUIRE_OK(llfs::read_fd(*fd,
                                MutableBuffer{
                                    &meta_block,
                                    sizeof(PackedMetaBlock),
                                },
                                /*offset=*/kMetaBlockOffset));

  if (meta_block.config.magic != PackedConfig::kMagic) {
    LOG(ERROR) << "Magic number at start of config block is incorrect; possible data corruption "
                  "or incorrect file type";
    return {batt::StatusCode::kDataLoss};
  }

  Config config = meta_block.config.unpack();

  BATT_ASSIGN_OK_RESULT(llfs::IoRing::File file,
                        llfs::IoRing::File::open(io_ring->get_io_ring(), fd));

  return {std::make_unique<ChangeLogFile>(std::move(io_ring), std::move(file), config)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogFile::ChangeLogFile(std::unique_ptr<llfs::ScopedIoRing>&& io_ring,
                                          llfs::IoRing::File&& file,
                                          const Config& config) noexcept
    : io_ring_{std::move(io_ring)}
    , file_{std::move(file)}
    , config_{config}
{
  BATT_CHECK_EQ(this->config_.block_size & 4095, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogFile::~ChangeLogFile() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogFile::read_meta_block(PackedMetaBlock& meta_block) const noexcept
{
  static_assert(alignof(meta_block) == 4096);
  static_assert(sizeof(meta_block) == 4096);

  return this->file_.read_all(/*offset=*/kMetaBlockOffset,
                              MutableBuffer{
                                  &meta_block,
                                  sizeof(PackedMetaBlock),
                              });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogFile::write_meta_block(const PackedMetaBlock& meta_block) const noexcept
{
  static_assert(alignof(meta_block) == 4096);
  static_assert(sizeof(meta_block) == 4096);

  BATT_CHECK_EQ(meta_block.config.unpack(), this->config());

  return this->file_.write_all(/*offset=*/kMetaBlockOffset,
                               ConstBuffer{
                                   &meta_block,
                                   sizeof(PackedMetaBlock),
                               });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>>
ChangeLogFile::read_blocks_into_vector()
{
  // TODO: [Gabe Bornstein 4/29/26] Consider adding optional parameter that could denote which block
  // to start reading from, and which block to stop reading from. We only need to read the potential
  // active range, denoted in ChangeLogFile::Config.
  //
  std::vector<boost::intrusive_ptr<ChangeLogBlock>> blocks;
  batt::Status read_blocks_status =
      this->read_blocks([&](boost::intrusive_ptr<ChangeLogBlock> block) -> batt::Status {
        BATT_CHECK_EQ(block->ref_count(), 1);

        blocks.push_back(block);

        VLOG(3) << "ChangeLogBlock->block_size() == " << blocks.back()->block_size()
                << " offset() == " << blocks.back()->edit_offset_lower_bound();

        return batt::OkStatus();
      });

  BATT_REQUIRE_OK(read_blocks_status);
  return blocks;
}

}  // namespace turtle_kv
