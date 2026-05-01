#include <turtle_kv/change_log/change_log_file.hpp>
//

#include <turtle_kv/import/constants.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto ChangeLogFile::Config::with_default_values() noexcept -> Config
{
  Config config;

  config.block_size = BlockSize{ChangeLogFile::kDefaultBlockSize};
  config.block_count = BlockCount{ChangeLogFile::kDefaultLogSize / config.block_size};
  config.block0_offset = FileOffset{ChangeLogFile::kDefaultBlock0Offset};
  config.lower_bound = 0;
  config.upper_bound = 0;

  return config;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::Config::pack_to(PackedConfig* packed_config) const noexcept
{
  std::memset(packed_config, 0, sizeof(PackedConfig));

  packed_config->magic = PackedConfig::kMagic;
  packed_config->block_size = this->block_size;
  packed_config->block_count = this->block_count;
  packed_config->block0_offset = this->block0_offset;
  packed_config->active_blocks_lower_bound = this->active_block_range.lower_bound;
  packed_config->active_blocks_upper_bound = this->active_block_range.upper_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogFile::PackedConfig::unpack() const noexcept -> ChangeLogFile::Config
{
  BATT_CHECK_EQ(this->magic, PackedConfig::kMagic);

  Config config;

  config.block_size = BlockSize{this->block_size.value()};
  config.block_count = BlockCount{this->block_count.value()};
  config.block0_offset = FileOffset{this->block0_offset.value()};
  config.active_block_range.lower_bound = BlockIndex{this->active_blocks_lower_bound};
  config.active_block_range.upper_bound = BlockIndex{this->active_blocks_upper_bound};
  config.trim_edit_offset = EditOffset{this->trim_edit_offset.value()};

  config.check_invariants(config.active_block_range);

  return config;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ Status ChangeLogFile::create(const std::filesystem::path& path,  //
                                        const Config& config,               //
                                        RemoveExisting remove_existing) noexcept
{
  if (remove_existing) {
    BATT_REQUIRE_OK(remove_existing_path(path));
  }

  static_assert(sizeof(PackedConfig) == 4096);

  BATT_CHECK_GE(config.block0_offset, 4096) << "block0 must not overlap the 4k config block!";

  StatusOr<int> fd = llfs::create_file_read_write(path.string(), llfs::OpenForAppend{false});
  BATT_REQUIRE_OK(fd);

  auto on_scope_exit = batt::finally([fd] {
    llfs::close_fd(*fd).IgnoreError();
  });

  const u64 file_size = config.block0_offset + (config.block_size * config.block_count);

  BATT_REQUIRE_OK(llfs::truncate_fd(*fd, file_size));

  PackedConfig packed_config;
  config.pack_to(&packed_config);

  BATT_REQUIRE_OK(llfs::write_fd(*fd,
                                 ConstBuffer{
                                     &packed_config,
                                     sizeof(PackedConfig),
                                 },
                                 /*offset=*/0));

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

  PackedConfig packed_config;

  BATT_REQUIRE_OK(llfs::read_fd(*fd,
                                MutableBuffer{
                                    &packed_config,
                                    sizeof(PackedConfig),
                                },
                                /*offset=*/0));

  if (packed_config.magic != PackedConfig::kMagic) {
    LOG(ERROR) << "Magic number at start of config block is incorrect; possible data corruption "
                  "or incorrect file type";
    return {batt::StatusCode::kDataLoss};
  }

  Config config = packed_config.unpack();

  BATT_ASSIGN_OK_RESULT(llfs::IoRing::File file,
                        llfs::IoRing::File::open(io_ring->get_io_ring(), fd));

  return {std::make_unique<ChangeLogFile>(std::move(io_ring), std::move(file), config)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogFile::flush_config() noexcept
{
  // TODO [tastolfi 2026-05-01] The ChangeLogWriter (or this class) should keep a ready-to-go packed
  // version of the config block to speed this up.

  // TODO [tastolfi 2026-05-01] Look into mapping the reserved config block buffer for faster
  // io_uring writes (by avoiding the user->kernel remap step)

  PackedConfig packed_config;
  this->config_.pack_to(&packed_config);

  return llfs::write_fd(this->file_.get_fd(),
                        ConstBuffer{
                            &packed_config,
                            sizeof(PackedConfig),
                        },
                        /*offset=*/0);
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
  BATT_CHECK_EQ(this->config_.block_size & 511, 0);
  std::memset(&this->packed_config_buffer_, 0, sizeof(PackedConfig));
  this->config_.pack_to(&this->packed_config_buffer_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogFile::~ChangeLogFile() noexcept
{
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
