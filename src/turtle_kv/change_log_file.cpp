#include <turtle_kv/change_log_file.hpp>
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

  BATT_STATIC_ASSERT_EQ(sizeof(PackedConfig), 4096);
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
/*explicit*/ ChangeLogFile::ChangeLogFile(std::unique_ptr<llfs::ScopedIoRing>&& io_ring,
                                          llfs::IoRing::File&& file,
                                          const Config& config) noexcept
    : io_ring_{std::move(io_ring)}
    , file_{std::move(file)}
    , config_{config}
{
  BATT_CHECK_EQ(this->config_.block_size & 511, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogFile::~ChangeLogFile() noexcept
{
  VLOG(1) << BATT_INSPECT(this->write_throughput_.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::lock_for_read(const Interval<i64>& block_range) noexcept
{
  this->for_block_range(block_range, [](ReadLockCounter& counter) {
    counter->fetch_add(1);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::unlock_for_read(const Interval<i64>& block_range) noexcept
{
  this->for_block_range(block_range, [](ReadLockCounter& counter) {
    const auto old_count = counter->fetch_sub(1);
    BATT_CHECK_GT(old_count, 0);
  });
  this->update_lower_bound();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::update_lower_bound() noexcept
{
  u64 n_blocks_freed = 0;
  i64 old_lower_bound = 0;

  {
    absl::MutexLock lock{&this->lower_bound_mutex_};

    i64 lower_bound_target = this->lower_bound_.get_value();
    const i64 observed_upper_bound = this->upper_bound_.load();

    old_lower_bound = lower_bound_target;

    i64 addr = lower_bound_target % this->config_.block_count;
    while (lower_bound_target < observed_upper_bound) {
      if (this->read_lock_counter_per_block_[addr]->load() > 0) {
        break;
      }
      ++addr;
      if (addr == this->config_.block_count) {
        addr = 0;
      }
      ++lower_bound_target;
      ++n_blocks_freed;
    }

    this->lower_bound_.clamp_min_value(lower_bound_target);
  }

  StatusOr<batt::Grant> newly_freed =
      this->token_pool_.issue_grant(n_blocks_freed, batt::WaitForResource::kFalse);

  BATT_CHECK_OK(newly_freed) << BATT_INSPECT(n_blocks_freed) << BATT_INSPECT(old_lower_bound)
                             << BATT_INSPECT(this->lower_bound_.get_value());

  this->free_pool_.subsume(std::move(*newly_freed));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Grant> ChangeLogFile::reserve_blocks(
    BlockCount count,
    batt::WaitForResource wait_for_resource) noexcept
{
  return this->free_pool_.spend(count, wait_for_resource);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ChangeLogFile::append(batt::Grant& grant, batt::SmallVecBase<ConstBuffer>& data) noexcept
    -> StatusOr<ReadLock>
{
  i64 append_lower_bound = this->upper_bound_.load();
  i64 addr = append_lower_bound % this->config_.block_count;
  i64 file_offset = this->config_.block0_offset + addr * this->config_.block_size;
  i64 total_written = 0;

  while (!data.empty()) {
    auto data_slice = batt::as_slice(data.data(), std::min(data.size(), this->max_batch_size_));

    StatusOr<i32> n_written = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      this->file_.async_write_some(file_offset, data_slice, BATT_FORWARD(handler));
    });

    BATT_REQUIRE_OK(n_written);

    total_written += *n_written;

    usize n_to_consume = BATT_CHECKED_CAST(usize, *n_written);
    this->total_bytes_written_ += n_to_consume;
    this->write_throughput_.update(this->total_bytes_written_);

    while (n_to_consume != 0) {
      usize n_this_block = std::min(n_to_consume, data.front().size());
      data.front() += n_this_block;
      if (data.front().size() == 0) {
        data.erase(data.begin());
      }

      file_offset += n_this_block;
      n_to_consume -= n_this_block;

      BATT_CHECK_LE(file_offset, this->last_block_offset_)
          << BATT_INSPECT(this->config_.block_size) << BATT_INSPECT(this->config_.block_count)
          << BATT_INSPECT(this->config_.block0_offset);
      if (file_offset == this->last_block_offset_) {
        file_offset = this->config_.block0_offset;
      }
    }
  }

  BATT_CHECK_EQ(total_written % this->config_.block_size, 0);

  i64 blocks_written = total_written / this->config_.block_size;
  i64 append_upper_bound = append_lower_bound + blocks_written;

  auto block_range = Interval<i64>{append_lower_bound, append_upper_bound};

  this->lock_for_read(block_range);

  // Important: only do this after locking the range.
  //
  {
    this->upper_bound_.fetch_add(blocks_written);

    BATT_CHECK_OK(grant.spend(blocks_written, batt::WaitForResource::kFalse));
  }

  return {ReadLock{this, block_range}};
}

}  // namespace turtle_kv
