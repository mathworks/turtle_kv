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

#if 0
  std::memset((void*)read_lock_counter_per_block_.get(),
              0,
              sizeof(ReadLockCounter) * this->config_.block_count);

  for (i64 i = 0; i < this->config_.block_count; ++i) {
    BATT_CHECK_EQ(read_lock_counter_per_block_[i]->load(), 0);
  }
#endif

  this->metrics_.freed_blocks_count.add(this->free_block_tokens_.total_size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogFile::~ChangeLogFile() noexcept
{
#if 0
  Interval<i64> block_range = this->active_blocks();
  BATT_CHECK_EQ(block_range.size(), 0);
  BATT_CHECK_EQ(this->active_block_count(), 0);
#endif

  VLOG(1) << BATT_INSPECT(this->write_throughput_.get());
}

#if 0
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::lock_for_read(const Interval<i64>& block_range) noexcept
{
  this->for_block_range(block_range, [](i64 block_i [[maybe_unused]], ReadLockCounter& counter) {
    counter->fetch_add(1);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogFile::unlock_for_read(const Interval<i64>& block_range) noexcept
{
  this->for_block_range(block_range,
                        [this](i64 block_i [[maybe_unused]], ReadLockCounter& counter) {
                          const auto old_count = counter->fetch_sub(1);
                          BATT_CHECK_GT(old_count, 0)
                              << BATT_INSPECT(batt::to_string(std::hex, (u64)old_count))
                              << BATT_INSPECT(block_i) << BATT_INSPECT(this->config_.block_count);
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

    i64 observed_upper_bound = this->upper_bound_.load();
    i64 observed_lower_bound = this->lower_bound_.load();
    i64 new_lower_bound = observed_lower_bound;
    old_lower_bound = observed_lower_bound;

    i64 addr = new_lower_bound % this->config_.block_count;
    while (this->read_lock_counter_per_block_[addr]->load() == 0) {
      if (new_lower_bound >= observed_upper_bound) {
        i64 new_observed_upper_bound = this->upper_bound_.load();
        if (new_observed_upper_bound == observed_upper_bound) {
          break;
        } else {
          observed_upper_bound = new_observed_upper_bound;
        }
      }
      ++addr;
      if (addr == this->config_.block_count) {
        addr = 0;
      }
      ++new_lower_bound;
      ++n_blocks_freed;
    }

    const i64 prior_lower_bound = this->lower_bound_.exchange(new_lower_bound);
    BATT_CHECK_EQ(prior_lower_bound, observed_lower_bound);
    BATT_CHECK_EQ(new_lower_bound - old_lower_bound, BATT_CHECKED_CAST(i64, n_blocks_freed));
  }

  StatusOr<batt::Grant> newly_freed =
      this->in_use_block_tokens_.spend(n_blocks_freed, batt::WaitForResource::kFalse);

  BATT_CHECK_OK(newly_freed) << BATT_INSPECT(n_blocks_freed) << BATT_INSPECT(old_lower_bound)
                             << BATT_INSPECT(this->lower_bound_.load())
                             << BATT_INSPECT(this->in_use_block_tokens_.size());

  this->metrics_.freed_blocks_count.add(newly_freed->size());
}
#endif

#if 0
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogFile::ReadLock ChangeLogFile::set_block_range_in_use(
    batt::Grant& grant,
    const Interval<i64>& block_range) noexcept
{
  i64 blocks_written = block_range.size();
  this->lock_for_read(block_range);

  // Important: only do this after locking the range.
  //
  {
    BATT_CHECK_EQ(grant.size(), blocks_written);

    batt::Grant now_in_use =
        BATT_OK_RESULT_OR_PANIC(grant.spend(blocks_written, batt::WaitForResource::kFalse));

    this->in_use_block_tokens_.subsume(std::move(now_in_use));

    BATT_CHECK_EQ(grant.size(), 0);
  }

  return ReadLock{this, block_range};
}
#endif

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>>
ChangeLogFile::read_blocks_into_vector()
{
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ChangeLogFile::append(const Slice<BlockBuffer*>& src) noexcept
{
  if (src.begin() == src.end()) {
    return OkStatus();
  }

  // Initialize src state.  We will repeatedly build as large a batch as possible by consuming
  // chunks of data from the src.
  //
  auto src_next = src.begin();
  const auto src_last = src.end();
  ConstBuffer src_first = (*src_next)->prepare_to_flush();

  // Helper function: returns true iff there is no more data in src to consume.
  //
  const auto src_empty = [&] {
    return src_next == src_last;
  };

  // Find the block at which to start appending.
  //
  i64 dst_block_i = this->append_pos();
  i64 file_offset = this->config_.block0_offset + dst_block_i * this->config_.block_size;
  i64 total_written = 0;

  //----- --- -- -  -  -   -
  batt::SmallVecBase<ConstBuffer>& batch = this->next_batch_;
  batch.clear();

  // The number of bytes in the next batch.
  //
  usize batch_size = 0;

  // The index within `batch` where the unconsumed data begins.
  //
  usize batch_start = 0;

  // Helper function: returns true iff the batch is empty.
  //
  const auto batch_empty = [&batch, &batch_start] {
    return batch_start == batch.size();
  };

  // We are not done until the input is consumed and all data is written.
  //
  while (!src_empty() || !batch_empty()) {
    // The available space (bytes) in the next batch.
    //
    usize space_in_batch = static_cast<usize>(std::min(this->last_block_offset_ - file_offset,
                                                       this->max_batch_size_)) -
                           batch_size;

    //----- --- -- -  -  -   -
    // Collect the next batch.
    //
    {
      absl::MutexLock lock{&this->trim_state_mutex_};

      while (!src_empty() && space_in_batch >= src_first.size()) {
        this->next_batch_.push_back(src_first);

        batch_size += src_first.size();
        space_in_batch -= src_first.size();

        ++src_next;
        src_first = (*src_next)->prepare_to_flush();
      }
    }

    //----- --- -- -  -  -   -
    // Write the batch.
    //
    StatusOr<i32> n_written = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      Slice<ConstBuffer> data_slice = batt::as_slice(batch.data() + batch_start,  //
                                                     batch.size() - batch_start);

      this->file_.async_write_some(file_offset, data_slice, BATT_FORWARD(handler));
    });
    BATT_REQUIRE_OK(n_written);

    total_written += *n_written;

    //----- --- -- -  -  -   -
    // Consume the written data from the front of the batch.
    //
    usize n_to_consume = BATT_CHECKED_CAST(usize, *n_written);
    this->total_bytes_written_ += n_to_consume;
    this->write_throughput_.update(this->total_bytes_written_);

    while (n_to_consume != 0) {
      ConstBuffer& batch_chunk = batch[batch_start];
      const usize consumed_this_chunk = std::min(n_to_consume, batch_chunk.size());

      // Advance/shrink the first chunk in the batch, and increase the available space.
      //
      batch_chunk += consumed_this_chunk;
      batch_size -= consumed_this_chunk;

      // When we have fully consumed the first chunk of the batch, increment the `batch_start`
      // index.
      //
      if (batch_chunk.size() == 0) {
        ++batch_start;

        // Once `batch_start` has advanced to half the size of the batch (in chunks), shift
        // everything down to the beginning.  This does not change the space_in_batch.
        //
        if (batch_start * 2 >= batch.size()) {
          this->next_batch_.erase(this->next_batch_.begin(),
                                  this->next_batch_.begin() + batch_start);
          batch_start = 0;
        }
      }

      file_offset += consumed_this_chunk;
      n_to_consume -= consumed_this_chunk;

      BATT_CHECK_LE(file_offset, this->last_block_offset_)
          << BATT_INSPECT(this->config_.block_size) << BATT_INSPECT(this->config_.block_count)
          << BATT_INSPECT(this->config_.block0_offset);
    }

    //----- --- -- -  -  -   -
    // Wrap around when we get to the end of the file.
    //
    if (file_offset == this->last_block_offset_) {
      file_offset = this->config_.block0_offset;
    }
  }

  BATT_CHECK_EQ(total_written % this->config_.block_size, 0);

  i64 blocks_written = total_written / this->config_.block_size;
  i64 append_upper_bound = append_lower_bound + blocks_written;

  auto block_range = Interval<i64>{append_lower_bound, append_upper_bound};

  ChangeLogFile::ReadLock read_lock = this->set_block_range_in_use(grant, block_range);
  this->upper_bound_.fetch_add(block_range.size());

  return {std::move(read_lock)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BlockIndex ChangeLogFile::append_pos() noexcept
{
  absl::MutexLock lock{&this->trim_state_mutex_};
  BATT_CHECK(this->trim_state_.next_block_to_append_);

  return *this->trim_state_.next_block_to_append_ % this->config_.block_count;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class ChangeLogFile::TrimState

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 ChangeLogFile::TrimState::notify_blocks_written(const Slice<BlockBuffer*>& blocks)
{
  BATT_CHECK(this->next_block_to_trim_ && this->next_block_to_append_)
      << "The next block to append must be set before updating the trim offset!  (Forgot to run "
         "recovery?)";

  i64 block_i = *this->next_block_to_append_;
  for (BlockBuffer* block_buffer : blocks) {
    this->set_block_upper_bound(block_i, block_buffer->edit_offset_upper_bound());
    ++block_i;
    if (block_i == this->block_count_) {
      block_i -= this->block_count_;
    }
  }

  *this->next_block_to_append_ += BATT_CHECKED_CAST(i64, blocks.size());

  const i64 active_count = *this->next_block_to_append_ - *this->next_block_to_trim_;

  BATT_CHECK_GE(active_count, 0);
  BATT_CHECK_LE(active_count, this->block_count_);
  BATT_CHECK_EQ(block_i, *this->next_block_to_append_ % this->block_count_);

  return block_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize ChangeLogFile::TrimState::set_trim_offset(EditOffset new_trim_offset)
{
  BATT_CHECK(this->next_block_to_trim_ && this->next_block_to_append_)
      << "The oldest untrimmed block must be set before updating the trim offset!  (Forgot to run "
         "recovery?)";

  BATT_CHECK_LE(*this->next_block_to_trim_, *this->next_block_to_append_);

  // If the current trim offset is past the new one, do nothing.
  //
  if (this->trim_offset_ && *this->trim_offset_ > new_trim_offset) {
    return 0;
  }

  usize n_blocks_trimmed = 0;

  for (;;) {
    // Are we out of potential blocks to trim?
    //
    if (*this->next_block_to_trim_ == *this->next_block_to_append_) {
      break;
    }

    Optional<EditOffset> next_block_upper_bound =
        this->get_block_upper_bound(*this->next_block_to_trim_);

    // Is the next block after the new trim offset?
    //
    if (next_block_upper_bound && *next_block_upper_bound > new_trim_offset) {
      break;
    }

    // Trim the block!
    //
    this->set_block_valid(*this->next_block_to_trim_, false);
    n_blocks_trimmed += 1;
    *this->next_block_to_trim_ += 1;
  }

  return n_blocks_trimmed;
}

}  // namespace turtle_kv
