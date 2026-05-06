//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_FILE_HPP

#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/change_log_config.hpp>
#include <turtle_kv/change_log/change_log_file_metrics.hpp>
#include <turtle_kv/change_log/change_log_meta_state.hpp>
#include <turtle_kv/change_log/recovered_change_log_state.hpp>

#include <turtle_kv/api_types.hpp>
#include <turtle_kv/file_utils.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/config.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_file.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/config.hpp>
#include <batteries/cpu_align.hpp>
#include <batteries/interval.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/operators.hpp>
#include <batteries/pointers.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/small_vec.hpp>

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <unordered_set>

namespace turtle_kv {

class ChangeLogFile
{
  friend class ChangeLogWriter;

 public:
  using ReadLockCounter = batt::CpuCacheLineIsolated<std::atomic<i64>>;
  using Metrics = ChangeLogFileMetrics;
  using BlockBuffer = ChangeLogBlock;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr i64 kMetaBlockOffset = 0;
  static constexpr i64 kDefaultBlockSize = 8192;
  static constexpr i64 kDefaultBlock0Offset = 4096;
  static constexpr i64 kDefaultLogSize = 32 * kMiB;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Config = ChangeLogConfig;
  using PackedConfig = PackedChangeLogConfig;

  using MetaState = ChangeLogMetaState;
  using PackedMetaState = PackedChangeLogMetaState;

  // The flag O_DIRECT is set to true when reading some files. In order for the
  // O_DIRECT flag to work on all filesystems, PackedConfig (the file we're reading) needs to have
  // its starting address be aligned with 4096.
  //
  struct alignas(llfs::kDirectIOBlockAlign) PackedMetaBlock {
    PackedConfig config;

    PackedMetaState meta_state;

    u8 reserved_[4096 - 128];
  };

  static_assert(sizeof(PackedMetaBlock) == 4096);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Status create(const std::filesystem::path& path,
                       const Config& config,
                       RemoveExisting remove_existing = RemoveExisting{false}) noexcept;

  static StatusOr<std::unique_ptr<ChangeLogFile>> open(const std::filesystem::path& path) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ChangeLogFile(std::unique_ptr<llfs::ScopedIoRing>&& io_ring,
                         llfs::IoRing::File&& file,
                         const Config& config) noexcept;

  ~ChangeLogFile() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Config& config() noexcept
  {
    return this->config_;
  }

  StatusOr<batt::Grant> reserve_blocks(BlockCount block_count,
                                       batt::WaitForResource wait_for_resource) noexcept;

  Status read_meta_block(PackedMetaBlock& meta_block) noexcept;

  Status write_meta_block(const PackedMetaBlock& meta_block) noexcept;

  /** \brief Recovers all previously active ChangeLogBlocks from disk and returns them to the user.
   * All blocks initially have a reference count of 1. intrusive_ptr will help manage the lifetime
   * of the block, however, the user is also resposnsible for altering and managing the lifetime of
   * the returned blocks (https://www.boost.org/doc/libs/1_40_0/libs/smart_ptr/intrusive_ptr.html).
   */
  batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> read_blocks_into_vector();

  /** \brief Read over all the blocks currently in the ChangeLogFile, calling process_block for each
   * block.
   * process_block is responsible for determining when to stop reading blocks, and what to do
   * with each recovered block.
   * Ownership of the ChangeLogBlock's memory is transferred to `process_block`.
   * `process_block` must free the block's memory if it plans to do nothing with it.
   */
  template <typename ConsumeBlockFn = Status(boost::intrusive_ptr<ChangeLogBlock>)>
  batt::Status read_blocks(ConsumeBlockFn consume_block);

  FileByteCount capacity() const
  {
    return FileByteCount{this->config_.block_count * this->config_.block_size};
  }

  auto size() const
  {
    // TODO [tastolfi 2026-04-20] fix this to be accurate
    //
    return this->capacity();
  }

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  Metrics& metrics()
  {
    return this->metrics_;
  }

  llfs::IoRing::File& file() noexcept
  {
    return this->file_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::unique_ptr<llfs::ScopedIoRing> io_ring_;

  llfs::IoRing::File file_;

  Config config_;

  Metrics metrics_;
};

BATT_OBJECT_PRINT_IMPL((inline), ChangeLogFile::Config, (block_size, block_count, block0_offset))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
template <typename ConsumeBlockFn>
batt::Status ChangeLogFile::read_blocks(ConsumeBlockFn consume_block)
{
  BATT_ASSIGN_OK_RESULT(const i64 file_size, llfs::sizeof_fd(this->file_.get_fd()));

  PackedMetaBlock meta_block;
  BATT_REQUIRE_OK(this->read_meta_block(meta_block));

  MetaState meta_state = meta_block.meta_state.unpack();

  Config& cfg = this->config_;

  std::unordered_set<i64> corrupted_block_offsets;

  // Track the number of blocks successfully read and consumed.
  //
  usize n_blocks_read = 0;

  // Calculate the block range to read, based on the most recently written MetaState.
  //
  Interval<BlockIndex> block_range = cfg.block_range_to_recover(meta_state.block_range);

  VLOG(1) << "reading " << BATT_INSPECT(block_range);

  for (; !block_range.empty(); cfg.increment_lower_bound(block_range)) {
    //----- --- -- -  -  -   -

    FileOffset file_offset = cfg.block_offset_from_index(block_range.lower_bound);

    // If the next read spans the end of the file, then we have data loss!
    //
    if (file_offset < file_size && file_offset + cfg.block_size > file_size) {
      return batt::StatusCode::kDataLoss;
    }

    // Allocate a buffer for the next block to read.
    //
    ChangeLogBlock::ScopedMemory block_memory = ChangeLogBlock::allocate_aligned(cfg.block_size);

    // Read the block!
    //
    batt::Status read_status = this->file_.read_all(file_offset, block_memory.buffer());
    BATT_REQUIRE_OK(read_status);

    // Recover the block from the read data; this will perform data integrity checks.
    //
    StatusOr<boost::intrusive_ptr<ChangeLogBlock>> block =
        ChangeLogBlock::recover(std::move(block_memory), block_range.lower_bound);

    // Keep reading subsequent block if we read an invalid block.
    //
    if (!block.ok()) {
      continue;
    }

    // `process_block` is responsible for determining when to stop reading.
    //
    batt::Status process_status = consume_block(std::move(*block));
    if (process_status == batt::StatusCode::kLoopBreak) {
      break;
    } else if (!process_status.ok()) {
      return process_status;
    }

    ++n_blocks_read;
  }

  return batt::OkStatus();
}

}  // namespace turtle_kv
