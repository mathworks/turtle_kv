//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_FILE_HPP

#include <turtle_kv/api_types.hpp>
#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/change_log_file_metrics.hpp>
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

  static constexpr i64 kDefaultBlockSize = 8192;
  static constexpr i64 kDefaultBlock0Offset = 4096;
  static constexpr i64 kDefaultLogSize = 32 * kMiB;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct PackedConfig;

  struct Config {
    BlockSize block_size;
    BlockCount block_count;
    FileOffset block0_offset;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static Config with_default_values() noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    void pack_to(PackedConfig* packed_config) const noexcept;

    /** \brief Returns the file offset corresponding to the end of the last block.
     */
    FileOffset last_block_end_offset() const noexcept
    {
      return this->block_offset_from_index(BlockIndex{this->block_count});
    }

    /** \brief Returns the file offset of the beginning of the specific block.
     */
    FileOffset block_offset_from_index(BlockIndex block_index) const noexcept
    {
      return FileOffset{this->block0_offset + (this->block_size * block_index)};
    }

    /** \brief Returns the index of the block that *ends* at `block_end_offset`.
     */
    BlockIndex block_index_from_end_offset(FileOffset block_end_offset) const noexcept
    {
      FileOffset block_begin_offset{block_end_offset - this->block_size};
      BlockIndex block_index{(block_begin_offset - this->block0_offset) / this->block_size};

      BATT_CHECK_EQ(block_begin_offset, this->block_offset_from_index(block_index))
          << "The passed `block_end_offset` must be aligned to the block size!"
          << BATT_INSPECT(this->block_size) << BATT_INSPECT(block_end_offset)
          << BATT_INSPECT(block_index);

      return block_index;
    }
  };

  // The flag O_DIRECT is set to true when reading some files. In order for the
  // O_DIRECT flag to work on all filesystems, PackedConfig (the file we're reading) needs to have
  // its starting address be aligned with 4096.
  //
  struct alignas(llfs::kDirectIOBlockAlign) PackedConfig {
    static constexpr u64 kMagic = 0x53ee6863bf7a1254ull;

    big_u64 magic;
    little_i64 block_size;
    little_i64 block_count;
    little_i64 block0_offset;

    u8 reserved_[4096 - 32];

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Config unpack() const noexcept;
  };

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

  const Config& config() const noexcept
  {
    return this->config_;
  }

  StatusOr<batt::Grant> reserve_blocks(BlockCount block_count,
                                       batt::WaitForResource wait_for_resource) noexcept;

  /** \brief Recovers all previously active ChangeLogBlocks from disk and returns them to the user.
   * All blocks initially have a reference count of 1. intrusive_ptr will help manage the lifetime
   * of the block, however, the user is also resposnsible for altering and managing the lifetime of
   * the returned blocks (https://www.boost.org/doc/libs/1_40_0/libs/smart_ptr/intrusive_ptr.html).
   */
  batt::StatusOr<std::vector<boost::intrusive_ptr<ChangeLogBlock>>> read_blocks_into_vector();

  // TODO: [Gabe Bornstein 1/20/26] Consider using concepts here to define required parameters and
  // return types?
  //
  /** \brief Read over all the blocks currently in the ChangeLogFile, calling process_block for each
   * block.
   * process_block is responsible for determining when to stop reading blocks, and what to do
   * with each recovered block.
   * Ownership of the ChangeLogBlock's memory is transferred to `process_block`.
   * `process_block` must free the block's memory if it plans to do nothing with it.
   */
  template <typename SerializeFn = batt::Status(boost::intrusive_ptr<ChangeLogBlock>)>
  batt::Status read_blocks(SerializeFn process_block);

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
// TODO: [Gabe Bornstein 1/16/26] Do I need to update other ChangeLogFile member data? Like lower,
// upper bound? They aren't recovered from ::open.
//
template <typename SerializeFn>
batt::Status ChangeLogFile::read_blocks(SerializeFn process_block)
{
  BATT_ASSIGN_OK_RESULT(const i64 file_size, llfs::sizeof_fd(this->file_.get_fd()));

  std::unordered_set<i64> corrupted_block_offsets;
  i64 file_offset = this->config_.block0_offset;

  for (i64 blocks_read = 0; blocks_read < this->config_.block_count; ++blocks_read) {
    //----- --- -- -  -  -   -

    // If the next read spans the end of the file, then we have data loss!
    //
    if (file_offset < file_size && file_offset + this->config_.block_size > file_size) {
      return batt::StatusCode::kDataLoss;
    }

    // If the file ends on a block boundary, then assume this is as far as we've written.
    //
    if (file_offset == file_size) {
      break;
    }

    // Allocate a buffer for the next block to read.
    //
    ChangeLogBlock::ScopedMemory block_memory =
        ChangeLogBlock::allocate_aligned(this->config_.block_size);

    // Read the block!
    //
    batt::Status read_status = this->file_.read_all(file_offset, block_memory.buffer());
    BATT_REQUIRE_OK(read_status);
    file_offset += this->config_.block_size;

    // Recover the block from the read data; this will perform data integrity checks.
    //
    StatusOr<boost::intrusive_ptr<ChangeLogBlock>> block =
        ChangeLogBlock::recover(std::move(block_memory));

    if (block.status() == batt::StatusCode::kOutOfRange) {
      LOG(INFO) << "Recovered " << blocks_read
                << " blocks. Stopped reading with status:" << BATT_INSPECT(block.status())
                << BATT_INSPECT(file_offset) << BATT_INSPECT(file_offset);

      return batt::OkStatus();

    } else if (block.status() == batt::StatusCode::kDataLoss) {
      LOG(INFO) << "Data loss detected at block offset " << file_offset
                << ". Continuing to read subsequent blocks.";

      // TODO: [Gabe Bornstein 4/3/26] We fail de-referencing this block if it's corrupted.
      // How do we want to handle tracking which blocks we want to ignore if we can't rely on
      // information in the corrupted block?
      //
      // corrupted_block_offsets.insert((*block)->edit_offset_lower_bound().value());
      // ++blocks_read;
      // continue;

      // TODO: [Gabe Bornstein 4/3/26] Temporarily return here when we encounter a corrupted block.
      // We won't read any blocks following a corrupted block detection.
      //
      return batt::OkStatus();
    }

    // TODO: [Gabe Bornstein 4/3/26] Optimize this case with Block Clusters (see design doc).
    //
    // TODO: [Gabe Bornstein 4/3/26] Currently broken. We aren't succesfully tracking which offsets
    // have been corrupted.
    //
    // Handle case where we reach a corrupt block, but need to keep
    // reading and reach correct blocks that come after it. Forget all blocks that have an
    // EditOffset higher than blocks with kDataLoss. Remember valid blocks with lower EditOffsets.
    //
    if (corrupted_block_offsets.size() > 0) {
      i64 curr_block_offset_upper_bound = (*block)->edit_offset_upper_bound().value();

      for (auto offset : corrupted_block_offsets) {
        // If these two blocks have any overlap, or if the current block came after the corrupt
        // block, we need to discard the current block
        //
        if (curr_block_offset_upper_bound >= offset) {
          LOG(INFO) << "Discarding block at offset " << file_offset
                    << " due to prior data loss at offset " << offset;
          continue;
        }
      }
    }

    // `process_block` is responsible for determining when to stop reading.
    //
    batt::Status process_status = process_block(std::move(*block));
    if (process_status == batt::StatusCode::kLoopBreak) {
      break;
    } else if (!process_status.ok()) {
      return process_status;
    }
  }
  return batt::OkStatus();
}

}  // namespace turtle_kv
