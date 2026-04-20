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
#include <turtle_kv/change_log/change_log_read_lock.hpp>
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

#if BATT_PLATFORM_IS_LINUX
#include <limits.h>
#endif

namespace turtle_kv {

class ChangeLogFile
{
  friend class ChangeLogWriter;

 public:
  using ReadLockCounter = batt::CpuCacheLineIsolated<std::atomic<i64>>;
  using ReadLock = ChangeLogReadLock;
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

    FileOffset block_offset_end() const noexcept
    {
      return FileOffset{this->block0_offset + (this->block_size * this->block_count)};
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

  Status append(const Slice<BlockBuffer*>& src) noexcept;

  /** \brief Returns the index of the next block to be written, in the file.
   */
  BlockIndex append_pos() noexcept;

#if 0
  Interval<i64> active_blocks() noexcept
  {
    return {this->lower_bound_.load(), this->upper_bound_.load()};
  }

  i64 active_block_count() const
  {
    return this->upper_bound_.load() - this->lower_bound_.load();
  }

  i64 size() const
  {
    return this->active_block_count() * this->config_.block_size;
  }
#endif

  FileByteCount capacity() const
  {
    return FileByteCount{this->config_.block_count * this->config_.block_size};
  }

#if 0
  FileByteCount space() const
  {
    return FileByteCount{this->capacity() - this->size()};
  }
#endif

  u64 available_block_tokens() const
  {
    return this->free_block_tokens_.available();
  }

  u64 in_use_block_tokens() const
  {
    return this->in_use_block_tokens_.size();
  }

  u64 reserved_block_tokens() const
  {
    return this->config_.block_count -
           (this->available_block_tokens() + this->in_use_block_tokens());
  }

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct TrimState {
    const i64 block_count_;

    Optional<EditOffset> trim_offset_;
    Optional<BlockIndex> next_block_to_trim_;
    Optional<BlockIndex> next_block_to_append_;
    std::unique_ptr<u64[]> block_valid_;
    std::unique_ptr<i64[]> block_upper_bound_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit TrimState(const Config& config) noexcept
        : block_count_{config.block_count}
        , trim_offset_{None}
        , next_block_to_trim_{None}
        , next_block_to_append_{None}
        , block_valid_{new u64[(this->block_count_ + 63) / 64]}
        , block_upper_bound_{new i64[this->block_count_]}
    {
      std::memset(this->block_valid_.get(), 0, sizeof(u64) * (config.block_count + 63) / 64);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    bool is_block_valid(BlockIndex i) const noexcept
    {
      return get_bit(this->block_valid_[i / 64], i % 64);
    }

    void set_block_valid(BlockIndex i, bool valid) noexcept
    {
      u64& w = this->block_valid_[i / 64];
      w = set_bit(w, i % 64, valid);
    }

    void set_block_upper_bound(BlockIndex i, EditOffset offset)
    {
      this->set_block_valid(i, true);
      this->block_upper_bound_[i] = offset.value();
    }

    void set_block_upper_bound(BlockIndex i, Optional<EditOffset> offset)
    {
      this->set_block_valid(i, offset.has_value());
      if (offset) {
        this->block_upper_bound_[i] = offset->value();
      }
    }

    Optional<EditOffset> get_block_upper_bound(BlockIndex i)
    {
      if (!this->is_block_valid(i)) {
        return None;
      }
      return EditOffset{this->block_upper_bound_[i]};
    }

    [[nodiscard]] i64 notify_blocks_written(i64 n_blocks_written);

    /** \brief Updates the trim offset, returning the number of blocks which are now overwritable
     * (starting at the previous trim offset).
     */
    [[nodiscard]] i64 set_trim_offset(EditOffset new_trim_offset);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

#if 0
  template <typename Fn = void(i64 block_i, ReadLockCounter& counter)>
  void for_block_range(const Interval<i64>& block_range, Fn&& fn) noexcept;

  void lock_for_read(const Interval<i64>& block_range) noexcept;

  void unlock_for_read(const Interval<i64>& block_range) noexcept;

  void update_lower_bound() noexcept;

  /** \brief Marks grant as in use by adding grant to this->in_use_block_tokens_.
   * Updates this->upper_bound_ to include the new number of blocks_written.
   * Returns a ReadLock on the range block_range.
   */
  ReadLock set_block_range_in_use(batt::Grant& grant, const Interval<i64>& block_range) noexcept;
#endif

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unique_ptr<llfs::ScopedIoRing> io_ring_;

  llfs::IoRing::File file_;

  Config config_;

  Metrics metrics_;

  // TODO [tastolfi 2026-04-20] rename block_offset_upper_bound (or similar)
  const FileOffset last_block_offset_ = this->config_.block_offset_end();

  const usize max_batch_size_ =
#if BATT_PLATFORM_IS_LINUX
      IOV_MAX;
#else
      2 * kMiB / this->config_.block_size;
#endif

  batt::Grant::Issuer free_block_tokens_{BATT_CHECKED_CAST(u64, this->config_.block_count.value())};

  batt::Grant in_use_block_tokens_{BATT_OK_RESULT_OR_PANIC(
      this->free_block_tokens_.issue_grant(0, batt::WaitForResource::kFalse))};

  batt::SmallVec<ConstBuffer, 32> next_batch_;

#if 0
  std::atomic<i64> lower_bound_{0};
  std::atomic<i64> upper_bound_{0};

  std::unique_ptr<ReadLockCounter[]> read_lock_counter_per_block_{
      new ReadLockCounter[this->config_.block_count]};
#endif

  absl::Mutex trim_state_mutex_;

  TrimState trim_state_{this->config_};

  u64 total_bytes_written_ = 0;

  batt::RateMetric<u64, /*seconds=*/100> write_throughput_;
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

    // TODO: [Gabe Bornstein 4/13/26] We're planning on removing ReadLocks. This code will need to
    // be removed once that happens.
    //

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

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

#if 0
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Fn>
inline void ChangeLogFile::for_block_range(const Interval<i64>& block_range, Fn&& fn) noexcept
{
  BATT_CHECK_GE(block_range.lower_bound, 0);
  BATT_CHECK_GE(block_range.upper_bound, 0);
  BATT_CHECK_LE(block_range.lower_bound, block_range.upper_bound);

  i64 block_i = block_range.lower_bound;
  i64 first_addr = block_range.lower_bound % this->config_.block_count;
  i64 count = block_range.size();
  BATT_CHECK_GE(count, 0);

  while (count != 0) {
    BATT_CHECK_LT(first_addr, this->config_.block_count);

    fn(block_i, this->read_lock_counter_per_block_[first_addr]);

    --count;
    ++block_i;
    ++first_addr;
    if (first_addr == this->config_.block_count) {
      first_addr = 0;
    }
  }
}
#endif

}  // namespace turtle_kv
