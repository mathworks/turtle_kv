#pragma once

#include <turtle_kv/api_types.hpp>
#include <turtle_kv/change_log_block.hpp>
#include <turtle_kv/change_log_file_metrics.hpp>
#include <turtle_kv/change_log_read_lock.hpp>
#include <turtle_kv/file_utils.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/config.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/ioring.hpp>
#include <llfs/ioring_file.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/cpu_align.hpp>
#include <batteries/interval.hpp>
#include <batteries/metrics/metric_collectors.hpp>
#include <batteries/pointers.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/small_vec.hpp>

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <filesystem>
#include <memory>

namespace turtle_kv {

class ChangeLogFile
{
  friend class ChangeLogReadLock;

 public:
  using ReadLockCounter = batt::CpuCacheLineIsolated<std::atomic<i64>>;
  using ReadLock = ChangeLogReadLock;
  using Metrics = ChangeLogFileMetrics;

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

  StatusOr<ReadLock> append(batt::Grant& grant, batt::SmallVecBase<ConstBuffer>& data) noexcept;

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

  i64 capacity() const
  {
    return this->config_.block_count * this->config_.block_size;
  }

  i64 space() const
  {
    return this->capacity() - this->size();
  }

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unique_ptr<llfs::ScopedIoRing> io_ring_;

  llfs::IoRing::File file_;

  Config config_;

  Metrics metrics_;

  const i64 last_block_offset_ = this->config_.block_offset_end();

  const usize max_batch_size_ = (16 * 1024 * 1024) / this->config_.block_size;

  batt::Grant::Issuer free_block_tokens_{BATT_CHECKED_CAST(u64, this->config_.block_count.value())};

  batt::Grant in_use_block_tokens_{BATT_OK_RESULT_OR_PANIC(
      this->free_block_tokens_.issue_grant(0, batt::WaitForResource::kFalse))};

  std::atomic<i64> lower_bound_{0};
  std::atomic<i64> upper_bound_{0};

  std::unique_ptr<ReadLockCounter[]> read_lock_counter_per_block_{
      new ReadLockCounter[this->config_.block_count]};

  absl::Mutex lower_bound_mutex_;

  u64 total_bytes_written_ = 0;

  batt::RateMetric<u64, /*seconds=*/100> write_throughput_;
};

// TODO: [Gabe Bornstein 1/16/26] Do I need to update other ChangeLogFile member data? Like lower,
// upper bound? They aren't recovered from ::open.
//
template <typename SerializeFn>
batt::Status ChangeLogFile::read_blocks(SerializeFn process_block)
{
  i64 blocks_read = 0;
  batt::Status status = batt::OkStatus();
  while (status.ok()) {
    // The offset of where we are writing to our buffer.
    //
    i64 curr_block_offset = blocks_read * this->config_.block_size;

    // The offset of where we are reading from the Change Log File.
    //
    i64 curr_file_offset = this->config_.block0_offset + curr_block_offset;

    batt::StatusOr<batt::Grant> buffer_grant =
        this->reserve_blocks(BlockCount{1}, batt::WaitForResource::kFalse);

    BATT_REQUIRE_OK(buffer_grant);

    ChangeLogBlock::ScopedMemory block_memory =
        ChangeLogBlock::allocate_aligned(this->config_.block_size);

    batt::Status read_status = this->file_.read_all(curr_file_offset, block_memory.buffer());

    if (!read_status.ok()) {
      LOG(INFO) << "Recovered " << blocks_read << " blocks. Stopped reading with status:"
                << BATT_INSPECT(batt::StatusCode::kOutOfRange);
      return batt::OkStatus();
    }

    StatusOr<boost::intrusive_ptr<ChangeLogBlock>> block =
        ChangeLogBlock::recover(std::move(block_memory), std::move(*buffer_grant));

    // TODO: [Gabe Bornstein 3/9/26] Handle case where we reach a corrupt block, but need to keep
    // reading and reach correct blocks that come after it.
    //
    if (block.status() == batt::StatusCode::kOutOfRange ||
        block.status() == batt::StatusCode::kDataLoss) {
      LOG(INFO) << "Recovered " << blocks_read
                << " blocks. Stopped reading with status:" << BATT_INSPECT(block.status());
      return batt::OkStatus();
    }

    // TODO: [Gabe Bornstein 3/11/26] Eventually, call (*block)->set_read_lock and
    // this->upper_bound_.fetch_add() when we know the logical ranges of each block.
    //

    // `process_block` is responsible for determining when to stop reading.
    //
    batt::Status process_status = process_block(std::move(*block));
    if (process_status == batt::StatusCode::kLoopBreak) {
      break;
    } else if (!process_status.ok()) {
      return process_status;
    }
    ++blocks_read;
  }
  return batt::OkStatus();
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

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

}  // namespace turtle_kv
