#pragma once

#include <turtle_kv/api_types.hpp>
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

  struct /*alignas(llfs::kDirectIOBlockAlign)*/ PackedConfig {
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

  void update_lower_bound(i64 update_upper_bound) noexcept;

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
