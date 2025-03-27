#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>

#include <batteries/pointers.hpp>
#include <batteries/shared_ptr.hpp>

namespace turtle_kv {

class ChangeLogFile;

class ChangeLogReadLock : public batt::RefCounted<ChangeLogReadLock>
{
 public:
  static ChangeLogReadLock combine(const ChangeLogReadLock& first,
                                   const ChangeLogReadLock& second) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ChangeLogReadLock(ChangeLogFile* file, const Interval<i64>& block_range) noexcept;

  ChangeLogReadLock(const ChangeLogReadLock&) = delete;
  ChangeLogReadLock& operator=(const ChangeLogReadLock&) = delete;

  ChangeLogReadLock(ChangeLogReadLock&&) = default;
  ChangeLogReadLock& operator=(ChangeLogReadLock&&) = default;

  ~ChangeLogReadLock() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize block_count() const noexcept;

  const Interval<i64>& block_range() const noexcept
  {
    return this->block_range_;
  }

  ChangeLogFile* change_log_file() const noexcept
  {
    return this->change_log_file_.get();
  }

  ChangeLogReadLock lock_subrange(const Interval<i64>& subrange) noexcept;

  ChangeLogReadLock lock_subrange(usize start, usize len = 0) noexcept;

  ChangeLogReadLock clone() const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  batt::UniqueNonOwningPtr<ChangeLogFile> change_log_file_;
  Interval<i64> block_range_;
};

}  // namespace turtle_kv
