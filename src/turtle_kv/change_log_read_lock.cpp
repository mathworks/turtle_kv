#include <turtle_kv/change_log_read_lock.hpp>
//

#include <turtle_kv/change_log_file.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ ChangeLogReadLock ChangeLogReadLock::combine(const ChangeLogReadLock& first,
                                                        const ChangeLogReadLock& second) noexcept
{
  BATT_CHECK_NOT_NULLPTR(first.change_log_file_);
  BATT_CHECK_EQ(first.change_log_file_, second.change_log_file_);
  BATT_CHECK_LT(first.block_range_.lower_bound, first.block_range_.upper_bound);
  BATT_CHECK_LT(second.block_range_.lower_bound, second.block_range_.upper_bound);

  Interval<i64> combined_range{
      .lower_bound = std::min(first.block_range_.lower_bound, second.block_range_.lower_bound),
      .upper_bound = std::max(first.block_range_.upper_bound, second.block_range_.upper_bound),
  };

  first.change_log_file_->lock_for_read(combined_range);

  return ChangeLogReadLock{first.change_log_file_.get(), combined_range};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ChangeLogReadLock::ChangeLogReadLock(ChangeLogFile* file,
                                                  const Interval<i64>& block_range) noexcept
    : change_log_file_{file}
    , block_range_{block_range}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogReadLock::~ChangeLogReadLock() noexcept
{
  if (this->change_log_file_) {
    this->change_log_file_->unlock_for_read(this->block_range_);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize ChangeLogReadLock::block_count() const noexcept
{
  return BATT_CHECKED_CAST(usize, this->block_range_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogReadLock ChangeLogReadLock::lock_subrange(const Interval<i64>& subrange) noexcept
{
  BATT_CHECK_LE(this->block_range_.lower_bound, subrange.lower_bound);
  BATT_CHECK_GE(this->block_range_.upper_bound, subrange.upper_bound);

  this->change_log_file_->lock_for_read(subrange);

  return ChangeLogReadLock{this->change_log_file_.get(), subrange};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogReadLock ChangeLogReadLock::lock_subrange(usize start, usize len) noexcept
{
  Interval<i64> i;
  i.lower_bound = this->block_range_.lower_bound + BATT_CHECKED_CAST(i64, start);
  if (len == 0) {
    i.upper_bound = this->block_range_.upper_bound;
  } else {
    i.upper_bound = i.lower_bound + len;
  }

  return this->lock_subrange(i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ChangeLogReadLock ChangeLogReadLock::clone() const noexcept
{
  BATT_CHECK_NOT_NULLPTR(this->change_log_file_);

  this->change_log_file_->lock_for_read(this->block_range_);

  return ChangeLogReadLock{this->change_log_file_.get(), this->block_range_};
}

}  // namespace turtle_kv
