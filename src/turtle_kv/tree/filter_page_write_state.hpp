#pragma once
#define TURTLE_KV_TREE_FILTER_PAGE_WRITE_STATE_HPP

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/shared_ptr.hpp>

#include <boost/intrusive_ptr.hpp>

#include <atomic>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Tracks the number of filter pages being written via an llfs::PageCache.
 */
class FilterPageWriteState : public batt::RefCounted<FilterPageWriteState>
{
 public:
  using Self = FilterPageWriteState;

  /** \brief Returns true iff the observed value of the `started_` counter indicates that halt has
   * been called.
   */
  static constexpr bool is_halted_state(i64 observed_started)
  {
    return (observed_started & 1) == 1;
  }

  /** \brief Returns a new reference-counted FilterPageWriteState.
   */
  static boost::intrusive_ptr<Self> make_new() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief This class is not movable or copyable.
   */
  FilterPageWriteState(const FilterPageWriteState&) = delete;

  /** \brief This class is not movable or copyable.
   */
  FilterPageWriteState& operator=(const FilterPageWriteState&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff `this->halt()` has been called at least once.
   */
  bool is_halted() const
  {
    return Self::is_halted_state(this->started_.load());
  }

  /** \brief Returns the number of started filter page writes.
   */
  i64 started_count() const
  {
    return this->started_.load() / 2;
  }

  /** \brief Returns the number of finished filter page writes.
   */
  i64 finished_count() const
  {
    return this->finished_.get_value();
  }

  /** \brief Returns an estimate of the number of active filter page writes.
   *
   * The true count may be higher than the returned value.
   */
  i64 active_count() const;

  /** \brief Causes all future calls to `this->start()` to fail with `batt::StatusCode::kClosed`.
   */
  void halt();

  /** \brief Blocks the caller until all pending filter writes have completed.
   */
  void join();

  /** \brief Attempts to increase the started count; if successful, returns `OkStatus()`.
   *
   * This function will fail if `this->halt()` has been called; in this case,
   * `batt::StatusCode::kClosed` is returned.
   */
  Status start();

  /** \brief Increments the finished counter, indicating a page write has finished.
   */
  void finish();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Creates a new FilterPageWriteState.
   */
  explicit FilterPageWriteState() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Holds the number of successful starts plus the halted state.
   */
  std::atomic<i64> started_{0};

  /** \brief Tracks the number of finishes.
   */
  batt::Watch<i64> finished_{0};
};

}  // namespace turtle_kv
