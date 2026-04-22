#include <turtle_kv/tree/filter_page_write_state.hpp>
//
#include <glog/logging.h>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto FilterPageWriteState::make_new() noexcept -> boost::intrusive_ptr<Self>
{
  return boost::intrusive_ptr<Self>{new Self{}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ FilterPageWriteState::FilterPageWriteState() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 FilterPageWriteState::active_count() const
{
  const i64 observed_finished = this->finished_count();
  const i64 observed_started = this->started_count();

  return observed_started - observed_finished;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilterPageWriteState::halt()
{
  this->started_.fetch_or(1);
  this->finished_.poke();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilterPageWriteState::join()
{
  for (;;) {
    Status status = this->finished_
                        .await_true([this](i64 observed_finished) {
                          const i64 observed_started = this->started_.load();
                          return observed_started == observed_finished * 2 + 1;
                        })
                        .status();
    if (status != batt::StatusCode::kPoke) {
      break;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status FilterPageWriteState::start()
{
  const i64 observed = this->started_.fetch_add(2);
  if (Self::is_halted_state(observed)) {
    // Undo the change to started_.
    //
    this->started_.fetch_sub(2);
    this->finished_.poke();
    return batt::StatusCode::kClosed;
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilterPageWriteState::finish()
{
  const i64 observed_started = this->started_count();
  const i64 prior_finished = this->finished_.fetch_add(1);
  const i64 observed_finished = prior_finished + 1;

  BATT_CHECK_GE(observed_started - observed_finished, 0);
}

}  // namespace turtle_kv
