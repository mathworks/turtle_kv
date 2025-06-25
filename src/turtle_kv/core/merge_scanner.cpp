#include <turtle_kv/core/merge_scanner.hpp>
//

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/small_vec.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeScanner::MergeScanner() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeScanner::~MergeScanner() noexcept
{
  this->stop();

  BATT_CHECK(!this->outside_);
  BATT_CHECK(!this->inside_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeScanner::set_generator(GeneratorFn&& fn)
{
  BATT_CHECK(this->by_front_key_.empty());
  BATT_CHECK(!this->inside_);
  BATT_CHECK(!this->outside_);

  this->inside_ = callcc(this->inside_stack_.get_allocator(),
                         [this, fn = std::move(fn)](batt::Continuation&& origin) {
                           this->outside_ = origin.resume();
                           {
                             GeneratorContext context{this};
                             this->generator_status_ = fn(context);
                           }
                           return std::move(this->outside_);
                         });

  this->next_item_ = None;
  this->at_end_ = false;
  this->needs_input_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeScanner::stop()
{
  this->stop_requested_ = true;

  if (this->inside_) {
    this->inside_ = this->inside_.resume();
  }

  this->by_front_key_.clear();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeScanner::push_frame_impl(MergeFrame* frame)
{
  VLOG(1) << "push_frame() entered " << BATT_INSPECT(this->depth_);

  BATT_CHECK_LE(frame->line_count(), 64);
  BATT_CHECK_LE(frame->line_count() + this->depth_, 64);

  frame->active_mask_ = 0;

  BATT_CHECK_EQ(frame->pushed_to_, nullptr)
      << "This frame is already pushed onto a different compactor's stack!";
  frame->pushed_to_ = this;

  for (usize i = 0; i < frame->line_count(); ++i) {
    this->depth_ += 1;
    MergeLine& line = *frame->get_line(i);
    BATT_CHECK_EQ(frame, line.frame_);
    line.depth_ = this->depth_;
    if (!line.empty()) {
      BATT_CHECK_EQ(line.get_index_in_frame(), i);
      frame->active_mask_ |= (u64{1} << i);
      VLOG(1) << " .. frame.line[" << i << "] activated;";
      this->by_front_key_.push(as_ref(line));
    } else {
      VLOG(1) << " .. frame.line[" << i << "] empty -- NOT active, ignoring;";
    }
  }
  VLOG(1) << "push_frame() finished " << BATT_INSPECT(this->depth_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MergeScanner::await_frame_consumed_impl(MergeFrame* frame)
{
  BATT_CHECK_EQ(frame->pushed_to_, this) << "The passed frame was never pushed to this compactor";

  while (frame->active_mask_ != 0) {
    if (!this->generator_status_.ok() || this->stop_requested_) {
      VLOG(1) << "await_frame_consumed() - compaction cancelled; unwinding...";
      return Status{batt::StatusCode::kCancelled};
    }

    BATT_CHECK(this->outside_);
    this->outside_ = this->outside_.resume();
  }
  const usize old_depth = this->depth_;
  this->depth_ -= frame->line_count();
  BATT_CHECK_LE(this->depth_, old_depth);

  frame->pushed_to_ = nullptr;

  VLOG(1) << "await_frame_consumed()... depth -> " << this->depth_;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeScanner::advance()
{
  if (this->at_end_) {
    return;
  }

  this->next_item_ = None;

  if (this->needs_input_) {
    if (this->inside_) {
      this->needs_input_ = false;
      this->inside_ = this->inside_.resume();
    } else {
      this->at_end_ = true;
      return;
    }
  }

  SmallVec<Ref<MergeLine>, 64> lines_to_replace;

  while (!this->by_front_key_.empty()) {
    MergeLine& line = this->by_front_key_.top().get();
    EditSlice& first_slice = line.first_;

    bool done = false;

    batt::case_of(first_slice, [&](auto& slice_case) {
      BATT_CHECK(!slice_case.empty());

      const EditView& slice_front = to_edit_view(slice_case.front());

      if (!this->next_item_) {
        this->next_item_.emplace(slice_front);

      } else if (get_key(slice_front) == get_key(*this->next_item_)) {
        if (this->next_item_->needs_combine()) {
          *this->next_item_ = combine(*this->next_item_, slice_front);
        }

      } else {
        done = true;
        return;
      }

      this->by_front_key_.pop();

      // Consume the first item in the current slice.
      //
      slice_case.drop_front();

      // The current line should be pushed back onto the heap if it is not empty.
      //
      if (!slice_case.empty() || line.advance()) {
        lines_to_replace.emplace_back(as_ref(line));

      } else {
        // Line is empty; deactivate.
        //
        const usize index_in_frame = line.get_index_in_frame();
        line.frame_->active_mask_ &= ~(u64{1} << index_in_frame);
        this->needs_input_ = true;
      }
    });

    if (done) {
      break;
    }
  }

  for (const Ref<MergeLine>& line : lines_to_replace) {
    this->by_front_key_.push(line);
  }

  if (this->by_front_key_.empty()) {
    this->at_end_ = true;
  }
}

}  // namespace turtle_kv
