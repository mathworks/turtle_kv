#pragma once

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/merge_compactor_base.hpp>
#include <turtle_kv/core/merge_frame.hpp>
#include <turtle_kv/core/merge_line.hpp>

#include <turtle_kv/util/preallocated_stack.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/async/continuation.hpp>

#include <functional>

namespace turtle_kv {

/** \brief Single-threaded version of MergeCompactor, optimized for short scans, single item at a
 * time.
 */
class MergeScanner : public MergeCompactorBase
{
 public:
  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class GeneratorContext
  {
   public:
    friend class MergeScanner;

    void push_frame(MergeFrame* frame)
    {
      this->scanner_->push_frame_impl(frame);
    }

    Status await_frame_consumed(MergeFrame* frame)
    {
      return this->scanner_->await_frame_consumed_impl(frame);
    }

    bool is_stop_requested() const
    {
      return this->scanner_->is_stop_requested_impl();
    }

   private:
    explicit GeneratorContext(MergeScanner* scanner) noexcept : scanner_{scanner}
    {
    }

    MergeScanner* scanner_;
  };

  using FrontKeyHeap = boost::heap::d_ary_heap<Ref<MergeLine>,                                 //
                                               boost::heap::arity<2>,                          //
                                               boost::heap::compare<MinKeyAndDepthHeapOrder>,  //
                                               boost::heap::mutable_<false>                    //
                                               >;

  // A function that produces stratified lines of edits to merge.
  //
  // This function may recursively traverse some structure (e.g., a tree), threading the
  // GeneratorContext down the stack and calling `push_frame` as it descends. `await_frame_consumed`
  // should be called on the way back out of this recursive call path, to guarantee that no
  // MergeFrames are scope-destroyed while still in use by the consumer!
  //
  using GeneratorFn = std::function<Status(GeneratorContext& context)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeScanner() noexcept;

  ~MergeScanner() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status status() const
  {
    return this->generator_status_;
  }

  //----- --- -- -  -  -   -

  void set_generator(GeneratorFn&& fn);

  void stop();

  Optional<Item> peek()
  {
    if (!this->next_item_ && !this->at_end_) {
      this->advance();
    }
    return this->next_item_;
  }

  Optional<Item> next()
  {
    Optional<Item> item = this->peek();
    this->next_item_ = None;
    return item;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void push_frame_impl(MergeFrame* frame);

  Status await_frame_consumed_impl(MergeFrame* frame);

  void advance();

  //----- --- -- -  -  -   -

  bool is_stop_requested_impl() const
  {
    return this->stop_requested_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The visitor coroutine; the code that runs on this stack recursively traverses the tree, calling
  // push_frame and await_frame_consumed as it goes.
  //
  batt::Continuation inside_;

  // The outer coroutine.
  //
  batt::Continuation outside_;

  // A binary heap storing all the active frame lines by front (min) key of the first slice.
  //
  FrontKeyHeap by_front_key_;

  // The current depth of the stack.
  //
  usize depth_ = 0;

  // If the generator has reported failure, this is the status code.
  //
  Status generator_status_;

  // Set to true if `this->stop()` has been called.
  //
  bool stop_requested_ = false;

  // The next item to consume.
  //
  Optional<EditView> next_item_;

  // Set to true when all merge lines have been consumed and there are no more items to scan.
  //
  bool at_end_ = false;

  // Set to true whenever a merge line is deactivated, to allow the producer coroutine to make
  // progress.
  //
  bool needs_input_ = false;

  // Inline allocation of producer coroutine's stack.
  //
  std::aligned_storage_t<512 * kKiB, 64> inside_stack_memory_;

  // The stack allocator to pass to batt::callcc.
  //
  PreallocatedStack inside_stack_{
      MutableBuffer{&this->inside_stack_memory_, sizeof(this->inside_stack_memory_)}};
};

}  // namespace turtle_kv
