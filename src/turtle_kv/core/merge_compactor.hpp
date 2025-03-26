#pragma once

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/merge_frame.hpp>
#include <turtle_kv/core/merge_line.hpp>

#include <turtle_kv/util/flatten.hpp>

#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/ref.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/async/continuation.hpp>
#include <batteries/async/worker_pool.hpp>
#include <batteries/small_vec.hpp>
#include <batteries/status.hpp>

#include <array>
#include <atomic>
#include <vector>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Implements the core edit multi-level merge/compaction algorithm.
//
class MergeCompactor
{
 public:
  static std::atomic<bool>& debug_log_on()
  {
    static std::atomic<bool> value_{false};
    return value_;
  }

  using FrontKeyHeap = MergeLine::FrontKeyHeap;
  using BackKeyHeap = MergeLine::BackKeyHeap;

  class GeneratorContext;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // A function that produces stratified lines of edits to merge.
  //
  // This function may recursively traverse some structure (e.g., a tree), threading the
  // GeneratorContext down the stack and calling `push_frame` as it descends. `await_frame_consumed`
  // should be called on the way back out of this recursive call path, to guarantee that no
  // MergeFrames are scope-destroyed while still in use by the consumer!
  //
  using GeneratorFn = std::function<Status(GeneratorContext& context)>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Passed to the GeneratorFn to allow it to emit frames for merge/compaction.  MergeFrames pushed
  // `first` are assumed to contain more recent Edits that should take priority over MergeFrames
  // pushed later.
  //
  class GeneratorContext
  {
   public:
    friend class MergeCompactor;

    void push_frame(MergeFrame* frame)
    {
      this->compactor_->push_frame_impl(frame);
    }

    Status await_frame_consumed(MergeFrame* frame)
    {
      return this->compactor_->await_frame_consumed_impl(frame);
    }

    bool is_stop_requested() const
    {
      return this->compactor_->is_stop_requested_impl();
    }

   private:
    explicit GeneratorContext(MergeCompactor* compactor) noexcept : compactor_{compactor}
    {
    }

    MergeCompactor* compactor_;
  };

  template <bool kDecayToItems>
  class ResultSet;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // A buffer that collects merge/compacted edits.
  //
  template <bool kDecayToItems>
  class OutputBuffer
  {
   public:
    template <bool>
    friend class ResultSet;

    friend class MergeCompactor;

    using value_type = std::conditional_t<kDecayToItems, ItemView, EditView>;

    Slice<const value_type> get() const
    {
      return this->merged_;
    }

    void reset()
    {
      this->merged_ = as_slice(this->merged_.begin(), 0);
    }

   private:
    std::array<std::vector<EditView>, 3> buffer_;
    Slice<const value_type> merged_{reinterpret_cast<const value_type*>(this->buffer_[0].data()),
                                    reinterpret_cast<const value_type*>(this->buffer_[0].data())};
  };

  using EditBuffer = OutputBuffer</*kDecayToItems=*/false>;
  using ItemBuffer = OutputBuffer</*kDecayToItems=*/true>;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  template <bool kDecayToItems>
  class ResultSet
  {
   public:
    using Self = ResultSet;

    static constexpr usize kDefaultChunkCount = 4;

    using value_type = std::conditional_t<kDecayToItems, ItemView, EditView>;

    static ResultSet from(OutputBuffer<kDecayToItems>&& output);

    /** \brief Returns the concatenation of the passed ResultSet objects, consuming them in the
     * process.
     */
    static ResultSet concat(ResultSet&& first, ResultSet&& second) noexcept;

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

    ResultSet() = default;

    ResultSet(const ResultSet&) = default;
    ResultSet& operator=(const ResultSet&) = default;

    ResultSet(ResultSet&&) = default;
    ResultSet& operator=(ResultSet&&) = default;

    auto get() const
    {
      return flatten(this->chunks_.begin(), std::prev(this->chunks_.end()));
    }

    void clear() noexcept
    {
      *this = ResultSet{};
    }

    /** \brief Returns the number of live edits in this result set.
     */
    usize size() const noexcept
    {
      const usize n = this->chunks_.back().offset;
      BATT_CHECK_EQ(n, this->get().size());
      return n;
    }

    void append(OutputBuffer<kDecayToItems>&& output);

    void append(std::vector<EditView>&& buffer, const Slice<const value_type>& items);

    void append(std::vector<EditView>&& buffer);

    /** \brief Queries the live item set for the given key.
     */
    StatusOr<ValueView> find_key(const KeyView& key) const noexcept;

    /** \brief Filters out the passed key range from the result set.
     */
    void drop_key_range(const CInterval<KeyView>& dropped_key_range);

    /** \brief Filters out the passed key range from the result set.
     */
    void drop_key_range_half_open(const Interval<KeyView>& dropped_key_range);

    /** \brief Filters out all edits/items after the specified position.
     */
    void drop_after_n(usize n_to_take) noexcept;

    /** \brief Drops the specified number of items from the beginning of the result set.
     */
    void drop_before_n(usize n_to_skip) noexcept;

    /** \brief Verify that all internal invariants hold; if not, panic.
     */
    void check_invariants() const noexcept;

    /** \brief Returns a function that dumps detailed information about this result set, for
     * diagnostic/debug purposes.
     */
    batt::SmallFn<void(std::ostream&)> debug_dump() const noexcept;

    /** \brief Returns the result set as a sequence of EditSlice instances.
     */
    BoxedSeq<EditSlice> live_edit_slices(const KeyView& lower_bound = KeyView{}) const noexcept;

    /** \brief Returns the min key in the live range.
     */
    KeyView get_min_key() const noexcept;

    /** \brief Returns the max key in the live range.
     */
    KeyView get_max_key() const noexcept;

    /** \brief Returns the closed interval of keys in the live range.
     */
    CInterval<KeyView> get_key_crange() const noexcept
    {
      return {this->get_min_key(), this->get_max_key()};
    }

    /** \brief Returns true iff this result set has no items (or all items have been
     * dropped/filtered).
     */
    bool empty() const noexcept;

    /** \brief Returns the number of packed bytes in this result set.
     */
    u64 get_packed_size() const noexcept;

    /** \brief Returns whether this ResultSet is marked as having page ref values.
     */
    HasPageRefs has_page_refs() const noexcept
    {
      return this->has_page_refs_;
    }

    /** \brief Sets whether this ResultSet is marked as having page ref values.
     */
    void set_has_page_refs(bool b) noexcept
    {
      this->has_page_refs_ = HasPageRefs{b};
    }

    /** \brief Sets this->has_page_refs() to true if the passed value is true; leaves it unchanged
     * otherwise.
     */
    void update_has_page_refs(bool b) noexcept
    {
      this->set_has_page_refs(this->has_page_refs_ || b);
    }

    /** \brief Verifies that the active set is sorted in key order; panics if not.
     */
    void check_items_sorted() const noexcept;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    static constexpr u64 kInvalidPackedSize = ~u64{0};

    void invalidate_packed_size() noexcept;

    std::atomic<u64>& packed_size() const noexcept
    {
      return *(std::atomic<u64>*)(&this->packed_size_);
    }

    template <typename IntervalT>
    void drop_key_range_impl(const IntervalT& dropped_key_range);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SmallVec<std::shared_ptr<const std::vector<EditView>>, kDefaultChunkCount> buffers_;
    SmallVec<Chunk<const value_type*>, kDefaultChunkCount + 1> chunks_ = {
        make_end_chunk<const value_type*>()};

    mutable u64 packed_size_{Self::kInvalidPackedSize};
    HasPageRefs has_page_refs_{false};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MergeCompactor(batt::WorkerPool& worker_pool) noexcept;

  ~MergeCompactor() noexcept;

  void set_generator(GeneratorFn&& fn);

  Status read_some(EditBuffer& output, const KeyView& max_key);

  Status read_some(ItemBuffer& output, const KeyView& max_key);

  template <bool kDecayToItems>
  StatusOr<ResultSet<kDecayToItems>> read(OutputBuffer<kDecayToItems>& output,
                                          const KeyView& max_key)
  {
    ResultSet<kDecayToItems> result;
    for (;;) {
      Status status = this->read_some(output, max_key);
      BATT_REQUIRE_OK(status);

      if (output.get().empty()) {
        break;
      }
      result.append(std::move(output));
    }
    return result;
  }

  void stop();

 private:
  template <bool kDecayToItems>
  Status read_some_impl(OutputBuffer<kDecayToItems>& buffer, const KeyView& max_key_limit);

  void push_frame_impl(MergeFrame* frame);

  Status await_frame_consumed_impl(MergeFrame* frame);

  bool is_stop_requested_impl() const;

  // Returns the minimum of the max keys of all active lines; this is the (inclusive/closed) upper
  // bound for the next slice-wise merge/compact operation.
  //
  KeyView next_cut_point() const;

  // Pop all active lines with prefixes that contain keys that overlap with max_key.
  //
  void pop_lines(const KeyView& max_key, SmallVecBase<Ref<MergeLine>>& lines_out);

  // Return the total number of items in all slices pushed onto the output vec,
  // `edit_slices_to_merge`.
  //
  usize cut_lines(const Slice<const Ref<MergeLine>>& src_lines,
                  SmallVecBase<EditSlice>& dst_slices,
                  const KeyView& max_key);

  // For each line `l` in the passed slice:
  //   If `l` is empty, mark it as inactive in its containing frame and remove it entirely from the
  //   heaps Else, push line onto heaps for future merging
  //
  void push_lines(const Slice<const Ref<MergeLine>>& lines);

  // Collect all EditViews in the src_slices, copying them to dst_edit_buffer in preparation for
  // merge/compact.
  //
  template <typename T>
  void collect_edits(const Slice<const EditSlice>& src_slices,
                     const Slice<T>& dst_edit_buffer,
                     SmallVecBase<Slice<const T>>& dst_slices);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::WorkerPool& worker_pool_;

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

  // A binary heap storing all the active frame lines by back (max) key of the first slice.
  //
  BackKeyHeap by_back_key_;

  // The current depth of  the stack.
  //
  usize depth_ = 0;

  // If the generator has reported failure, this is the status code.
  //
  Status generator_status_;

  // Set to true if `this->stop()` has been called.
  //
  bool stop_requested_ = false;
};

}  // namespace turtle_kv
