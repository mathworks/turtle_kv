#include <turtle_kv/core/merge_compactor.hpp>
//

#include <turtle_kv/core/algo/merge_compact_edits.hpp>
#include <turtle_kv/core/key_range.hpp>
#include <turtle_kv/core/packed_sizeof_edit.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <boost/context/pooled_fixedsize_stack.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::MergeCompactor(batt::WorkerPool& worker_pool) noexcept : worker_pool_{worker_pool}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::~MergeCompactor() noexcept
{
  this->stop();

  BATT_CHECK(!this->outside_);
  BATT_CHECK(!this->inside_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeCompactor::set_generator(GeneratorFn&& fn)
{
  this->inside_ = callcc(batt::StackSize{512 * kKiB}, batt::StackType::kFixedSize,
                         [this, fn = std::move(fn)](batt::Continuation&& origin) {
                           this->outside_ = origin.resume();
                           {
                             GeneratorContext context{this};
                             this->generator_status_ = fn(context);
                           }
                           return std::move(this->outside_);
                         });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MergeCompactor::read_some(EditBuffer& buffer, const KeyView& max_key)
{
  return this->read_some_impl(buffer, max_key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MergeCompactor::read_some(ItemBuffer& buffer, const KeyView& max_key)
{
  return this->read_some_impl(buffer, max_key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
Status MergeCompactor::read_some_impl(OutputBuffer<kDecayToItems>& output,
                                      const KeyView& max_key_limit)
{
  output.reset();
  for (;;) {
    LOG_IF(INFO, debug_log_on()) << "(top of read_some loop)";

    // If the visitation coroutine is still active, then resume it to gather more data.  If it has
    // finished, then just continue on.
    //
    if (this->inside_) {
      this->inside_ = this->inside_.resume();
      BATT_REQUIRE_OK(this->generator_status_);
    }

    // If we are out of segments to merge, then return.
    //
    if (this->by_back_key_.empty()) {
      LOG_IF(INFO, debug_log_on()) << "back key heap is empty; returning";
      BATT_CHECK(this->by_front_key_.empty());
      break;
    }

    // Find the (inclusive/closed) least upper bound for all active stack levels and cut there.
    //
    const KeyView cut_point_max_key = this->next_cut_point();
    const KeyView max_key = std::min(max_key_limit, cut_point_max_key);
    LOG_IF(INFO, debug_log_on()) << "read_impl; least max_key=" << batt::c_str_literal(max_key)
                                 << " cut_point_max_key=" << batt::c_str_literal(cut_point_max_key)
                                 << " max_key_limit=" << batt::c_str_literal(max_key_limit);

    SmallVec<EditSlice, 64> edit_slices_to_merge;
    const usize total_items = [&] {
      SmallVec<Ref<MergeLine>, 64> lines_to_merge;
      this->pop_lines(max_key, lines_to_merge);

      // Cut each line at the least max_key we calculated above, updating both priority heaps as we
      // go.
      //
      const usize total_items =
          this->cut_lines(as_slice(lines_to_merge), edit_slices_to_merge, max_key);

      // Remove any empty lines, and push any non-empty lines back onto the heaps.
      //
      this->push_lines(as_slice(lines_to_merge));

      return total_items;
    }();

    // After selecting the next lines to merge and applying the cut, we may find that there are no
    // slices at any level whose lower bound is <= max_key; if so, bail out now because we'll never
    // make progress.
    //
    if (edit_slices_to_merge.empty() || total_items == 0) {
      break;
    }

    // Reserve buffer space.
    //
    for (auto& buffer : output.buffer_) {
      buffer.reserve(total_items);
    }

    SmallVec<Slice<const EditView>, 64> collected_edit_slices;

    // Copy slice data into buffer_[0] as ItemT (EditView or ItemView) to prepare for merge.
    //
    this->collect_edits(as_slice(edit_slices_to_merge),
                        as_slice(output.buffer_[0].data(), total_items), collected_edit_slices);

    LOG_IF(INFO, debug_log_on()) << "merge_compact_edits(collected_edit_slices="
                                 << batt::dump_range(as_const_slice(collected_edit_slices),
                                                     batt::Pretty::True)
                                 << ")";

    // Do the merge/compact.
    //
    output.merged_ = merge_compact_edits(this->worker_pool_, as_const_slice(collected_edit_slices),
                                         as_slice(output.buffer_[1].data(), total_items),
                                         as_slice(output.buffer_[2].data(), total_items),
                                         DecayToItem<kDecayToItems>{});

    // If we are using DecayToItem<true>, the merge/compact operation may result in an empty output
    // for non-empty input.  This isn't an error, nor is it a reason to stop going; if `merged_` is
    // empty at this point, then loop back to the top of this function and try again.
    //
    if (!output.merged_.empty()) {
      LOG_IF(INFO, debug_log_on()) << "output is non-empty; exiting `read_some` loop.";
      break;
    }
  }

  LOG_IF(INFO, debug_log_on()) << "filled buffer with merged items: " << std::endl
                               <<
      [&](std::ostream& out) {
        constexpr usize kLimit = 10;
        for (usize i = 0; i < kLimit; ++i) {
          if (i >= output.merged_.size()) {
            break;
          }
          out << "   " << output.merged_[i] << std::endl;
        }
        if (output.merged_.size() > kLimit) {
          out << "   ... (" << (output.merged_.size() - kLimit) << " more items)" << std::endl;
        }
      };

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeyView MergeCompactor::next_cut_point() const
{
  const MergeLine& line = this->by_back_key_.top().get();

  LOG_IF(INFO, debug_log_on()) << BATT_INSPECT(this->by_back_key_.size())
                               << " line.max_key=" << batt::c_str_literal(get_max_key(line))
                               << " line=" << debug_print(line) <<
      [&](std::ostream& out) {
        out << "this->by_back_key_ = " << std::endl;
        for (const MergeLine& line : this->by_back_key_) {
          out << " .. "
              << "line.max_key=" << batt::c_str_literal(get_max_key(line))
              << " line=" << debug_print(line) << std::endl;
        }
      };

  return get_max_key(line);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeCompactor::pop_lines(const KeyView& max_key, SmallVecBase<Ref<MergeLine>>& lines_out)
{
  lines_out.clear();

  // Collect all the slices that overlap the merge range.  THIS WILL push the lines in min-key
  // order, not depth order (update/time order), which is what we need for the merge/compact; we fix
  // this with the `std::sort` below.
  //
  while (!this->by_front_key_.empty()) {
    Ref<MergeLine> line = this->by_front_key_.top();
    if (line.get().begins_after(max_key)) {
      VLOG(1) << " .. skipping line at depth=" << line.get().depth() << "; " << debug_print(line);
      break;
    }
    VLOG(1) << " .. adding line at depth=" << line.get().depth() << "; " << debug_print(line);
    lines_out.emplace_back(line);
    this->by_front_key_.pop();
    //
    // We will call update later to rearrange `this->by_back_key_`.
  }

  // Sort by depth.
  //
  std::sort(lines_out.begin(), lines_out.end(), [](const MergeLine& l, const MergeLine& r) {
    return l.depth() < r.depth();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MergeCompactor::cut_lines(const Slice<const Ref<MergeLine>>& src_lines,
                                SmallVecBase<EditSlice>& dst_slices, const KeyView& max_key)
{
  // Cut each line at the least max_key we calculated above, updating both priority heaps as we go.
  //
  usize total_items = 0;

  for (const Ref<MergeLine>& line_ref : src_lines) {
    MergeLine& line = line_ref.get();
    {
      EditSlice line_prefix = line.cut(max_key);
      LOG_IF(INFO, debug_log_on())
          << "(cut_lines) line_prefix=" << batt::dump_range(line_prefix, batt::Pretty::True);
      if (!is_empty(line_prefix)) {
        total_items += get_item_count(line_prefix);
        VLOG(1) << " .. slice to merge: "
                << "(depth=" << line.depth() << ")" << BATT_INSPECT(total_items);
        dst_slices.emplace_back(line_prefix);
      }
    }
    if (line.empty()) {
      LOG_IF(INFO, debug_log_on()) << "line is now empty; removing from the back key heap";
      this->by_back_key_.erase(line.back_handle_);
    } else {
      LOG_IF(INFO, debug_log_on())
          << "updating line in back key heap: line.max_key="
          << batt::c_str_literal(get_max_key(line)) << " line=" << debug_print(line);
      this->by_back_key_.update(line.back_handle_);
    }
  }

  return total_items;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeCompactor::push_lines(const Slice<const Ref<MergeLine>>& lines)
{
  for (const Ref<MergeLine>& line_ref : lines) {
    MergeLine& line = line_ref.get();

    // If prior `cut` operations consumed all of `line`, then pop it from various heaps and report
    // it as inactive.  Otherwise update its position in both heaps.
    //
    if (line.empty()) {
      // (We already removed this line from the `by_front_key_` heap when we added it to `src_lines`
      // above, so just don't push it back on.)
      line.frame_->active_mask_ &= ~(u64{1} << line.get_index_in_frame());

      VLOG(1) << "deactivating " << debug_print(line) << "; " << [this](std::ostream& out) {
        out << "this->by_back_key_ = " << std::endl;
        for (const MergeLine& line : this->by_back_key_) {
          out << " .. " << debug_print(line) << std::endl;
        }
      };
    } else {
      this->by_front_key_.push(line_ref);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
void MergeCompactor::collect_edits(const Slice<const EditSlice>& src_slices,
                                   const Slice<T>& dst_edit_buffer,
                                   SmallVecBase<Slice<const T>>& dst_slices)
{
  batt::ScopedWorkContext context{this->worker_pool_};

  EditView* dst = dst_edit_buffer.begin();
  for (const auto& edit_slice : src_slices) {
    auto work_fn = [edit_slice, dst, &context, this] {
      batt::case_of(edit_slice, [&](const auto& slice) {
        const ParallelAlgoDefaults& algo_defaults = parallel_algo_defaults();
        parallel_copy(context, slice.begin(), slice.end(), dst,
                      /*min_task_size = */ algo_defaults.copy_edits.min_task_size,
                      /*max_tasks = */ batt::TaskCount{this->worker_pool_.size() - 1});
      });
    };

    if (&edit_slice == &src_slices.back()) {
      work_fn();
    } else {
      BATT_CHECK_OK(context.async_run(std::move(work_fn)))
          << "this->worker_pool_ must not be closed!";
    }

    const usize edit_slice_size = get_item_count(edit_slice);
    dst_slices.emplace_back(as_slice(dst, edit_slice_size));
    dst += edit_slice_size;
    BATT_CHECK_LE(dst, dst_edit_buffer.end());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeCompactor::stop()
{
  this->stop_requested_ = true;

  if (this->inside_) {
    this->inside_ = this->inside_.resume();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MergeCompactor::push_frame_impl(MergeFrame* frame)
{
  VLOG(1) << "push_frame() entered " << BATT_INSPECT(this->depth_);

  BATT_CHECK_LE(frame->lines_.size(), 64);
  BATT_CHECK_LE(frame->lines_.size() + this->depth_, 64);

  frame->active_mask_ = 0;

  BATT_CHECK_EQ(frame->pushed_to_, nullptr)
      << "This frame is already pushed onto a different compactor's stack!";
  frame->pushed_to_ = this;

  for (usize i = 0; i < frame->lines_.size(); ++i) {
    this->depth_ += 1;
    MergeLine& line = frame->lines_[i];
    BATT_CHECK_EQ(frame, line.frame_);
    line.depth_ = this->depth_;
    if (!line.empty()) {
      BATT_CHECK_EQ(line.get_index_in_frame(), i);
      frame->active_mask_ |= (u64{1} << i);
      VLOG(1) << " .. frame.line[" << i << "] activated;";
      this->by_front_key_.push(as_ref(line));
      line.back_handle_ = this->by_back_key_.push(as_ref(line));
    } else {
      VLOG(1) << " .. frame.line[" << i << "] empty -- NOT active, ignoring;";
    }
  }
  VLOG(1) << "push_frame() finished " << BATT_INSPECT(this->depth_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MergeCompactor::await_frame_consumed_impl(MergeFrame* frame)
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
  this->depth_ -= frame->lines_.size();
  BATT_CHECK_LE(this->depth_, old_depth);

  frame->pushed_to_ = nullptr;

  VLOG(1) << "await_frame_consumed()... depth -> " << this->depth_;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MergeCompactor::is_stop_requested_impl() const
{
  return this->stop_requested_;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <bool kDecayToItems>
/*static*/ auto MergeCompactor::ResultSet<kDecayToItems>::from(OutputBuffer<kDecayToItems>&& output)
    -> ResultSet
{
  ResultSet result;
  result.append(std::move(output));
  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
/*static*/ auto MergeCompactor::ResultSet<kDecayToItems>::concat(ResultSet&& first,
                                                                 ResultSet&& second) noexcept
    -> ResultSet
{
  ResultSet ans;

  //----- --- -- -  -  -   -
  // Paranoid check.
  //
  if (false) {
    std::for_each(first.chunks_.begin(), std::prev(first.chunks_.end()), [](const auto& chunk) {
      BATT_CHECK(!chunk.items.empty());
    });
    std::for_each(second.chunks_.begin(), std::prev(second.chunks_.end()), [](const auto& chunk) {
      BATT_CHECK(!chunk.items.empty());
    });
  }
  // (end of paranoid checking)
  //----- --- -- -  -  -   -

  //----- --- -- -  -  -   -
  ans.buffers_.insert(ans.buffers_.end(),                               //
                      std::make_move_iterator(first.buffers_.begin()),  //
                      std::make_move_iterator(first.buffers_.end()));

  ans.buffers_.insert(ans.buffers_.end(),                                //
                      std::make_move_iterator(second.buffers_.begin()),  //
                      std::make_move_iterator(second.buffers_.end()));

  //----- --- -- -  -  -   -
  ans.chunks_.clear();

  BATT_CHECK(ans.chunks_.empty());
  BATT_CHECK(!first.chunks_.empty()) << "Chunk ranges must have an empty end chunk!";
  BATT_CHECK(!second.chunks_.empty()) << "Chunk ranges must have an empty end chunk!";

  ans.chunks_.insert(ans.chunks_.end(),                               //
                     std::make_move_iterator(first.chunks_.begin()),  //
                     std::make_move_iterator(first.chunks_.end()));

  // Discard the last (empty) chunk of `ans`, but remember its offset so we can shift the chunks
  // from `second` below.
  //
  const isize first_size = ans.chunks_.back().offset;
  ans.chunks_.pop_back();

  auto iter2 = ans.chunks_.insert(ans.chunks_.end(),                                //
                                  std::make_move_iterator(second.chunks_.begin()),  //
                                  std::make_move_iterator(second.chunks_.end()));

  if (iter2 != ans.chunks_.end()) {
    BATT_CHECK_EQ(iter2->offset, 0);
  }

  // Shift the offsets for all chunks from `second` by the final offset of `first`.
  //
  std::for_each(iter2, std::prev(ans.chunks_.end()),
                [first_size](Chunk<const value_type*>& chunk_from_second) {
                  chunk_from_second.offset += first_size;
                });

  first.clear();
  second.clear();

  BATT_CHECK(!ans.chunks_.empty());

  //----- --- -- -  -  -   -
  // Paranoid check.
  //
  if (false) {
    std::for_each(ans.chunks_.begin(), std::prev(ans.chunks_.end()), [&](const auto& chunk) {
      BATT_CHECK(!chunk.items.empty()) << [&](std::ostream& out) {
        usize i = 0;
        for (const auto& c : ans.chunks_) {
          out << "\n  [" << i << "] " << BATT_INSPECT(c.offset) << BATT_INSPECT(c.items.size());
          ++i;
        }
      };
    });
  }
  // (end of paranoid checking)
  //----- --- -- -  -  -   -

  return ans;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::append(OutputBuffer<kDecayToItems>&& output)
{
  if (output.merged_.empty()) {
    return;
  }

  auto it = std::find_if(output.buffer_.begin(), output.buffer_.end(),
                         [&](const std::vector<EditView>& b) {
                           return static_cast<const void*>(b.data()) ==
                                  static_cast<const void*>(output.merged_.begin());
                         });
  BATT_CHECK_NE(it, output.buffer_.end());
  std::vector<EditView>& active_buffer = *it;

  this->append(std::move(active_buffer), output.merged_);

  output.merged_ = Slice<const value_type>{nullptr, nullptr};

  BATT_CHECK(active_buffer.empty());
  BATT_CHECK(output.get().empty());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::append(std::vector<EditView>&& buffer,
                                                      const Slice<const value_type>& items)
{
  if (items.empty()) {
    return;
  }

  // TODO [tastolfi 2024-11-04] incrementally recalculate
  //
  this->invalidate_packed_size();

  const auto new_inner_end = items.end();
  const isize new_size = this->chunks_.back().offset + items.size();

  this->buffers_.emplace_back(std::make_shared<std::vector<EditView>>(std::move(buffer)));
  this->chunks_.back().items = items;
  this->chunks_.emplace_back(make_end_chunk(new_size, new_inner_end));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::append(std::vector<EditView>&& buffer)
{
  Slice<const value_type> all_items{(const value_type*)buffer.data(),
                                    (const value_type*)(buffer.data() + buffer.size())};
  this->append(std::move(buffer), all_items);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::check_items_sorted() const noexcept
{
  const auto items_slice = this->get();

  BATT_CHECK(std::is_sorted(items_slice.begin(), items_slice.end(), KeyOrder{}))
      << [&](std::ostream& out) {
           auto iter = items_slice.begin();
           auto prev = iter;
           auto last = items_slice.end();
           usize i = 0;
           for (; iter != last; prev = iter, ++iter, ++i) {
             if (get_key(*prev) > get_key(*iter)) {
               out << "\n  at " << i << ", keys out of order: " << std::endl
                   << BATT_INSPECT_STR(get_key(*prev)) << std::endl
                   << BATT_INSPECT_STR(get_key(*iter)) << std::endl;
             }
           }
         };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
StatusOr<ValueView> MergeCompactor::ResultSet<kDecayToItems>::find_key(
    const KeyView& key) const noexcept
{
  const auto items_slice = this->get();
  const auto result = std::equal_range(items_slice.begin(), items_slice.end(), key, KeyOrder{});

  if (result.first == result.second) {
    return {batt::StatusCode::kNotFound};
  }

  return {get_value(*result.first)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
batt::SmallFn<void(std::ostream&)> MergeCompactor::ResultSet<kDecayToItems>::debug_dump()
    const noexcept
{
  return [this](std::ostream& out) {
    for (const auto& chunk : this->chunks_) {
      if (chunk.empty()) {
        out << "\n  {}";
      } else {
        out << "\n  " << batt::c_str_literal(get_key(chunk.items.front())) << ".."
            << batt::c_str_literal(get_key(chunk.items.back()));
      }
    }
    out << "\n";
    out << BATT_INSPECT_RANGE_PRETTY(this->chunks_) << "\n"
        << BATT_INSPECT_RANGE_PRETTY(this->buffers_);
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
template <typename IntervalT>
void MergeCompactor::ResultSet<kDecayToItems>::drop_key_range_impl(
    const IntervalT& dropped_key_range)
{
  using IntervalTraits = typename IntervalT::traits_type;

  DVLOG(1) << "--------------------------------------\ndrop_key_range("
           << batt::c_str_literal(dropped_key_range.lower_bound) << ".."
           << batt::c_str_literal(dropped_key_range.upper_bound) << ")" << this->debug_dump();

  auto [outer_first, outer_last] =
      std::equal_range(this->chunks_.begin(), std::prev(this->chunks_.end()), dropped_key_range,
                       ExtendedKeyRangeOrder{});

  // Base case 1: no match, do nothing.
  //
  if (outer_first == outer_last) {
    return;
  }

  // TODO [tastolfi 2024-11-04] incrementally recalculate
  //
  this->invalidate_packed_size();

  // Base case 2: single chunk match.
  //
  if (outer_last == std::next(outer_first)) {
    const auto chunk_items_begin = outer_first->items.begin();
    const auto chunk_items_end = outer_first->items.end();

    BATT_CHECK_NE(chunk_items_begin, chunk_items_end);

    const auto [inner_first, inner_last] = std::equal_range(
        chunk_items_begin, chunk_items_end, dropped_key_range, ExtendedKeyRangeOrder{});

    const isize n_dropped = std::distance(inner_first, inner_last);
    if (n_dropped == 0) {
      return;
    }

    Chunk<const value_type*> lower_split_chunk{
        .offset = outer_first->offset,
        .items = boost::iterator_range<const value_type*>{chunk_items_begin, inner_first},
    };

    Chunk<const value_type*> upper_split_chunk{
        .offset = static_cast<isize>(lower_split_chunk.offset + lower_split_chunk.items.size()),
        .items = boost::iterator_range<const value_type*>{inner_last, chunk_items_end},
    };

    if (lower_split_chunk.empty()) {
      if (upper_split_chunk.empty()) {
        outer_last = this->chunks_.erase(outer_first);
      } else {
        *outer_first = upper_split_chunk;
      }
    } else {
      *outer_first = lower_split_chunk;

      if (!upper_split_chunk.empty()) {
        auto iter = this->chunks_.insert(outer_last, upper_split_chunk);
        outer_last = std::next(iter);
      }
    }

    std::for_each(outer_last, this->chunks_.end(),
                  [&](Chunk<const value_type*>& chunk_after_insert) {
                    chunk_after_insert.offset -= n_dropped;
                  });
    return;
  }

  // General case: multiple chunks match.
  //
  // We will trim items off the first and last chunks in the matched range, then erase all
  // chunks in between, and finally shift the offset of all chunks after the matched range by
  // the amount of items dropped.
  //
  isize n_dropped = 0;

  // Trim front.
  //
  auto middle_first = outer_first;
  {
    auto& front_chunk = *outer_first;

    auto chunk_items_begin = front_chunk.items.begin();
    auto chunk_items_end = front_chunk.items.end();

    auto inner_first = std::lower_bound(chunk_items_begin, chunk_items_end,
                                        dropped_key_range.lower_bound, KeyOrder{});

    if (inner_first != chunk_items_begin) {
      ++middle_first;
      n_dropped += std::distance(inner_first, chunk_items_end);
      front_chunk.items = boost::make_iterator_range(chunk_items_begin, inner_first);
    }
  }

  // Trim back.
  //
  auto middle_last = outer_last;
  {
    auto& back_chunk = *std::prev(outer_last);

    auto chunk_items_begin = back_chunk.items.begin();
    auto chunk_items_end = back_chunk.items.end();

    auto inner_last = std::lower_bound(chunk_items_begin, chunk_items_end,
                                       dropped_key_range.upper_bound, KeyOrder{});

    while (inner_last != chunk_items_end &&
           !IntervalTraits::x_excluded_by_upper(get_key(*inner_last),  //
                                                dropped_key_range.upper_bound)) {
      ++inner_last;
    }

    if (inner_last != chunk_items_end) {
      --middle_last;
      const isize n_dropped_back_chunk = std::distance(chunk_items_begin, inner_last);
      n_dropped += n_dropped_back_chunk;
      back_chunk.offset += n_dropped_back_chunk;
      back_chunk.items.advance_begin(n_dropped_back_chunk);
      BATT_CHECK(!back_chunk.items.empty());
    }
  }

  // Count dropped items in middle and erase.
  //
  std::for_each(middle_first, middle_last,
                [&n_dropped](const Chunk<const value_type*>& middle_chunk) {
                  n_dropped += middle_chunk.items.size();
                });

  middle_last = this->chunks_.erase(middle_first, middle_last);

  // Shift offsets down (after matched range).
  //
  std::for_each(middle_last, this->chunks_.end(),
                [n_dropped](Chunk<const value_type*>& chunk_after_dropped) {
                  chunk_after_dropped.offset -= n_dropped;
                });

  // Done!
  //
  DVLOG(1) << this->debug_dump();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::drop_key_range(
    const CInterval<KeyView>& dropped_key_range)
{
  this->drop_key_range_impl(dropped_key_range);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::drop_key_range_half_open(
    const Interval<KeyView>& dropped_key_range)
{
  this->drop_key_range_impl(dropped_key_range);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::drop_after_n(usize n_to_take) noexcept
{
  const isize new_end_offset = n_to_take;

  auto next_chunk = this->chunks_.begin();
  auto last_chunk = std::prev(this->chunks_.end());
  for (; next_chunk != last_chunk && n_to_take > 0; ++next_chunk) {
    if (n_to_take >= next_chunk->size()) {
      n_to_take -= next_chunk->size();
    } else {
      next_chunk->items.drop_back(next_chunk->items.size() - n_to_take);
      n_to_take = 0;
    }
  }
  this->chunks_.erase(next_chunk, last_chunk);
  this->chunks_.back().offset = new_end_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::drop_before_n(usize n_to_drop) noexcept
{
  auto first_chunk = this->chunks_.begin();
  auto next_chunk = first_chunk;
  auto last_chunk = std::prev(this->chunks_.end());
  for (; next_chunk != last_chunk && n_to_drop > 0; ++next_chunk) {
    if (n_to_drop >= next_chunk->size()) {
      n_to_drop -= next_chunk->size();
    } else {
      next_chunk->items.drop_front(n_to_drop);
      n_to_drop = 0;
      break;
    }
  }

  this->chunks_.erase(first_chunk, next_chunk);

  isize offset = 0;
  for (auto& chunk : this->chunks_) {
    chunk.offset = offset;
    offset += chunk.size();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::check_invariants() const noexcept
{
  isize expect_offset = 0;
  bool seen_last = false;
  for (const auto& chunk : this->chunks_) {
    BATT_CHECK(!seen_last);
    BATT_CHECK_EQ(expect_offset, chunk.offset);
    expect_offset += chunk.items.size();
    if (chunk.items.size() == 0) {
      seen_last = true;
    }
  }
  BATT_CHECK(seen_last);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
BoxedSeq<EditSlice> MergeCompactor::ResultSet<kDecayToItems>::live_edit_slices(
    const KeyView& lower_bound) const noexcept
{
  const Chunk<const EditView*>* chunks_begin =  //
      (const Chunk<const EditView*>*)this->chunks_.data();

  const Chunk<const EditView*>* chunks_end =      //
      chunks_begin + (this->chunks_.size() - 1);  // minus one to exclude the "null" end chunk.

  if (!lower_bound.empty()) {
    chunks_begin = std::lower_bound(chunks_begin, chunks_end, lower_bound, ExtendedKeyRangeOrder{});
  }

  if (chunks_begin == chunks_end) {
    return seq::Empty<EditSlice>{} | seq::boxed();
  }

  auto rest_of_chunks = as_seq(as_slice(std::next(chunks_begin), chunks_end))  //
                        | seq::map([](const Chunk<const EditView*>& chunk) -> EditSlice {
                            return EditSlice{chunk.items};
                          });

  Chunk<const EditView*> first_chunk = *chunks_begin;

  const EditView* first_chunk_end = first_chunk.items.end();

  const EditView* first_edit =
      std::lower_bound(first_chunk.items.begin(), first_chunk_end, lower_bound, KeyOrder{});

  if (first_edit == first_chunk.items.end()) {
    return std::move(rest_of_chunks) | seq::boxed();
  }

  return seq::single_item(EditSlice{as_slice(first_edit, first_chunk_end)})  //
         | seq::chain(std::move(rest_of_chunks))                             //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
KeyView MergeCompactor::ResultSet<kDecayToItems>::get_min_key() const noexcept
{
  BATT_CHECK(!this->empty());
  BATT_CHECK_GT(this->chunks_.size(), 0u);
  for (usize i = 0;; ++i) {
    if (!this->chunks_[i].items.empty()) {
      return get_key(this->chunks_[i].items.front());
    }
    BATT_CHECK_NE(i, this->chunks_.size() - 1) << BATT_INSPECT(this->empty());
  }
  BATT_PANIC();
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
KeyView MergeCompactor::ResultSet<kDecayToItems>::get_max_key() const noexcept
{
  BATT_CHECK_GT(this->chunks_.size(), 0u);
  for (usize i = this->chunks_.size() - 1;; --i) {
    if (!this->chunks_[i].items.empty()) {
      return get_key(this->chunks_[i].items.back());
    }
    BATT_CHECK_NE(i, 0) << BATT_INSPECT(this->empty());
  }
  BATT_PANIC();
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
bool MergeCompactor::ResultSet<kDecayToItems>::empty() const noexcept
{
  return this->chunks_.size() < 2;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
void MergeCompactor::ResultSet<kDecayToItems>::invalidate_packed_size() noexcept
{
  this->packed_size().store(~u64{0});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <bool kDecayToItems>
u64 MergeCompactor::ResultSet<kDecayToItems>::get_packed_size() const noexcept
{
  u64 observed = this->packed_size().load();
  if (observed != ~u64{0}) {
    return observed;
  }

  u64 calculated = 0;

  // TODO [tastolfi 2025-03-17] profile/instrument to see if this is worth parallel-optimizing.
  //
  calculated = this->live_edit_slices(KeyView{})  //
               | batt::seq::map([](const EditSlice& edit_slice) -> u64 {
                   return batt::case_of(edit_slice, [](const auto& slice_impl) -> u64 {
                     u64 total = 0;
                     for (const auto& edit : slice_impl) {
                       total += PackedSizeOfEdit{}(edit);
                     }
                     return total;
                   });
                 })  //
               | batt::seq::sum();

  this->packed_size().compare_exchange_strong(observed, calculated);

  return calculated;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Explicit template instantiations for (decayed) Items and Edits.
//
template class MergeCompactor::ResultSet</*kDecayToItems=*/false>;
template class MergeCompactor::ResultSet</*kDecayToItems=*/true>;

}  // namespace turtle_kv
