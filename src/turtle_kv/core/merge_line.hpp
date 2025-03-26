#pragma once

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/ref.hpp>
#include <turtle_kv/import/seq.hpp>

#include <batteries/suppress.hpp>

BATT_SUPPRESS_IF_GCC("-Wunused-parameter")

#if defined(__GNUC__) && __GNUC__ >= 9
BATT_SUPPRESS_IF_GCC("-Wdeprecated-copy")
#endif  // defined(__GNUC__) && __GNUC__ >= 9

#include <boost/heap/d_ary_heap.hpp>
#include <boost/heap/policies.hpp>

namespace turtle_kv {

// Forward-declarations.
//
class MergeCompactor;
class MergeFrame;

/** \brief A sequence of non-intersecting EditSlice objects at a constant merge-depth.
 */
class MergeLine
{
 public:
  friend class MergeCompactor;
  friend class MergeFrame;

  using FrontKeyHeap = boost::heap::d_ary_heap<Ref<MergeLine>,                         //
                                               boost::heap::arity<2>,                  //
                                               boost::heap::compare<MinKeyHeapOrder>,  //
                                               boost::heap::mutable_<false>            //
                                               >;

  using BackKeyHeap = boost::heap::d_ary_heap<Ref<MergeLine>,                         //
                                              boost::heap::arity<2>,                  //
                                              boost::heap::compare<MaxKeyHeapOrder>,  //
                                              boost::heap::mutable_<true>             //
                                              >;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  MergeLine(MergeFrame* frame, BoxedSeq<EditSlice>&& edit_slices) noexcept;

  /** \brief Returns true iff this line has unread data.
   */
  bool empty();

  /** \brief Pulls a slice from `rest` to `first`, if possible.
   */
  void advance();

  /** \brief Returns the stack-depth of this line.
   */
  u32 depth() const
  {
    return this->depth_;
  }

  /** \brief Returns the portion of `first` up to and including `last_key`.
   *
   * If last_key is the final item in `first`, then the next slice is pulled from `rest`; otherwise
   * first becomes everything *after* last_key.
   */
  EditSlice cut(const KeyView& last_key);

  /** \brief If the line is non-empty, returns true iff the given key is strictly less-than the
   * first unread key in this line.  If the line is empty, always returns true.
   */
  bool begins_after(const KeyView& key) const;

  /** \brief Returns the (0-based) index of this line within its containing MergeFrame.
   */
  usize get_index_in_frame() const;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  friend inline KeyView get_min_key(const MergeLine& line)
  {
    return get_min_key(line.first_);
  }

  friend inline KeyView get_max_key(const MergeLine& line)
  {
    return get_max_key(line.first_);
  }

  friend inline auto debug_print(const MergeLine& line)
  {
    return batt::case_of(line.first_,
                         [&line](const auto& slice) -> std::function<void(std::ostream&)> {
                           return [&slice, &line](std::ostream& out) {
                             out << "line[" << line.depth_ << "]: " << batt::dump_range(slice);
                           };
                         });
  }

 private:
  // (set by Scanner) Points to the Frame that owns this line.
  //
  MergeFrame* frame_;

  // (set by Scanner) The tree-depth of this line; lower depth is more recent.
  //
  u32 depth_;

  // The first slice of unread edits in this line.
  //
  EditSlice first_;

  // The remaining unread edits in this line, if any.
  //
  BoxedSeq<EditSlice> rest_;

  // (set by Scanner) The handle of this line in the back-key-priority heap.
  //
  typename BackKeyHeap::handle_type back_handle_;
};

}  // namespace turtle_kv
