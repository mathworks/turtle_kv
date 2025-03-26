#pragma once

#include <turtle_kv/import/int_types.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <iterator>
#include <ostream>
#include <type_traits>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename Iter>
class Chunk
{
 public:
  using iterator = Iter;

  using value_type = std::remove_reference_t<decltype(*std::declval<Iter>())>;
  using reference = value_type&;

  isize offset;
  boost::iterator_range<Iter> items;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const noexcept
  {
    return this->items.empty();
  }

  usize size() const noexcept
  {
    return this->items.size();
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Iter>
Chunk<Iter> make_end_chunk(isize offset = 0, const Iter& inner_end = Iter{})
{
  return Chunk<Iter>{offset, boost::iterator_range<Iter>{inner_end, inner_end}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Iter>
inline std::ostream& operator<<(std::ostream& out, const Chunk<Iter>& t)
{
  return out << "Chunk{.offset=" << t.offset << ", .items[" << t.items.size()
             << "]={.begin=" << batt::make_printable(t.items.begin())
             << ", .end=" << batt::make_printable(t.items.end()) << ",},}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename OuterIter, typename InnerIter>
class Flatten
    : public boost::iterator_facade<              //
          Flatten<OuterIter, InnerIter>,          // <- Derived
          typename Chunk<InnerIter>::value_type,  // <- Value
          std::random_access_iterator_tag         // <- CategoryOrTraversal
          >
{
 public:
  using iterator_category = std::random_access_iterator_tag;

  using value_type = typename Chunk<InnerIter>::value_type;
  using reference = typename Chunk<InnerIter>::reference;

  Flatten() = default;

  explicit Flatten(const OuterIter& iter) noexcept : chunk_iter_{iter}, cached_chunk_{*iter}
  {
  }

  reference dereference() const
  {
    return this->cached_chunk_.items.front();
  }

  bool equal(const Flatten& that) const
  {
    return this->chunk_iter_ == that.chunk_iter_ &&
           this->cached_chunk_.offset == that.cached_chunk_.offset;
  }

  void increment()
  {
    this->cached_chunk_.items.advance_begin(1);
    if (this->cached_chunk_.items.empty()) {
      this->to_next_chunk_first();
    } else {
      this->cached_chunk_.offset += 1;
    }
  }

  void decrement()
  {
    BATT_ASSERT_GT(this->cached_chunk_.offset, 0);

    if (this->cached_chunk_.offset == this->chunk_iter_->offset) {
      this->to_prev_chunk_last();
    }
    this->cached_chunk_.offset -= 1;
    this->cached_chunk_.items.advance_begin(-1);
  }

  void advance(isize delta)
  {
    while (delta < 0) {
      if (this->cached_chunk_.offset == this->chunk_iter_->offset) {
        this->to_prev_chunk_last();
      }
      const isize step =
          std::min(static_cast<isize>(this->cached_chunk_.offset - this->chunk_iter_->offset),
                   -delta);
      delta += step;
      this->cached_chunk_.items.advance_begin(-step);
      this->cached_chunk_.offset -= step;
    }
    while (delta > 0) {
      const isize step = std::min(static_cast<isize>(this->cached_chunk_.items.size()), delta);
      delta -= step;
      this->cached_chunk_.items.advance_begin(step);
      if (this->cached_chunk_.items.empty()) {
        this->to_next_chunk_first();
      } else {
        this->cached_chunk_.offset += step;
      }
    }
  }

  isize distance_to(const Flatten& that) const
  {
    return that.cached_chunk_.offset - this->cached_chunk_.offset;
  }

  void to_next_chunk_first()
  {
    ++this->chunk_iter_;
    this->cached_chunk_ = *this->chunk_iter_;
  }

 private:
  void to_prev_chunk_last()
  {
    --this->chunk_iter_;
    this->cached_chunk_ = *this->chunk_iter_;
    const isize step = this->cached_chunk_.items.size();
    this->cached_chunk_.offset += step;
    this->cached_chunk_.items.advance_begin(step);
  }

 public:
  OuterIter chunk_iter_;
  Chunk<InnerIter> cached_chunk_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OuterIter, typename InnerIter>
inline std::ostream& operator<<(std::ostream& out, const Flatten<OuterIter, InnerIter>& t)
{
  return out << "Flatten{.iter=" << batt::make_printable(t.chunk_iter_) << " ("
             << batt::make_printable(*t.chunk_iter_) << ")"
             << ", .chunk=" << t.cached_chunk_ << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename OuterIter,
          typename InnerIter = typename std::iterator_traits<OuterIter>::value_type::iterator>
boost::iterator_range<Flatten<OuterIter, InnerIter>> flatten(const OuterIter& begin,
                                                             const OuterIter& end)
{
  using Iter = Flatten<OuterIter, InnerIter>;
  return boost::make_iterator_range(Iter{begin}, Iter{end});
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

}  // namespace turtle_kv

namespace std {

template <typename OuterIter, typename InnerIter, typename EachFn>
void for_each(::turtle_kv::Flatten<OuterIter, InnerIter> first,
              const ::turtle_kv::Flatten<OuterIter, InnerIter>& last,
              EachFn&& each_fn)
{
  for (;;) {
    if (first == last) {
      return;
    }
    if (last.cached_chunk_.offset <
        first.cached_chunk_.offset +
            static_cast<::turtle_kv::isize>(first.cached_chunk_.items.size())) {
      for (::turtle_kv::isize offset = first.cached_chunk_.offset;
           offset < last.cached_chunk_.offset;
           ++offset) {
        each_fn(*first);
        first.cached_chunk_.items.pop_front();
      }
      return;
    } else {
      for (auto& item : first.cached_chunk_.items) {
        each_fn(item);
      }
    }
    first.to_next_chunk_first();
  }
}

template <typename OuterIter, typename InnerIter, typename DstIter>
DstIter copy(::turtle_kv::Flatten<OuterIter, InnerIter> first,
             const ::turtle_kv::Flatten<OuterIter, InnerIter>& last,
             DstIter dst)
{
  for (;;) {
    if (first == last) {
      break;
    }
    if (last.cached_chunk_.offset <
        first.cached_chunk_.offset +
            static_cast<::turtle_kv::isize>(first.cached_chunk_.items.size())) {
      for (::turtle_kv::isize offset = first.cached_chunk_.offset;
           offset < last.cached_chunk_.offset;
           ++offset) {
        *dst = *first;
        ++dst;
        first.cached_chunk_.items.pop_front();
      }
      break;
    } else {
      dst = std::copy(first.cached_chunk_.items.begin(), first.cached_chunk_.items.end(), dst);
    }
    first.to_next_chunk_first();
  }
  return dst;
}

}  // namespace std
