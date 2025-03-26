#pragma once

#include <turtle_kv/core/packed_page_slice.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/logging.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <ostream>
#include <string_view>

namespace turtle_kv {

class ValueView
{
 public:
  struct PtrData {
  };
  struct InlineData {
  };
  struct I32Data {
  };

  enum OpCode : u16 {
    OP_DELETE = 0,
    OP_NOOP = 1,
    OP_WRITE = 2,
    OP_ADD_I32 = 3,
    OP_PAGE_SLICE = 4,
    BEGIN_OP_UNDEFINED,
    END_OP_UNDEFINED = 0x7fff,
  };

  friend inline std::ostream& operator<<(std::ostream& out, OpCode code) noexcept
  {
    switch (code) {
      case OP_DELETE:
        return out << "delete";

      case OP_NOOP:
        return out << "noop";

      case OP_WRITE:
        return out << "write";

      case OP_ADD_I32:
        return out << "add_i32";

      case OP_PAGE_SLICE:
        return out << "page_slice";

      case BEGIN_OP_UNDEFINED:  // fall-through
      case END_OP_UNDEFINED:    // fall-through
        break;
    }
    return out << "???";
  }

  using OpCodePair = u32;

  static constexpr OpCodePair op_pair(OpCode first, OpCode second) noexcept
  {
    return (u32{first} << 16) | u32{second};
  }

  static constexpr usize kMaxSmallStrSize = 8;
  static constexpr i32 kOpShift = (64 - 16);
  static constexpr u64 kMaxSize = (u64{1} << kOpShift) - 1;
  static constexpr u64 kSizeMask = kMaxSize;
  static constexpr i32 kInlineShift = 63;
  static constexpr u64 kInlineMask = u64{1} << kInlineShift;
  static constexpr u64 kOpMask = ~(kSizeMask | kInlineMask);

  static u64 tag_from_op_and_size(bool is_inline, u64 op, u64 size) noexcept
  {
    // TODO [tastolfi 2022-06-14] Add a one-time post read validation function to do all these
    // checks, in case of corrupted/malicious data.
    //
    BATT_ASSERT_LE(size, kMaxSize);
    BATT_ASSERT_LT(op, BEGIN_OP_UNDEFINED);
    BATT_ASSERT((int)is_inline == 0 || (int)is_inline == 1);

    return (u64{is_inline} << kInlineShift) | ((op << kOpShift) & kOpMask) | (size & kSizeMask);
  }

  static constexpr OpCode op_from_tag(u64 tag) noexcept
  {
    return static_cast<OpCode>((tag & kOpMask) >> kOpShift);
  }

  static constexpr u64 size_from_tag(u64 tag) noexcept
  {
    return tag & kSizeMask;
  }

  static ValueView deleted() noexcept
  {
    static const char* empty_str_ = "";
    return ValueView{OP_DELETE, PtrData{}, empty_str_, 0};
  }

  static ValueView from_packed(OpCode op, const std::string_view& str) noexcept
  {
    if (str.size() <= kMaxSmallStrSize) {
      return ValueView{op, InlineData{}, str.data(), str.size()};
    }
    return ValueView{op, PtrData{}, str.data(), str.size()};
  }

  static ValueView from_page_slice(const PackedPageSlice& page_slice)
  {
    return ValueView{OP_PAGE_SLICE,
                     PtrData{},
                     reinterpret_cast<const char*>(&page_slice),
                     packed_sizeof(page_slice)};
  }

  static ValueView from_str(const std::string_view& str)
  {
    return ValueView::from_packed(OP_WRITE, str);
  }

  static ValueView from_buffer(const ConstBuffer& buffer)
  {
    return ValueView::from_str(
        std::string_view{static_cast<const char*>(buffer.data()), buffer.size()});
  }

  template <typename T>
  static ValueView from_struct(const T& src)
  {
    return ValueView::from_buffer(batt::buffer_from_struct(src));
  }

  static ValueView empty_value()
  {
    return ValueView::from_str(std::string_view{});
  }

  template <typename I>
  static ValueView write_i32(I i)
  {
    static_assert(std::is_same_v<I, i32>, "type of `i` must be i32");
    return ValueView{OP_WRITE, I32Data{}, i};
  }

  template <typename I>
  static ValueView add_i32(I i)
  {
    static_assert(std::is_same_v<I, i32>, "type of `i` must be i32");
    return ValueView{OP_ADD_I32, I32Data{}, i};
  }

 private:
  explicit ValueView(OpCode op, PtrData, const char* ptr, usize size) noexcept
      : size_tag_{tag_from_op_and_size(false, op, size)}
  {
    this->data_.ptr_ = ptr;
  }

  explicit ValueView(OpCode op, InlineData, const char* ptr, usize size) noexcept
      : size_tag_{tag_from_op_and_size(true, op, size)}
  {
    std::memcpy(this->data_.chars_, ptr, size);
  }

  explicit ValueView(OpCode op, I32Data, i32 i) noexcept
      : size_tag_{tag_from_op_and_size(true, op, sizeof(i32))}
  {
    this->data_.i32_ = i;
  }

 public:
  ValueView() noexcept : size_tag_{tag_from_op_and_size(true, OP_NOOP, 0)}
  {
  }

  bool is_self_contained() const
  {
    return this->size_tag_ & kInlineMask;
  }

  HasPageRefs has_page_refs() const
  {
    if (this->op() == OP_PAGE_SLICE) {
      return HasPageRefs{true};
    }
    return HasPageRefs{false};
  }

  batt::BoxedSeq<llfs::PageId> trace_refs() const
  {
    if (this->op() == OP_PAGE_SLICE) {
      StatusOr<const PackedPageSlice&> page_slice = this->as_page_slice();
      if (page_slice.ok()) {
        return {batt::seq::single_item(page_slice->page_id)       //
                | batt::seq::map(BATT_OVERLOADS_OF(get_page_id))  //
                | batt::seq::boxed()};
      }
      LOG(ERROR) << "Invalid PackedPageSlice detected! (returning empty page ref set)"
                 << BATT_INSPECT(page_slice.status());
      //
      // fall-through
    }

    return {batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed()};
  }

  bool empty() const
  {
    return this->size() == 0;
  }

  const char* data() const
  {
    if (this->is_self_contained()) {
      return this->data_.chars_;
    }
    return this->data_.ptr_;
  }

  std::string_view as_str() const
  {
    return std::string_view{this->data(), this->size()};
  }

  batt::ConstBuffer as_buffer() const
  {
    return batt::ConstBuffer{this->data(), this->size()};
  }

  StatusOr<const PackedPageSlice&> as_page_slice() const
  {
    return llfs::unpack_cast<PackedPageSlice>(this->as_buffer());
  }

  StatusOr<llfs::PageId> as_page_id() const
  {
    StatusOr<const PackedPageSlice&> packed = this->as_page_slice();
    BATT_REQUIRE_OK(packed);

    return packed->page_id.unpack();
  }

  i32 as_i32() const
  {
    switch (this->size()) {
      case 0:
        return 0;
      case 1:
        return *reinterpret_cast<const little_i8*>(this->data());
      case 2:
        return *reinterpret_cast<const little_i16*>(this->data());
      case 3:
        return *reinterpret_cast<const little_i24*>(this->data());
      default:
        return *reinterpret_cast<const little_i32*>(this->data());
    }
  }

  usize size() const
  {
    return size_from_tag(this->size_tag_);
  }

  OpCode op() const
  {
    return static_cast<OpCode>(op_from_tag(this->size_tag_));
  }

  bool is_delete() const noexcept
  {
    return this->op() == OP_DELETE;
  }

  bool needs_combine() const
  {
    if (this->op() == OP_ADD_I32) {
      return true;
    }
    return false;
  }

  friend inline std::ostream& operator<<(std::ostream& out, const ValueView& t)
  {
    out << "Value{.op=" << t.op();
    if (t.op() != OP_DELETE) {
      out << ", .str=" << batt::c_str_literal(t.as_str()) << ", .i32=" << t.as_i32();
    }
    return out << ",}";
  }

  friend inline bool operator==(const ValueView& l, const ValueView& r)
  {
    return l.as_str() == r.as_str() && l.op() == r.op();
  }

 private:
  usize size_tag_;
  union data_type {
    const char* ptr_;
    char chars_[sizeof(const char*)];
    little_i32 i32_;
  } data_;
};

namespace {
BATT_STATIC_ASSERT_EQ(sizeof(ValueView), sizeof(std::string_view));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline ValueView combine(const ValueView& newer, const ValueView& older) noexcept
{
  if (!newer.needs_combine()) {
    return newer;
  }
  switch (ValueView::op_pair(newer.op(), older.op())) {
    case ValueView::op_pair(ValueView::OP_ADD_I32, ValueView::OP_WRITE):
      return ValueView::write_i32(newer.as_i32() + older.as_i32());

    case ValueView::op_pair(ValueView::OP_ADD_I32, ValueView::OP_DELETE):
      return ValueView::write_i32(newer.as_i32());

    case ValueView::op_pair(ValueView::OP_ADD_I32, ValueView::OP_ADD_I32):
      return ValueView::add_i32(newer.as_i32() + older.as_i32());

    default:
      LOG(WARNING) << "Bad combination of opcodes: newer=" << newer << " older=" << older;
      return newer;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline [[nodiscard]] bool combine_in_place(Optional<ValueView>* newer,
                                           const ValueView& older) noexcept
{
  if (!*newer) {
    newer->emplace(older);
  } else {
    **newer = combine(**newer, older);
  }

  return !(**newer).needs_combine();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline StatusOr<bool> combine_in_place(Optional<ValueView>* newer,
                                       const StatusOr<ValueView>& older) noexcept
{
  if (older.ok()) {
    return combine_in_place(newer, *older);
  }
  if (older.status() == batt::StatusCode::kNotFound) {
    return {false};
  }
  return older.status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const ValueView& get_value(const ValueView& value)
{
  return value;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool decays_to_item(const ValueView& value)
{
  switch (value.op()) {
    case ValueView::OP_DELETE:
      return false;

    case ValueView::OP_NOOP:
      return false;

    case ValueView::OP_WRITE:
      return true;

    case ValueView::OP_ADD_I32:
      return true;

    case ValueView::OP_PAGE_SLICE:
      return true;

    case ValueView::BEGIN_OP_UNDEFINED:
      return false;

    case ValueView::END_OP_UNDEFINED:
      return false;
  }

  return false;
}

}  // namespace turtle_kv
