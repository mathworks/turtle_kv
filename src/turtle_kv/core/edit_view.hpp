#pragma once

#include <turtle_kv/core/item_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_key_value.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/logging.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/assert.hpp>
#include <batteries/case_of.hpp>
#include <batteries/ref.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/type_traits.hpp>

#include <ostream>
#include <variant>

namespace turtle_kv {

struct EditView;

std::ostream& operator<<(std::ostream& out, const EditView& t);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct EditView {
  static std::atomic<bool>& terse_printing_enabled()
  {
    static std::atomic<bool> on{false};
    return on;
  }

  EditView() = default;

  explicit EditView(const KeyView& key, const ValueView& value) noexcept : key{key}, value{value}
  {
  }

  template <typename T, typename = batt::EnableIfNoShadow<EditView, T>>
  explicit EditView(T&& kv) noexcept : key{get_key(kv)}
                                     , value{get_value(kv)}
  {
  }

  template <typename T, typename = batt::EnableIfNoShadow<EditView, T>>
  EditView& operator=(T&& kv) noexcept
  {
    this->key = get_key(kv);
    this->value = get_value(kv);

    return *this;
  }

  friend inline bool operator==(const EditView& l, const EditView& r)
  {
    return l.key == r.key && l.value == r.value;
  }

  friend std::ostream& operator<<(std::ostream& out, const EditView& t);

  static const EditView& from_item_view(const ItemView& item)
  {
    return reinterpret_cast<const EditView&>(item);
  }

  struct From {
    EditView operator()(const ItemView& item) const
    {
      return EditView::from_item_view(item);
    }
    EditView operator()(const EditView& edit) const
    {
      return edit;
    }
    template <typename Var, typename = std::enable_if_t<batt::IsVariant<std::decay_t<Var>>{}>>
    EditView operator()(Var&& var) const
    {
      return std::visit(*this, BATT_FORWARD(var));
    }
    template <typename T, typename = decltype(to_edit_view(std::declval<T>())), typename = void>
    EditView operator()(T&& val) const
    {
      return to_edit_view(BATT_FORWARD(val));
    }
  };

  bool needs_combine() const
  {
    return value.needs_combine();
  }

  KeyView key;
  ValueView value;
};

// Assert the properties that make it safe to convert between EditView and ItemView by casting.
//
static_assert(sizeof(ItemView) == sizeof(EditView), "");
static_assert(sizeof(ItemView) == sizeof(ItemView::key) + sizeof(ItemView::value), "");
static_assert(std::is_same_v<decltype(ItemView::key), decltype(EditView::key)>, "");
static_assert(std::is_same_v<decltype(ItemView::value), decltype(EditView::value)>, "");
static_assert(offsetof(ItemView, key) == offsetof(EditView, key), "");
static_assert(offsetof(ItemView, value) == offsetof(EditView, value), "");

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct NeedsCombine {
  bool operator()(const EditView& edit) const
  {
    return edit.needs_combine();
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline EditView combine(const EditView& newer, const EditView& older)
{
  BATT_ASSERT_EQ(older.key, newer.key) << "combine requires the keys to be the same";

  return EditView{newer.key, combine(newer.value, older.value)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline EditView combine(const EditView& newer, const ItemView& older)
{
  return combine(newer, EditView::from_item_view(older));
}

struct Combine {
  template <typename T, typename U>
  EditView operator()(T&& t, U&& u) const
  {
    return combine(EditView::From{}(BATT_FORWARD(t)), EditView::From{}(BATT_FORWARD(u)));
  }
};

template <typename Seq>
inline Optional<EditView> combine_edits(Seq&& edits)
{
  Optional<EditView> combined_item = edits.next().map(EditView::From{});
  if (combined_item) {
    auto loop_body = [&](auto&& next_item) -> seq::LoopControl {
      combined_item->value = combine(combined_item->value, EditView::From{}(next_item).value);
      if (!combined_item->needs_combine()) {
        return seq::kBreak;
      }
      return seq::kContinue;
    };

    BATT_FORWARD(edits) | seq::for_each(loop_body);
  }
  return combined_item;
}

struct CombineEdits {
  template <typename Seq>
  decltype(auto) operator()(Seq&& edits) const
  {
    static_assert(std::is_same_v<std::decay_t<Seq>, Seq>, "must not be called with a reference");

    return combine_edits(BATT_FORWARD(edits));
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline Optional<ItemView> to_item_view(const EditView& edit)
{
  switch (edit.value.op()) {
    case ValueView::OP_DELETE:
      return None;

    case ValueView::OP_NOOP:
      return None;

    case ValueView::OP_WRITE:
      return ItemView{edit.key, edit.value};

    case ValueView::OP_ADD_I32:
      return ItemView{edit.key, edit.value};

    case ValueView::OP_PAGE_SLICE:
      return ItemView{edit.key, edit.value};

    case ValueView::BEGIN_OP_UNDEFINED:
      return None;

    case ValueView::END_OP_UNDEFINED:
      return None;
  }

  return None;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline Optional<ItemView> to_item_view(const Optional<EditView>& edit)
{
  if (edit) {
    return to_item_view(*edit);
  }
  return None;
}

inline bool decays_to_item(const EditView& edit)
{
  return decays_to_item(edit.value);
}

inline bool decays_to_item(const ItemView&)
{
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_ALWAYS_INLINE inline const KeyView& get_key(const EditView& edit)
{
  return edit.key;
}

inline const ValueView& get_value(const EditView& edit)
{
  return edit.value;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline Optional<ItemView> to_item_view(const PackedKeyValue& kv)
{
  return to_item_view(EditView{get_key(kv), get_value(kv)});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline EditView to_edit_view(const PackedKeyValue& kv)
{
  return EditView{get_key(kv), get_value(kv)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const EditView& to_edit_view(const EditView& edit)
{
  return edit;
}

}  // namespace turtle_kv
