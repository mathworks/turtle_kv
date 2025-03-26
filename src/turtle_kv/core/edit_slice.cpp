#include <turtle_kv/core/edit_slice.hpp>
//

#include <batteries/case_of.hpp>
#include <batteries/stream_util.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeyView get_min_key(const EditSlice& edit_slice)
{
  return batt::case_of(edit_slice, [](const auto& slice) -> KeyView {
    return get_key(slice.front());
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeyView get_max_key(const EditSlice& edit_slice)
{
  return batt::case_of(edit_slice, [](const auto& slice) -> KeyView {
    return get_key(slice.back());
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool is_empty(const EditSlice& edit_slice)
{
  return batt::case_of(edit_slice, [](const auto& slice) -> bool {
    return slice.empty();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize get_item_count(const EditSlice& edit_slice)
{
  return batt::case_of(edit_slice, [](const auto& slice) -> usize {
    return slice.size();
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool is_sorted_by_key(const EditSlice& edit_slice)
{
  return batt::case_of(edit_slice, [](const auto& edits) {
    return std::is_sorted(edits.begin(), edits.end(), KeyOrder{});
  });
}

}  // namespace turtle_kv

namespace batt {

SmallFn<void(std::ostream&)> dump_range(const ::turtle_kv::EditSlice& edit_slice, Pretty pretty)
{
  return [edit_slice, pretty](std::ostream& out) {
    case_of(edit_slice, [&](const auto& items) {
      out << dump_range(items, pretty);
    });
  };
}

}  // namespace batt
