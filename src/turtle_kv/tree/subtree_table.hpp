#pragma once

#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/core/table.hpp>

namespace turtle_kv {

class SubtreeTable : public Table
{
 public:
  explicit SubtreeTable(llfs::PageLoader& page_loader,
                        const TreeOptions& tree_options,
                        Subtree& subtree) noexcept
      : page_loader_{page_loader}
      , tree_options_{tree_options}
      , subtree_{subtree}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(const KeyView& key, const ValueView& value) override
  {
    BATT_PANIC() << "This will never be implemented.";
    return batt::StatusCode::kUnimplemented;
  }

  StatusOr<ValueView> get(const KeyView& key) override
  {
    BATT_ASSIGN_OK_RESULT(i32 height, this->subtree_.get_height(this->page_loader_));

    this->page_slice_storage_.emplace();

    KeyQuery query{
        this->page_loader_,
        *this->page_slice_storage_,
        this->tree_options_,
        key,
    };

    return this->subtree_.find_key(ParentNodeHeight{height + 1}, query);
  }

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) override
  {
    BATT_PANIC() << "This will never be implemented.";
    return {batt::StatusCode::kUnimplemented};
  }

  Status remove(const KeyView& key) override
  {
    BATT_PANIC() << "This will never be implemented.";
    return batt::StatusCode::kUnimplemented;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& page_loader_;
  TreeOptions tree_options_;
  Optional<PageSliceStorage> page_slice_storage_;
  Subtree& subtree_;
};

}  // namespace turtle_kv
