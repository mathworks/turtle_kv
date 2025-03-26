#pragma once

#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/core/table.hpp>

namespace turtle_kv {

class SubtreeTable : public Table
{
 public:
  explicit SubtreeTable(llfs::PageLoader& page_loader, Subtree& subtree) noexcept
      : page_loader_{page_loader}
      , subtree_{subtree}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status put(const KeyView& key, const ValueView& value) noexcept override
  {
    BATT_PANIC() << "This will never be implemented.";
    return batt::StatusCode::kUnimplemented;
  }

  StatusOr<ValueView> get(const KeyView& key) noexcept override
  {
    return this->subtree_.find_key(this->page_loader_, this->latest_pinned_page_, key);
  }

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept override
  {
    BATT_PANIC() << "This will never be implemented.";
    return {batt::StatusCode::kUnimplemented};
  }

  Status remove(const KeyView& key) noexcept override
  {
    BATT_PANIC() << "This will never be implemented.";
    return batt::StatusCode::kUnimplemented;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& page_loader_;
  llfs::PinnedPage latest_pinned_page_;
  Subtree& subtree_;
};

}  // namespace turtle_kv
