#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <map>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class Table
{
 public:
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  virtual ~Table() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual Status put(const KeyView& key, const ValueView& value) = 0;

  virtual StatusOr<ValueView> get(const KeyView& key) = 0;

  virtual StatusOr<usize> scan(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) = 0;

  virtual Status remove(const KeyView& key) = 0;

 protected:
  Table() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename MapT>
class OrderedMapTable : public Table
{
 public:
  using Key = typename MapT::key_type;

  Status put(const KeyView& key, const ValueView& value) override
  {
    auto [iter, inserted] = this->state_.emplace(key, value.as_str());
    if (!inserted) {
      iter->second = value.as_str();
    }

    return OkStatus();
  }

  StatusOr<ValueView> get(const KeyView& key) override
  {
    auto iter = this->state_.find(Key{key});
    if (iter == this->state_.end()) {
      return {batt::StatusCode::kNotFound};
    }

    return {ValueView::from_str(iter->second)};
  }

  StatusOr<usize> scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) override
  {
    usize n = 0;

    auto iter = this->state_.lower_bound(Key{min_key});
    while (iter != this->state_.end()) {
      if (n == items_out.size()) {
        break;
      }

      items_out[n].first = iter->first;
      items_out[n].second = ValueView::from_str(iter->second);

      ++n;
      ++iter;
    }

    return n;
  }

  Status remove(const KeyView& key) override
  {
    this->state_.erase(Key{key});
    return OkStatus();
  }

  MapT state_;
};

using StdMapTable = OrderedMapTable<std::map<std::string, std::string>>;

}  // namespace turtle_kv
