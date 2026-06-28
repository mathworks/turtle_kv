#pragma once
#define TURTLE_KV_UTIL_ART_DEFAULT_INSERTERS_HPP

#include <turtle_kv/import/status.hpp>

#include <batteries/utility.hpp>

#include <utility>

namespace turtle_kv {

template <typename ValueT>
struct DefaultCopyInserter {
  const ValueT& copy_from_;

  explicit DefaultCopyInserter(const ValueT& copy_from) noexcept : copy_from_{copy_from}
  {
  }

  Status insert_new(void* copy_to)
  {
    new (copy_to) ValueT{this->copy_from_};
    return OkStatus();
  }

  Status update_existing(ValueT* copy_to)
  {
    *copy_to = this->copy_from_;
    return OkStatus();
  }
};

template <typename ValueT>
struct DefaultMoveInserter {
  ValueT&& move_from_;

  explicit DefaultMoveInserter(ValueT&& move_from) noexcept : move_from_{move_from}
  {
  }

  Status insert_new(void* move_to)
  {
    new (move_to) ValueT{std::move(this->move_from_)};
    return OkStatus();
  }

  Status update_existing(ValueT* move_to)
  {
    *move_to = std::move(this->move_from_);
    return OkStatus();
  }
};

struct DefaultVoidInserter {
  BATT_ALWAYS_INLINE Status insert_new(void*)
  {
    return OkStatus();
  }

  BATT_ALWAYS_INLINE Status update_existing(void*)
  {
    return OkStatus();
  }
};

}  // namespace turtle_kv
