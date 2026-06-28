#pragma once
#define TURTLE_KV_UTIL_DETAIL_SCANNER_VALUE_STORAGE_BASE_HPP

#include <turtle_kv/util/art_base.hpp>

#include <turtle_kv/import/optional.hpp>

namespace turtle_kv {
namespace detail {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT, ARTBase::Synchronized kSynchronized>
struct ScannerValueStorageBase {
  std::aligned_storage_t<sizeof(ValueT), alignof(ValueT)> value_storage_;

  template <typename NodeT>
  std::conditional_t<std::is_const_v<NodeT>, const void*, void*> value_storage_address(
      NodeT* node,
      const Optional<bool>& sync)
  {
    if (kSynchronized == ARTBase::Synchronized::kTrue || sync.value_or(true)) {
      return &this->value_storage_;
    }
    return node + 1;
  }
};

//----- --- -- -  -  -   -

template <typename ValueT>
struct ScannerValueStorageBase<ValueT, ARTBase::Synchronized::kFalse> {
  template <typename NodeT>
  const void* value_storage_address(const NodeT* node, const Optional<bool>&) const
  {
    return node + 1;
  }
};

//----- --- -- -  -  -   -

template <>
struct ScannerValueStorageBase<void, ARTBase::Synchronized::kFalse> {
  template <typename NodeT>
  void* value_storage_address(const NodeT*, const Optional<bool>&) const
  {
    return nullptr;
  }
};

//----- --- -- -  -  -   -

template <>
struct ScannerValueStorageBase<void, ARTBase::Synchronized::kTrue> {
  template <typename NodeT>
  void* value_storage_address(const NodeT*, const Optional<bool>&) const
  {
    return nullptr;
  }
};

//----- --- -- -  -  -   -

template <>
struct ScannerValueStorageBase<void, ARTBase::Synchronized::kDynamic> {
  template <typename NodeT>
  void* value_storage_address(const NodeT*, const Optional<bool>&) const
  {
    return nullptr;
  }
};

}  // namespace detail
}  // namespace turtle_kv
