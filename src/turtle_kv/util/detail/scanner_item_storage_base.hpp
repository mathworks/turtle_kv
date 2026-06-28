#pragma once
#define TURTLE_KV_UTIL_DETAIL_SCANNER_ITEM_STORAGE_BASE_HPP

#include "scanner_value_storage_base.hpp"

#include <turtle_kv/util/art_base.hpp>

namespace turtle_kv {
namespace detail {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Base class for scanner; contains storage for the item at the current scanner position.
 */
template <typename ValueT, ARTBase::Synchronized kSynchronized, bool kValuesOnly>
class ScannerItemStorageBase;

/** \brief General case (kValuesOnly == false) for scanner item storage.
 */
template <typename ValueT, ARTBase::Synchronized kSynchronized>
class ScannerItemStorageBase<ValueT, kSynchronized, /*kValuesOnly=*/false>
    : public ScannerValueStorageBase<ValueT, kSynchronized>
{
 public:
  std::array<char, ARTBase::kMaxKeyLen> key_buffer_;
  usize key_len_ = 0;

  //----- --- -- -  -  -   -

  void append_key(usize prefix_len, const char* suffix_data, usize suffix_len) BATT_ALWAYS_INLINE
  {
    __builtin_memcpy(this->key_buffer_.data() + prefix_len, suffix_data, suffix_len);
  }

  void append_key_byte(usize prefix_len, const ByteInt& suffix_byte) BATT_ALWAYS_INLINE
  {
    this->key_buffer_[prefix_len] = suffix_byte.to_char();
  }

  void set_key_len(usize len) BATT_ALWAYS_INLINE
  {
    this->key_len_ = len;
  }

  std::string_view get_key() const
  {
    return std::string_view{this->key_buffer_.data(), this->key_len_};
  }
};

/** \brief kValuesOnly == true case; no key-related data members.
 */
template <typename ValueT, ARTBase::Synchronized kSynchronized>
class ScannerItemStorageBase<ValueT, kSynchronized, /*kValuesOnly=*/true>
    : public ScannerValueStorageBase<ValueT, kSynchronized>
{
 public:
  void append_key(usize, const char*, usize) BATT_ALWAYS_INLINE
  {
    // nothing to do.
  }

  void append_key_byte(usize, const ByteInt&) BATT_ALWAYS_INLINE
  {
    // nothing to do.
  }

  void set_key_len(usize) BATT_ALWAYS_INLINE
  {
    // nothing to do.
  }
};

}  // namespace detail
}  // namespace turtle_kv
