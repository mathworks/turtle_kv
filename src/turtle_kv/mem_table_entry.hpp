#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <absl/base/config.h>
#include <absl/container/internal/hash_function_defaults.h>

#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <string_view>

namespace turtle_kv {

inline u64 get_key_hash_val(const std::string_view& key)
{
  return absl::container_internal::hash_default_hash<std::string_view>{}(key);
}

struct PackedValueUpdate {
  /** \brief Must be 0.
   */
  little_u16 key_len;

  /** \brief The least-significant 16 bits of the key version (within the MemTable/batch).
   */
  little_u16 revision;

  /** \brief The locator of the first slot for this key in this MemTable/batch.
   */
  little_u32 base_locator;

  /** \brief The locator of the slot for the previous version of this key.
   */
  little_u32 prev_locator;

  /** \brief The (reader) version for this update.
   */
  little_u32 version;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedValueUpdate), 16);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Used to insert or update a key/value entry in a hash set.
 */
template <typename StorageT>
struct MemTableEntryInserter {
  MemTableEntryInserter(const MemTableEntryInserter&) = delete;
  MemTableEntryInserter& operator=(const MemTableEntryInserter&) = delete;

  /** \brief Constructs a new inserter for the given key/value pair.
   */
  template <typename K, typename V>
  explicit MemTableEntryInserter(StorageT& storage_arg,
                                 K&& key_arg,
                                 V&& value_arg,
                                 u32 version_arg) noexcept
      : storage{storage_arg}
      , key{BATT_FORWARD(key_arg)}
      , value{BATT_FORWARD(value_arg)}
      , version{version_arg}
      , hash_val{get_key_hash_val(key)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs (set at construction-time)

  StorageT& storage;
  const std::string_view key;
  const ValueView value;
  const u32 version;
  const u64 hash_val;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs (set by store_insert or store_update).

  ValueView stored_value;
  u32 stored_locator;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view store_insert() noexcept
  {
    char* key_dst;
    const usize key_len = this->key.size();
    const usize value_len = this->value.size();
    const usize insert_size = sizeof(little_u16)  // header
                              + key_len           // key
                              + sizeof(big_u32)   // version-suffix
                              + value_len         // value
        ;

    this->storage.store_data(insert_size, [&](u32 locator, const MutableBuffer& buffer) {
      this->stored_locator = locator;

      auto* header_dst = place_first<little_u16>(buffer.data());
      *header_dst = key_len;

      key_dst = place_next<char>(header_dst, 1);
      std::memcpy(key_dst, this->key.data(), key_len);

      auto* version_dst = place_next<big_u32>(key_dst, key_len);
      *version_dst = this->version;

      auto* value_dst = place_next<char>(version_dst, 1);
      std::memcpy(value_dst, this->value.data(), value_len);
      this->stored_value = ValueView::from_str(std::string_view{value_dst, value_len});
    });

    return std::string_view{key_dst, key_len};
  }

  void store_update(u32 revision, u32 base_locator, u32 prev_locator) noexcept
  {
    const usize value_len = this->value.size();
    const usize update_size = sizeof(PackedValueUpdate)  // header
                              + value_len                // value
        ;

    this->storage.store_data(update_size, [&](u32 locator, const MutableBuffer& buffer) {
      this->stored_locator = locator;

      auto* header = place_first<PackedValueUpdate>(buffer.data());

      header->key_len = 0;
      header->revision = static_cast<u16>(revision);
      header->base_locator = base_locator;
      header->prev_locator = prev_locator;
      header->version = this->version;

      auto* value_dst = place_next<char>(header, 1);
      std::memcpy(value_dst, this->value.data(), value_len);
      this->stored_value = ValueView::from_str(std::string_view{value_dst, value_len});
    });
  }
};

template <typename StorageT>
inline const std::string_view& get_key(const MemTableEntryInserter<StorageT>& i) noexcept
{
  return i.key;
}
template <typename StorageT>
inline u64 get_key_hash_val(const MemTableEntryInserter<StorageT>& i) noexcept
{
  return i.hash_val;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Value type for hash set index in hybrid MemTable.
 */
class MemTableEntry
{
 public:
  MemTableEntry() = default;
  MemTableEntry(MemTableEntry&&) = default;
  MemTableEntry& operator=(MemTableEntry&&) = default;

  template <typename StorageT>
  explicit MemTableEntry(MemTableEntryInserter<StorageT>& i) noexcept
      : key_{i.store_insert()}
      , value_{i.stored_value}
      , locator_{i.stored_locator}
      , base_locator_{this->locator_}
      , revision_{0}
  {
  }

  template <typename StorageT>
  void update(MemTableEntryInserter<StorageT>& i) const noexcept
  {
    ++this->revision_;
    i.store_update(this->revision_, this->base_locator_, this->locator_);
    this->value_ = i.stored_value;
    this->locator_ = i.stored_locator;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view key_;
  mutable ValueView value_;
  mutable u32 locator_;
  u32 base_locator_;
  mutable u32 revision_;
};

inline const std::string_view& get_key(const MemTableEntry& e)
{
  return e.key_;
}

inline u64 get_key_hash_val(const MemTableEntry& e)
{
  return get_key_hash_val(get_key(e));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A Query object which precomputes the hash_val so we can shard hash table locks.
 */
struct MemTableQuery {
  std::string_view key;
  u64 hash_val = get_key_hash_val(this->key);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemTableQuery(const std::string_view& key_arg) noexcept : key{key_arg}
  {
  }
};

inline const std::string_view& get_key(const MemTableQuery& query)
{
  return query.key;
}

inline u64 get_key_hash_val(const MemTableQuery& query)
{
  return query.hash_val;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Default hash functor for set-based hash index in hybrid MemTable
 */
struct DefaultStrHash {
  using is_transparent = void;

  template <typename T>
  decltype(auto) operator()(T&& t) const
  {
    return get_key_hash_val(BATT_FORWARD(t));
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Default equality functor for set-based hash index in hybrid MemTable
 */
struct DefaultStrEq : absl::container_internal::hash_default_eq<std::string_view> {
  using Super = absl::container_internal::hash_default_eq<std::string_view>;

  template <typename L, typename R>
  decltype(auto) operator()(L&& l, R&& r) const
  {
    return Super::operator()(get_key(BATT_FORWARD(l)), get_key(BATT_FORWARD(r)));
  }
};

}  // namespace turtle_kv
