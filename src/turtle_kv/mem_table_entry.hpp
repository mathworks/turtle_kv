#pragma once

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <absl/base/config.h>
#include <absl/container/internal/hash_function_defaults.h>

#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <string_view>

namespace turtle_kv {

inline u64 get_key_hash_val(const std::string_view& key)
{
  return absl::container_internal::hash_default_hash<std::string_view>{}(key) | 1;
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
  big_u32 version;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedValueUpdate), 16);

class MemTableEntry;
class MemTableValueEntry;

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
  explicit MemTableEntryInserter(std::atomic<i64>& current_mem_table_byte_size,
                                 i64 mem_table_size_limit,
                                 bool replace_old_value_size,
                                 StorageT& storage_arg,
                                 K&& key_arg,
                                 V&& value_arg,
                                 u32 version_arg) noexcept
      : current_mem_table_byte_size_{current_mem_table_byte_size}
      , mem_table_size_limit_{mem_table_size_limit}
      , replace_old_value_size_{replace_old_value_size}
      , storage{storage_arg}
      , key{BATT_FORWARD(key_arg)}
      , value{BATT_FORWARD(value_arg)}
      , version{version_arg}
      , hash_val{get_key_hash_val(key)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs (set at construction-time)

  std::atomic<i64>& current_mem_table_byte_size_;
  const i64 mem_table_size_limit_;
  const bool replace_old_value_size_;
  StorageT& storage;
  const std::string_view key;
  const ValueView value;
  const u32 version;
  const u64 hash_val;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs (set by store_insert or store_update).

  ValueView stored_value;
  u32 stored_locator;
  bool inserted = false;
  const MemTableEntry* entry = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status update_table_size(i64 table_size_delta)
  {
    const i64 old_table_size = this->current_mem_table_byte_size_.fetch_add(table_size_delta);
    const i64 new_table_size = old_table_size + table_size_delta;

    if (new_table_size > this->mem_table_size_limit_) {
      this->current_mem_table_byte_size_.fetch_sub(table_size_delta);
      return {batt::StatusCode::kResourceExhausted};
    }

    return OkStatus();
  }

  StatusOr<std::string_view> store_insert() noexcept
  {
    char* key_dst;
    const usize key_len = this->key.size();
    const usize value_len = this->value.size();
    const usize insert_size = sizeof(little_u16)  // header
                              + key_len           // key
                              + sizeof(big_u32)   // version-suffix
                              + value_len;        // value

    BATT_REQUIRE_OK(this->update_table_size(insert_size));

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

    this->inserted = true;

    return std::string_view{key_dst, key_len};
  }

  Status store_update(const ValueView& prev_value,
                      u32 revision,
                      u32 base_locator,
                      u32 prev_locator) noexcept
  {
    const usize value_len = this->value.size();
    const usize update_size = sizeof(PackedValueUpdate)  // header
                              + value_len;               // value

    const i64 table_size_delta = (this->replace_old_value_size_)
                                     ? ((i64)value_len - (i64)prev_value.size())
                                     : (i64)value_len;
    // ^^^^
    // TODO [tastolfi 2025-05-27] we need to make sure that we eventually do fill up a MemTable,
    // even if we just overwrite a single key over and over again.

    BATT_REQUIRE_OK(this->update_table_size(table_size_delta));

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

    return OkStatus();
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
  using Self = MemTableEntry;

  MemTableEntry() = default;

  MemTableEntry(MemTableEntry&&) = default;
  MemTableEntry& operator=(MemTableEntry&&) = default;

  MemTableEntry(const MemTableEntry&) = default;
  MemTableEntry& operator=(const MemTableEntry&) = default;

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
  Status assign(MemTableEntryInserter<StorageT>& i)
  {
    StatusOr<std::string_view> stored_key = i.store_insert();
    BATT_REQUIRE_OK(stored_key);

    this->key_ = *stored_key;
    this->value_ = i.stored_value;
    this->locator_ = i.stored_locator;
    this->base_locator_ = this->locator_;
    this->revision_ = 0;

    i.entry = this;

    return OkStatus();
  }

  template <typename StorageT>
  Status update(MemTableEntryInserter<StorageT>& i) const noexcept
  {
    BATT_REQUIRE_OK(
        i.store_update(this->value_, this->revision_ + 1, this->base_locator_, this->locator_));

    this->revision_ += 1;
    this->value_ = i.stored_value;
    this->locator_ = i.stored_locator;

    i.entry = this;

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view key_;
  mutable ValueView value_;
  mutable u32 locator_;
  u32 base_locator_;
  mutable u32 revision_;

  u32 get_version() const
  {
    const big_u32* stored_version = reinterpret_cast<const big_u32*>(this->value_.data()) - 1;
    return *stored_version;
  }
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Reduced-size Value type without key view; for ART-based indexing.
 */
class MemTableValueEntry
{
 public:
  using Self = MemTableValueEntry;

  MemTableValueEntry() = default;

  MemTableValueEntry(MemTableValueEntry&&) = default;
  MemTableValueEntry& operator=(MemTableValueEntry&&) = default;

  MemTableValueEntry(const MemTableValueEntry&) = default;
  MemTableValueEntry& operator=(const MemTableValueEntry&) = default;

  explicit MemTableValueEntry(const char* key_data,
                              const char* value_data,
                              u32 value_size,
                              u32 locator) noexcept
      : key_data_{key_data}
      , value_data_{value_data}
      , value_size_{value_size}
      , locator_{locator}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const char* key_data_;
  const char* value_data_;
  mutable u32 value_size_;
  mutable u32 locator_;

  u32 get_version() const
  {
    const big_u32* stored_version = reinterpret_cast<const big_u32*>(this->value_data_) - 1;
    return *stored_version;
  }

  KeyView key_view() const
  {
    const u16 key_size = *(reinterpret_cast<const little_u16*>(this->key_data_) - 1);
    return KeyView{this->key_data_, key_size};
  }

  ValueView value_view() const
  {
    return ValueView::from_str(std::string_view{this->value_data_, this->value_size_});
  }
};

static_assert(sizeof(MemTableValueEntry) == 24);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Used to insert or update a key/value entry in a hash set.
 */
template <typename StorageT>
struct MemTableValueEntryInserter {
  MemTableValueEntryInserter(const MemTableValueEntryInserter&) = delete;
  MemTableValueEntryInserter& operator=(const MemTableValueEntryInserter&) = delete;

  /** \brief Constructs a new inserter for the given key/value pair.
   */
  template <typename K, typename V>
  explicit MemTableValueEntryInserter(std::atomic<i64>& current_mem_table_byte_size,
                                      i64 mem_table_size_limit,
                                      bool replace_old_value_size,
                                      StorageT& storage_arg,
                                      K&& key_arg,
                                      V&& value_arg,
                                      u32 version_arg) noexcept
      : current_mem_table_byte_size_{current_mem_table_byte_size}
      , mem_table_size_limit_{mem_table_size_limit}
      , replace_old_value_size_{replace_old_value_size}
      , storage{storage_arg}
      , key{BATT_FORWARD(key_arg)}
      , value{BATT_FORWARD(value_arg)}
      , version{version_arg}
      , hash_val{get_key_hash_val(key)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs (set at construction-time)

  std::atomic<i64>& current_mem_table_byte_size_;
  const i64 mem_table_size_limit_;
  const bool replace_old_value_size_;
  StorageT& storage;
  const std::string_view key;
  const ValueView value;
  const u32 version;
  const u64 hash_val;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs (set by store_insert or store_update).

  ValueView stored_value;
  u32 stored_locator;
  bool inserted = false;
  const MemTableValueEntry* entry = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status update_table_size(i64 table_size_delta)
  {
    const i64 old_table_size = this->current_mem_table_byte_size_.fetch_add(table_size_delta);
    const i64 new_table_size = old_table_size + table_size_delta;

    if (new_table_size > this->mem_table_size_limit_) {
      this->current_mem_table_byte_size_.fetch_sub(table_size_delta);
      return {batt::StatusCode::kResourceExhausted};
    }

    return OkStatus();
  }

  Status insert_new(void* entry_memory)
  {
    char* key_dst;
    const usize key_len = this->key.size();
    const usize value_len = this->value.size();
    const usize insert_size = sizeof(little_u16)  // header
                              + key_len           // key
                              + sizeof(big_u32)   // version-suffix
                              + value_len;        // value

    BATT_REQUIRE_OK(this->update_table_size(insert_size));

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

      this->entry =
          new (entry_memory) MemTableValueEntry{key_dst, value_dst, (u32)value_len, locator};
    });

    this->inserted = true;

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    const usize value_len = this->value.size();
    const usize update_size = sizeof(PackedValueUpdate)  // header
                              + value_len;               // value

    const i64 table_size_delta = (this->replace_old_value_size_)
                                     ? ((i64)value_len - (i64)p_entry->value_size_)
                                     : (i64)value_len;
    // ^^^^
    // TODO [tastolfi 2025-05-27] we need to make sure that we eventually do fill up a MemTable,
    // even if we just overwrite a single key over and over again.

    BATT_REQUIRE_OK(this->update_table_size(table_size_delta));

    this->storage.store_data(update_size, [&](u32 locator, const MutableBuffer& buffer) {
      this->stored_locator = locator;

      auto* header = place_first<PackedValueUpdate>(buffer.data());

      header->key_len = 0;
      header->revision = 0;      // TODO [tastolfi 2025-07-24]
      header->base_locator = 0;  // TODO [tastolfi 2025-07-24]
      header->prev_locator = p_entry->locator_;
      header->version = this->version;

      auto* value_dst = place_next<char>(header, 1);
      std::memcpy(value_dst, this->value.data(), value_len);
      this->stored_value = ValueView::from_str(std::string_view{value_dst, value_len});

      this->entry = p_entry;

      p_entry->value_data_ = value_dst;
      p_entry->value_size_ = value_len;
      p_entry->locator_ = locator;
    });

    return OkStatus();
  }
};

}  // namespace turtle_kv
