//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ENTRY_HPP

#include <turtle_kv/mem_table/mem_table_entry_inserter.hpp>

#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_key_value_slot.hpp>
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Stores a single key update record for ART-based indexing.
 */
class MemTableValueEntry
{
 public:
  using Self = MemTableValueEntry;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MemTableValueEntry() = default;

  MemTableValueEntry(MemTableValueEntry&&) = default;
  MemTableValueEntry& operator=(MemTableValueEntry&&) = default;

  MemTableValueEntry(const MemTableValueEntry&) = default;
  MemTableValueEntry& operator=(const MemTableValueEntry&) = default;

  explicit MemTableValueEntry(const std::pair<KeyView, ValueView>& packed_pair,
                              EditOffset edit_offset) noexcept
      : key_data_{packed_pair.first.data()}
      , value_tag_{packed_pair.second.tag()}
      , key_size_{static_cast<u32>(packed_pair.first.size())}
      , value_data_union_{packed_pair.second.get_data_union()}
      , edit_offset_{edit_offset}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyView key_view() const
  {
    return KeyView{this->key_data_, this->key_size_};
  }

  ValueView value_view() const
  {
    return ValueView::from_tag_and_data_union(this->value_tag_, this->value_data_union_);
  }

  EditOffset edit_offset() const
  {
    return this->edit_offset_;
  }

  void update_value(const std::pair<KeyView, ValueView>& packed_pair, EditOffset edit_offset)
  {
    ValueView new_value = combine(packed_pair.second, this->value_view());

    this->key_data_ = packed_pair.first.data();
    this->value_tag_ = new_value.tag();
    //
    // (this->key_size_ unchanged)
    //
    this->value_data_union_ = new_value.get_data_union();
    this->edit_offset_ = edit_offset;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const char* key_data_;
  ValueView::TagInt32 value_tag_;
  u32 key_size_;
  ValueView::DataUnion value_data_union_;
  EditOffset edit_offset_{0};
};

static_assert(sizeof(MemTableValueEntry) == 32);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Returns the key for a MemTableValueEntry.
 */
BATT_ALWAYS_INLINE inline KeyView get_key(const MemTableValueEntry& entry)
{
  return entry.key_view();
}

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
  explicit MemTableValueEntryInserter(StorageT& storage_arg, K&& key_arg, V&& value_arg) noexcept
      : storage{storage_arg}
      , key{BATT_FORWARD(key_arg)}
      , value{BATT_FORWARD(value_arg)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs (set at construction-time)

  StorageT& storage;
  const KeyView key;
  const ValueView value;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs

  MemTableValueEntry* entry_out = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    const usize insert_size = packed_key_value_slot_size(this->key, this->value);

    BATT_REQUIRE_OK(this->storage.store_data(  //
        insert_size,                           //
        [this, entry_memory]                   //
        (const MutableBuffer& buffer, EditOffset edit_offset) {
          const std::pair<KeyView, ValueView> packed_pair =
              pack_key_value_slot(this->key, this->value, buffer);

          this->entry_out = new (entry_memory) MemTableValueEntry{packed_pair, edit_offset};
        }));

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    const usize update_size = packed_key_value_slot_size(this->key, this->value);

    BATT_REQUIRE_OK(this->storage.store_data(  //
        update_size,                           //
        [this, p_entry](const MutableBuffer& buffer, EditOffset edit_offset) {
          const std::pair<KeyView, ValueView> packed_pair =
              pack_key_value_slot(this->key, this->value, buffer);

          p_entry->update_value(packed_pair, edit_offset);

          this->entry_out = p_entry;
        }));

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(MemTableEntryInserter<MemTableValueEntryInserter>);
};

/** \brief Used during recovery; inserts a MemTableValueEntry without appending a slot to the change
 * log.
 */
struct MemTableRecoveryInserter {
  explicit MemTableRecoveryInserter(EditOffset edit_offset,
                                    const KeyView& key,
                                    const ValueView& value) noexcept
      : edit_offset_{edit_offset}
      , key_{key}
      , value_{value}
  {
  }

  MemTableRecoveryInserter(const MemTableRecoveryInserter&) = delete;
  MemTableRecoveryInserter& operator=(const MemTableRecoveryInserter&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Inputs

  EditOffset edit_offset_;
  KeyView key_;
  ValueView value_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Outputs

  MemTableValueEntry* entry_out = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status insert_new(void* entry_memory)
  {
    this->entry_out = new (entry_memory) MemTableValueEntry{
        std::make_pair(this->key_, this->value_),
        this->edit_offset_,
    };

    return OkStatus();
  }

  Status update_existing(MemTableValueEntry* p_entry)
  {
    this->entry_out = p_entry;
    p_entry->update_value(std::make_pair(this->key_, this->value_), this->edit_offset_);

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(MemTableEntryInserter<MemTableRecoveryInserter>);
};

}  // namespace turtle_kv
