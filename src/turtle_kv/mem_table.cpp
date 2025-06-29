#include <turtle_kv/mem_table.hpp>
//

#include <turtle_kv/import/env.hpp>

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

#define GET_USE_ORDERED_INDEX()                                                                    \
  static const bool use_ordered_index = [] {                                                       \
    const bool b = getenv_as<bool>("turtlekv_memtable_ordered_index").value_or(true);              \
    LOG(INFO) << "turtlekv_memtable_ordered_index=" << b;                                          \
    return b;                                                                                      \
  }()

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ MemTable::RuntimeOptions MemTable::RuntimeOptions::with_default_values() noexcept
{
  return RuntimeOptions{
      .limit_size_by_latest_updates_only =
          getenv_as<bool>("turtlekv_memtable_count_latest_update_only").value_or(true),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(usize max_byte_size, Optional<u64> id) noexcept
    : is_finalized_{false}
    , hash_index_{max_byte_size /
                  getenv_as<usize>("turtlekv_memtable_hash_bucket_div").value_or(32)}
    , max_byte_size_{BATT_CHECKED_CAST(i64, max_byte_size)}
    , current_byte_size_{0}
    , self_id_{id.or_else([&] {
      return MemTable::next_id();
    })}
    , next_block_owner_id_{this->get_next_block_owner_id()}
    , version_{0}
    , block_list_mutex_{}
    , blocks_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTable::~MemTable() noexcept
{
  for (ChangeLogWriter::BlockBuffer* buffer : this->blocks_) {
    buffer->remove_ref(1);
  }

  [[maybe_unused]] const bool b = this->finalize();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  GET_USE_ORDERED_INDEX();

  StorageImpl storage{*this, context, OkStatus()};

  MemTableEntryInserter<StorageImpl> inserter{
      this->current_byte_size_,
      this->max_byte_size_,
      this->runtime_options_.limit_size_by_latest_updates_only,
      storage,
      key,
      value,
      this->version_.fetch_add(1),
  };

  BATT_REQUIRE_OK(this->hash_index_.insert(inserter));

  // If this is a key we haven't seen before, add it to the ordered index.
  //
  if (use_ordered_index && inserter.inserted) {
    this->ordered_index_.insert(get_key(*inserter.entry));
  }

  return storage.status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  MemTableEntry entry;
  if (!this->hash_index_.find_key(key, entry)) {
    return None;
  }
  return entry.value_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan(const KeyView& min_key,
                     const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  usize n_found = 0;

  // const u32 read_version = this->version_.load();
  this->ordered_index_.scan(min_key, [&](const std::string_view& tmp_key) {
    if (n_found >= items_out.size()) {
      return false;
    }
    MemTableEntry entry;
    if (this->hash_index_.find_key(tmp_key, entry)) {
      items_out[n_found].first = entry.key_;
      items_out[n_found].second = entry.value_;
      ++n_found;
    }
    return true;
  });

  return n_found;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  const MemTableEntry* entry = this->hash_index_.unsynchronized_find_key(key);
  if (!entry) {
    return None;
  }
  return entry->value_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::finalized_scan(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  BATT_CHECK(this->is_finalized_);

  usize k = this->scan_keys_impl(min_key, items_out);

  for (usize i = 0; i < k; ++i) {
    items_out[i].second = this->finalized_get(items_out[i].first).value_or_panic();
  }

  return k;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan_keys_impl(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  BATT_PANIC() << "Fix scanning!";
  return 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::finalize() noexcept
{
  const bool prior_value = this->is_finalized_.exchange(true);
  return prior_value == false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false> MemTable::compact() noexcept
{
  GET_USE_ORDERED_INDEX();

  BATT_CHECK(this->is_finalized_);

  usize total_keys = 0;
  total_keys = this->hash_index_.size();

  std::vector<EditView> edits_out;
  edits_out.reserve(total_keys);

  const auto value_from_entry = [this](const MemTableEntry& entry) {
    ValueView value = entry.value_;
    if (value.needs_combine()) {
      ConstBuffer slot_buffer = this->fetch_slot(entry.locator_);
      auto* packed_update = static_cast<const PackedValueUpdate*>(slot_buffer.data());
      if (packed_update->key_len == 0) {
        do {
          slot_buffer = this->fetch_slot(packed_update->prev_locator);
          packed_update = static_cast<const PackedValueUpdate*>(slot_buffer.data());

          if (packed_update->key_len == 0) {
            ConstBuffer value_buffer = slot_buffer + sizeof(PackedValueUpdate);

            value = combine(value, ValueView::from_buffer(value_buffer));

          } else {
            ConstBuffer value_buffer =
                slot_buffer + (sizeof(little_u16) + packed_update->key_len + sizeof(big_u32));

            value = combine(value, ValueView::from_buffer(value_buffer));
            break;
          }

        } while (value.needs_combine());
      }
      // else (key_len == 0) - the current revision is also the first; nothing else can be done.
    }
    return value;
  };

  if (use_ordered_index) {
    ART::Scanner<ART::Synchronized::kFalse> scanner{this->ordered_index_,
                                                    /*lower_bound_key=*/std::string_view{}};

    while (!scanner.is_done()) {
      const std::string_view& tmp_key = scanner.get_key();

      const MemTableEntry* entry = this->hash_index_.unsynchronized_find_key(tmp_key);
      BATT_CHECK_NOT_NULLPTR(entry);

      edits_out.emplace_back(get_key(*entry), value_from_entry(*entry));

      scanner.advance();
    }

  } else {
    this->hash_index_.for_each(  //
        [&](const MemTableEntry& entry) {
          KeyView key = get_key(entry);
          ValueView value = value_from_entry(entry);
          edits_out.emplace_back(key, value);
        });

    std::sort(edits_out.begin(), edits_out.end(), KeyOrder{});
  }

  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  result_set.append(std::move(edits_out));

  return result_set;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer MemTable::fetch_slot(u32 locator) const noexcept
{
  //
  // MUST only be called once the MemTable is finalized.

  const usize block_index = (locator >> 16);
  const usize slot_index = (locator & 0xffff);

  return this->blocks_[block_index]->get_slot(slot_index);
}

}  // namespace turtle_kv
