#include <turtle_kv/mem_table.hpp>
//

#include <turtle_kv/import/env.hpp>

#include <turtle_kv/util/env_param.hpp>

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

namespace {

constexpr usize kHashIndexOverheadPct = 285;
constexpr usize kOrderedIndexOverheadPct = 35;
constexpr usize kArtIndexOverheadPct = 50;

}  // namespace

TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_hash_index, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_ordered_index, true);
TURTLE_KV_ENV_PARAM(bool, turtlekv_memtable_count_latest_update_only, true);
TURTLE_KV_ENV_PARAM(u32, turtlekv_memtable_hash_bucket_div, 32);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ MemTable::RuntimeOptions MemTable::RuntimeOptions::with_default_values() noexcept
{
  return RuntimeOptions{
      .limit_size_by_latest_updates_only =
          getenv_param<turtlekv_memtable_count_latest_update_only>(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(llfs::PageCache& page_cache,
                                KVStoreMetrics& metrics,
                                usize max_byte_size,
                                Optional<u64> id) noexcept
    : page_cache_{page_cache}
    , metrics_{metrics}
    , is_finalized_{false}
    , hash_index_{}
    , ordered_index_{}
    , art_index_{}
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
  usize overhead_estimate_pct = 0;

  if (getenv_param<turtlekv_memtable_hash_index>()) {
    this->hash_index_.emplace(max_byte_size / getenv_param<turtlekv_memtable_hash_bucket_div>());
    overhead_estimate_pct += kHashIndexOverheadPct;

    if (getenv_param<turtlekv_memtable_ordered_index>()) {
      this->ordered_index_.emplace();
      overhead_estimate_pct += kOrderedIndexOverheadPct;
    }

  } else {
    this->art_index_.emplace();
    overhead_estimate_pct += kArtIndexOverheadPct;
  }

  this->metrics_.mem_table_alloc.add(1);

  const usize total_overhead_estimate = (max_byte_size * overhead_estimate_pct + 99) / 100;
  this->reserve_cache_space(total_overhead_estimate);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTable::~MemTable() noexcept
{
  for (ChangeLogWriter::BlockBuffer* buffer : this->blocks_) {
    buffer->remove_ref(1);
  }

  [[maybe_unused]] const bool b = this->finalize();

  this->metrics_.mem_table_free.add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  StorageImpl storage{*this, context, OkStatus()};

  if (this->hash_index_) {
    MemTableEntryInserter<StorageImpl> inserter{
        this->current_byte_size_,
        this->max_byte_size_,
        this->runtime_options_.limit_size_by_latest_updates_only,
        storage,
        key,
        value,
        this->version_.fetch_add(1),
    };

    BATT_REQUIRE_OK(this->hash_index_->insert(inserter));

    // If this is a key we haven't seen before, add it to the ordered index.
    //
    if (this->ordered_index_ && inserter.inserted) {
      this->ordered_index_->insert(get_key(*inserter.entry));
    }

  } else {
    MemTableValueEntryInserter<StorageImpl> inserter{
        this->current_byte_size_,
        this->max_byte_size_,
        this->runtime_options_.limit_size_by_latest_updates_only,
        storage,
        key,
        value,
        this->version_.fetch_add(1),
    };

    BATT_REQUIRE_OK(this->art_index_->insert(key, inserter));
  }

  return storage.status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  if (this->hash_index_) {
    MemTableEntry entry;
    if (!this->hash_index_->find_key(key, entry)) {
      return None;
    }
    return entry.value_;
  }

  Optional<MemTableValueEntry> entry = this->art_index_->find(key);
  if (!entry) {
    return None;
  }

  return entry->value_view();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan(const KeyView& min_key,
                     const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  usize n_found = 0;

  if (this->ordered_index_) {
    this->ordered_index_->scan(min_key, [&](const std::string_view& tmp_key) {
      if (n_found >= items_out.size()) {
        return false;
      }
      MemTableEntry entry;
      if (this->hash_index_->find_key(tmp_key, entry)) {
        items_out[n_found].first = entry.key_;
        items_out[n_found].second = entry.value_;
        ++n_found;
      }
      return true;
    });

  } else {
    ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kTrue> scanner{*this->art_index_,
                                                                           min_key};

    for (; n_found < items_out.size() && !scanner.is_done(); ++n_found) {
      items_out[n_found].first = scanner.get_key();
      items_out[n_found].second = scanner.get_value().value_view();
      scanner.advance();
    }
  }

  return n_found;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  if (this->hash_index_) {
    const MemTableEntry* entry = this->hash_index_->unsynchronized_find_key(key);
    if (!entry) {
      return None;
    }
    return entry->value_;
  }

  const MemTableValueEntry* entry = this->art_index_->unsynchronized_find(key);
  if (!entry) {
    return None;
  }
  return entry->value_view();
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
  BATT_CHECK(this->is_finalized_);

  for (;;) {
    const u32 observed = this->compaction_state_.fetch_or(Self::kCompactionState_InProgress);
    if (observed == Self::kCompactionState_Todo) {
      break;
    }
    if (observed == Self::kCompactionState_Complete) {
      return this->compacted_edits_;
    }
    BATT_CHECK_EQ(observed, Self::kCompactionState_InProgress);
  }

  auto on_scope_exit = batt::finally([&] {
    const u32 locked_state = this->compaction_state_.fetch_or(kCompactionState_Complete);
    BATT_CHECK_EQ(locked_state, Self::kCompactionState_InProgress);
  });

  std::vector<EditView> edits_out;

  if (this->hash_index_) {
    edits_out = this->compact_hash_index();
  } else {
    edits_out = this->compact_art_index();
  }

  this->compacted_edits_.append(std::move(edits_out));

  return this->compacted_edits_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<EditView> MemTable::compact_hash_index()
{
  std::vector<EditView> edits_out;
  usize total_keys = this->hash_index_->size();
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

  if (this->ordered_index_) {
    ART<void>::Scanner<ARTBase::Synchronized::kFalse> scanner{
        *this->ordered_index_,
        /*lower_bound_key=*/std::string_view{}};

    while (!scanner.is_done()) {
      const std::string_view& tmp_key = scanner.get_key();

      const MemTableEntry* entry = this->hash_index_->unsynchronized_find_key(tmp_key);
      BATT_CHECK_NOT_NULLPTR(entry);

      edits_out.emplace_back(get_key(*entry), value_from_entry(*entry));

      scanner.advance();
    }

  } else {
    this->hash_index_->for_each(  //
        [&](const MemTableEntry& entry) {
          KeyView key = get_key(entry);
          ValueView value = value_from_entry(entry);
          edits_out.emplace_back(key, value);
        });

    std::sort(edits_out.begin(), edits_out.end(), KeyOrder{});
  }

  return edits_out;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<EditView> MemTable::compact_art_index()
{
  std::vector<EditView> edits_out;

  ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kFalse> scanner{
      *this->art_index_,
      /*min_key=*/std::string_view{}};

  for (; !scanner.is_done(); scanner.advance()) {
    const MemTableValueEntry& entry = scanner.get_value();
    edits_out.emplace_back(entry.key_view(), entry.value_view());
    //
    // TODO [tastolfi 2025-07-24] compact merge-op values
  }

  return edits_out;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<Slice<const EditView>> MemTable::poll_compacted_edits() const
{
  if (this->compaction_state_.load() != Self::kCompactionState_Complete) {
    return None;
  }
  return this->compacted_edits_slice_impl();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<const EditView> MemTable::await_compacted_edits() const
{
  while (this->compaction_state_.load() != Self::kCompactionState_Complete) {
    continue;
  }
  return this->compacted_edits_slice_impl();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<const EditView> MemTable::compacted_edits_slice_impl() const
{
  MergeCompactor::ResultSet</*decay_to_items=*/false>::range_type flat_edits =
      this->compacted_edits_.get();

  Flatten<const Chunk<const EditView*>*, const EditView*> flat_edits_begin = flat_edits.begin();
  Flatten<const Chunk<const EditView*>*, const EditView*> flat_edits_end = flat_edits.end();

  const Chunk<const EditView*>* chunks_begin = flat_edits_begin.chunk_iter_;
  const Chunk<const EditView*>* chunks_end = flat_edits_end.chunk_iter_;

  const isize n_chunks = std::distance(chunks_begin, chunks_end);

  if (n_chunks == 1) {
    BATT_CHECK_EQ(flat_edits_begin.cached_chunk_.offset, 0);
    return flat_edits_begin.cached_chunk_.items;
  }

  BATT_CHECK_EQ(n_chunks, 0);
  return {};
}

}  // namespace turtle_kv
