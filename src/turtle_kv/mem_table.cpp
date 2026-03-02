#include <turtle_kv/mem_table.hpp>
//

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <turtle_kv/import/env.hpp>

#include <turtle_kv/core/packed_sizeof_edit.hpp>

#include <turtle_kv/util/atomic.hpp>

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

namespace turtle_kv {

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
                                usize max_bytes_per_batch,
                                usize max_batch_count,
                                Optional<u64> id) noexcept
    : page_cache_{page_cache}
    , metrics_{metrics}
    , art_metrics_{}
    , is_finalized_{false}
    , hash_index_{}
    , ordered_index_{}
    , art_index_{}
    , max_bytes_per_batch_{BATT_CHECKED_CAST(i64, max_bytes_per_batch)}
    , max_batch_count_{BATT_CHECKED_CAST(i64, max_batch_count)}
    , max_byte_size_{this->calculate_max_byte_size()}
    , current_byte_size_{0}
    , self_id_{id.or_else([&] {
      return MemTable::next_id();
    })}
    , next_block_owner_id_{this->get_next_block_owner_id()}
    , version_{0}
    , block_list_mutex_{}
    , blocks_{}
{
  if (getenv_param<turtlekv_memtable_hash_index>()) {
    this->hash_index_.emplace(this->max_byte_size_ /
                              getenv_param<turtlekv_memtable_hash_bucket_div>());

    if (getenv_param<turtlekv_memtable_ordered_index>()) {
      this->ordered_index_.emplace();
    }

  } else {
    this->art_index_.emplace(this->art_metrics_);
  }

  this->metrics_.mem_table_alloc.add(1);
  this->metrics_.mem_table_count_stats.update(this->metrics_.mem_table_alloc.get() -
                                              this->metrics_.mem_table_free.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTable::~MemTable() noexcept
{
  // Try to detect double-deletions.
  //
  BATT_CHECK_EQ(this->magic_num_.exchange(Self::kDeadMagicNum), Self::kAliveMagicNum);

  this->metrics_.mem_table_log_bytes_freed.add(this->block_size_total_);

  for (ChangeLogWriter::BlockBuffer* buffer : this->blocks_) {
    buffer->remove_ref(1);
  }

  [[maybe_unused]] const bool b = this->finalize();

  this->metrics_.mem_table_free.add(1);
  this->metrics_.mem_table_count_stats.update(this->metrics_.mem_table_alloc.get() -
                                              this->metrics_.mem_table_free.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  {
    // Update the maximum packed item size, and possibly also the maximum total byte size.  When max
    // item size goes up, so does the maximum number of wasted bytes at the end of a batch, so the
    // batch limit might go down.
    //
    const i64 item_size = PackedSizeOfEdit{}(key.size(), value.size());
    if (item_size > atomic_clamp_min(this->max_item_size_, item_size)) {
      atomic_clamp_max(this->max_byte_size_, this->calculate_max_byte_size());
    }

    const i64 old_mem_table_size = this->current_byte_size_.fetch_add(item_size);
    const i64 new_mem_table_size = old_mem_table_size + item_size;

    if (new_mem_table_size > this->max_byte_size_.load()) {
      this->current_byte_size_.fetch_sub(item_size);
      return {batt::StatusCode::kResourceExhausted};
    }
  }

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

#if !TURTLE_KV_BIG_MEM_TABLES

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

  ART<MemTableValueEntry>::Scanner<ARTBase::Synchronized::kFalse,  //
                                   /*kValuesOnly=*/true>
      scanner{*this->art_index_,
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

#endif  // !TURTLE_KV_BIG_MEM_TABLES

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 MemTable::update_external_cache_alloc()
{
  // The number of bytes to claim (if positive) or release (negative) as external allocation
  // from the PageCache; the return value of this function.
  //
  i64 cache_alloc_delta = 0;

  ++this->since_last_cache_alloc_update_;

  if (!this->cache_alloc_in_progress_ &&
      this->since_last_cache_alloc_update_ >= MemTable::kBlocksPerExternalCacheAllocUpdate) {
    this->cache_alloc_in_progress_ = true;
    this->since_last_cache_alloc_update_ = 0;

    if (getenv_param<turtlekv_memtable_cache_alloc_log>()) {
      BATT_CHECK_GE(this->block_size_total_, this->block_size_last_update_);
      const i64 block_delta = this->block_size_total_ - this->block_size_last_update_;
      cache_alloc_delta += block_delta;
      this->block_size_last_update_ = this->block_size_total_;
    }

    if (getenv_param<turtlekv_memtable_cache_alloc_art>()) {
      const i64 new_art_size = (i64)this->art_metrics_.bytes_in_use();
      const i64 art_delta = new_art_size - this->art_reserved_size_;
      cache_alloc_delta += art_delta;
      this->art_reserved_size_ = new_art_size;
    }

    if (cache_alloc_delta == 0) {
      this->cache_alloc_in_progress_ = false;
    }
  }

  return cache_alloc_delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemTable::handle_external_cache_alloc(i64 cache_alloc_delta)
{
  if (cache_alloc_delta > 0) {
    const i64 observed_current_size = this->current_byte_size_.load();

    llfs::PageCacheOvercommit overcommit;
    overcommit.allow(true);

    llfs::PageCache::ExternalAllocation alloc =
        this->page_cache_.allocate_external(cache_alloc_delta, overcommit);

    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      this->total_cache_alloc_.subsume(std::move(alloc));
      this->cache_alloc_in_progress_ = false;
    }

    // If we trigger cache overcommit, then cut the size limit down so we don't make things too
    // much worse.
    //
    if (overcommit.is_triggered()) {
      atomic_clamp_max(this->current_byte_size_,
                       std::max<i64>(observed_current_size, this->max_bytes_per_batch_ / 2));

      on_page_cache_overcommit(
          [this, observed_current_size](std::ostream& out) {
            out << "Truncating MemTable size due to cache overcommit;"
                << BATT_INSPECT(this->current_byte_size_)
                << BATT_INSPECT(this->max_bytes_per_batch_) << BATT_INSPECT(observed_current_size);
          },
          this->page_cache_,
          this->metrics_.overcommit);
    }

  } else if (cache_alloc_delta < 0) {
    StatusOr<llfs::PageCache::ExternalAllocation> alloc_to_release;
    {
      absl::MutexLock lock{&this->block_list_mutex_};
      BATT_CHECK(this->cache_alloc_in_progress_);
      alloc_to_release = this->total_cache_alloc_.split(-cache_alloc_delta);
      this->cache_alloc_in_progress_ = false;
    }
    BATT_CHECK_OK(alloc_to_release)
        << BATT_INSPECT(cache_alloc_delta) << BATT_INSPECT(this->total_cache_alloc_.size());
  }
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
i64 MemTable::calculate_max_byte_size() const
{
  const i64 max_wasted_per_batch = this->max_item_size_.load() - 1;
  const i64 min_full_batch_size = this->max_bytes_per_batch_ - max_wasted_per_batch;
  return this->max_batch_count_ * min_full_batch_size;
}

#if TURTLE_KV_BIG_MEM_TABLES

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class MemTable::BatchCompactor
//

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::BatchCompactor::BatchCompactor(MemTable& mem_table,
                                                      usize byte_size_limit) noexcept
    : mem_table_{mem_table}
    , byte_size_limit_{byte_size_limit}
    , batch_count_{0}
    , scanner_{[this]() -> auto& {
                 BATT_CHECK(this->mem_table_.art_index_)
                     << "BatchCompactor can only be used with art_index_! (no hash)";
                 return *this->mem_table_.art_index_;
               }(),
               /*min_key=*/std::string_view{}}
{
  BATT_CHECK(this->mem_table_.is_finalized());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::BatchCompactor::has_next() const
{
  return !this->scanner_.is_done();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false>
MemTable::BatchCompactor::consume_next() noexcept
{
  MergeCompactor::ResultSet</*decay_to_items=*/false> compacted_edits;

  compacted_edits.append([&]() -> std::vector<EditView> {
    std::vector<EditView> edits_out;
    //----- --- -- -  -  -   -
    PackedSizeOfEdit packed_size_of;
    usize total_size = 0;

    for (; !this->scanner_.is_done(); this->scanner_.advance()) {
      const MemTableValueEntry& entry = this->scanner_.get_value();

      EditView edit{entry.key_view(), entry.value_view()};
      total_size += packed_size_of(edit);

      if (total_size < this->byte_size_limit_) {
        edits_out.emplace_back(edit);
      } else {
        break;
      }
    }
    //----- --- -- -  -  -   -
    return edits_out;
  }());

  if (!compacted_edits.empty()) {
    atomic_clamp_min(this->mem_table_.max_batch_index_, this->batch_count_);
    ++this->batch_count_;
  }

  return compacted_edits;
}

#endif  // TURTLE_KV_BIG_MEM_TABLES

}  // namespace turtle_kv
