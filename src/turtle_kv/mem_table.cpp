#include <turtle_kv/mem_table.hpp>
//

#include <batteries/async/task.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(std::unique_ptr<ChangeLogWriter>&& writer,
                                usize max_byte_size,
                                Optional<u64> id) noexcept
    : writer_{std::move(writer)}
    , max_byte_size_{max_byte_size}
    , current_byte_size_{0}
    , self_id_{id.or_else([&] {
      return MemTable::next_id();
    })}
{
  this->hashed_.resize(this->n_shards_);
  this->hash_mutex_storage_.resize(this->n_shards_);

  for (MutexStorage& storage : this->hash_mutex_storage_) {
    new (std::addressof(storage)) batt::CpuCacheLineIsolated<absl::Mutex>{};
  }
  this->hash_mutex_ = as_slice(
      reinterpret_cast<batt::CpuCacheLineIsolated<absl::Mutex>*>(this->hash_mutex_storage_.data()),
      this->n_shards_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTable::~MemTable() noexcept
{
  for (ChangeLogWriter::BlockBuffer* buffer : this->blocks_) {
    buffer->remove_ref(1);
  }

  std::unique_ptr<ChangeLogWriter> writer = this->finalize();
  if (writer) {
    writer->halt();
    writer->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  const usize size_estimate = std::max<usize>(2 + key.size() + 4, 32) + value.size() + 2 + 16;
  const usize prior_byte_size = this->current_byte_size_.fetch_add(size_estimate);
  const usize new_byte_size = prior_byte_size + size_estimate;

  if (new_byte_size > this->max_byte_size_) {
    this->current_byte_size_.fetch_sub(size_estimate);
    return batt::StatusCode::kResourceExhausted;
  }

  StorageImpl storage{*this, context, OkStatus()};
  MemTableEntryInserter<StorageImpl> inserter{storage, key, value, this->version_.fetch_add(1)};
  Optional<std::string_view> new_key;

  {
    const u64 shard_i = this->shard_for_hash_val(inserter.hash_val);
    absl::WriterMutexLock lock{this->hashed_mutex_for_shard(shard_i)};

    auto [iter, inserted] = this->hashed_[shard_i].emplace(inserter);
    if (!inserted) {
      iter->update(inserter);
    } else {
      new_key.emplace(get_key(*iter));
    }
  }

  if (new_key) {
    absl::WriterMutexLock lock{this->ordered_mutex()};
    this->ordered_.emplace(*new_key);
  }

  return storage.status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  MemTableQuery query{key};
  const u64 shard_i = this->shard_for_hash_val(query.hash_val);

  absl::ReaderMutexLock lock{this->hashed_mutex_for_shard(shard_i)};

  return this->get_impl(query, shard_i);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan(const KeyView& min_key,
                     const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  absl::ReaderMutexLock lock{this->ordered_mutex()};

  return this->scan_impl(min_key, items_out, /*need_locks=*/true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  BATT_CHECK(!this->writer_);

  MemTableQuery query{key};

  return this->get_impl(query, this->shard_for_hash_val(query.hash_val));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::finalized_scan(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  BATT_CHECK(!this->writer_);

  return this->scan_impl(min_key, items_out, /*need_locks=*/false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get_impl(const MemTableQuery& query, u64 shard_i) noexcept
{
  auto iter = this->hashed_[shard_i].find(query);
  if (iter == this->hashed_[shard_i].end()) {
    return None;
  }

  // TODO [tastolfi 2025-02-25] compact this value if necessary (see MemTable::compact for example).
  //
  return iter->value_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan_impl(const KeyView& min_key,
                          const Slice<std::pair<KeyView, ValueView>>& items_out,
                          bool needs_lock) noexcept
{
  auto first = this->ordered_.lower_bound(min_key);
  auto ordered_end = this->ordered_.end();

  usize k = 0;
  while (first != ordered_end && k != items_out.size()) {
    MemTableQuery query{*first};
    const u64 shard_i = this->shard_for_hash_val(query.hash_val);
    Optional<absl::ReaderMutexLock> hash_read_lock;
    if (needs_lock) {
      hash_read_lock.emplace(this->hashed_mutex_for_shard(shard_i));
    }

    auto iter = this->hashed_[shard_i].find(*first);
    BATT_CHECK_NE(iter, this->hashed_[shard_i].end());

    items_out[k].first = *first;
    items_out[k].second = iter->value_;

    ++first;
    ++k;
  }

  return k;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<ChangeLogWriter> MemTable::finalize() noexcept
{
  absl::WriterMutexLock ordered_lock{this->ordered_mutex()};

  usize n_shards_locked = 0;
  auto on_scope_exit = batt::finally([&] {
    for (usize i = 0; i < n_shards_locked; ++i) {
      this->hashed_mutex_for_shard(i)->Unlock();
    }
  });
  for (; n_shards_locked < this->n_shards_; ++n_shards_locked) {
    this->hashed_mutex_for_shard(n_shards_locked)->Lock();
  }

  return std::move(this->writer_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false> MemTable::compact() noexcept
{
  BATT_CHECK(!this->writer_);

  std::vector<EditView> edits_out;
  {
    const usize num_edits = this->hashed_.size();
    edits_out.reserve(num_edits);

    for (const KeyView& key : this->ordered_) {
      MemTableQuery query{key};
      const usize shard_i = this->shard_for_hash_val(query.hash_val);
      auto iter = this->hashed_[shard_i].find(query);

      ValueView value = iter->value_;
      if (value.needs_combine()) {
        ConstBuffer slot_buffer = this->fetch_slot(iter->locator_);
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
      edits_out.emplace_back(key, value);
    }
  }

  MergeCompactor::ResultSet</*decay_to_items=*/false> result_set;
  result_set.append(std::move(edits_out));

  return result_set;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer MemTable::fetch_slot(u32 locator) const noexcept
{
  const usize block_index = (locator >> 16);
  const usize slot_index = (locator & 0xffff);

  return this->blocks_[block_index]->get_slot(slot_index);
}

}  // namespace turtle_kv
