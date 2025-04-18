#include <turtle_kv/mem_table.hpp>
//

#include <turtle_kv/import/env.hpp>

#include <batteries/async/task.hpp>

namespace turtle_kv {

namespace {

i32 get_n_shards_log2()
{
  static const i32 n_shards = getenv_as<i32>("turtlekv_memtable_hash_shards")
                                  .value_or((std::thread::hardware_concurrency() + 1) / 2);
  static const i32 n_shards_log2 =
      std::min<i32>(MemTable::kMaxShardsLog2, batt::log2_ceil(n_shards));

  return n_shards_log2;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(usize max_byte_size, Optional<u64> id) noexcept
    : is_finalized_{false}
    , n_shards_log2_{get_n_shards_log2()}
    , n_shards_{usize{1} << this->n_shards_log2_}
    , shard_mask_{this->n_shards_ - 1}
    , hash_mutex_storage_(this->n_shards_)
    , max_byte_size_{max_byte_size}
    , current_byte_size_{0}
    , self_id_{id.or_else([&] {
      return MemTable::next_id();
    })}
    , next_block_owner_id_{this->get_next_block_owner_id()}
    , version_{0}
    , block_list_mutex_{}
    , blocks_{}
    , hashed_(this->n_shards_)
    , ordered_{}
{
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

  [[maybe_unused]] const bool b = this->finalize();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status MemTable::put(ChangeLogWriter::Context& context,
                     const KeyView& key,
                     const ValueView& value) noexcept
{
  const usize size_estimate = std::max<usize>(2 + key.size() + 4, 32)  //
                              + value.size() + 2 + sizeof(PackedValueUpdate);
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

    if (this->is_finalized_) {
      return batt::StatusCode::kResourceExhausted;
    }

    auto [iter, inserted] = this->hashed_[shard_i].emplace(inserter);
    if (!inserted) {
      iter->update(inserter);
#if 0
    } else {
      absl::WriterMutexLock lock{this->ordered_mutex()};
      this->ordered_.emplace(get_key(*iter));
#endif
    }
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
  BATT_PANIC() << "Fix scanning!";

  usize k = 0;
  {
    absl::ReaderMutexLock lock{this->ordered_mutex()};
    k = this->scan_keys_impl(min_key, items_out);
  }

  for (usize i = 0; i < k; ++i) {
    items_out[i].second = this->get(items_out[i].first).value_or_panic();
  }

  return k;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  BATT_CHECK(this->is_finalized_);

  MemTableQuery query{key};

  return this->get_impl(query, this->shard_for_hash_val(query.hash_val));
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
usize MemTable::scan_keys_impl(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  BATT_PANIC() << "Fix scanning!";

  auto first = this->ordered_.lower_bound(min_key);
  auto ordered_end = this->ordered_.end();

  usize k = 0;
  while (first != ordered_end && k != items_out.size()) {
    items_out[k].first = *first;
    ++first;
    ++k;
  }

  return k;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool MemTable::finalize() noexcept
{
  usize n_shards_locked = 0;
  auto on_scope_exit = batt::finally([&] {
    for (usize i = 0; i < n_shards_locked; ++i) {
      this->hashed_mutex_for_shard(i)->Unlock();
    }
  });
  for (; n_shards_locked < this->n_shards_; ++n_shards_locked) {
    this->hashed_mutex_for_shard(n_shards_locked)->Lock();
  }

  const bool prior_value = this->is_finalized_;
  this->is_finalized_ = true;
  return prior_value == false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false> MemTable::compact() noexcept
{
  BATT_CHECK(this->is_finalized_);

  using HashedIndex = absl::flat_hash_set<MemTableEntry, DefaultStrHash, DefaultStrEq>;

  usize total_keys = 0;
  for (const HashedIndex& shard : this->hashed_) {
    total_keys += shard.size();
  }

  std::vector<EditView> edits_out;
  edits_out.reserve(total_keys);

  {
    const usize num_edits = this->hashed_.size();
    edits_out.reserve(num_edits);

    for (const HashedIndex& shard : this->hashed_) {
      const auto last = shard.end();
      for (auto iter = shard.begin(); iter != last; ++iter) {
        KeyView key = get_key(*iter);
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
  }
  std::sort(edits_out.begin(), edits_out.end(), KeyOrder{});

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
