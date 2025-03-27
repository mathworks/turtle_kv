#include <turtle_kv/mem_table.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTable::MemTable(std::unique_ptr<ChangeLogWriter>&& writer,
                                usize max_byte_size,
                                Optional<u64> id) noexcept
    : writer_{std::move(writer)}
    , context_{*this->writer_}
    , max_byte_size_{max_byte_size}
    , current_byte_size_{0}
    , self_id_{id.or_else([&] {
      return MemTable::next_id();
    })}
{
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
Status MemTable::put(const KeyView& key, const ValueView& value) noexcept
{
  const usize size_estimate = std::max<usize>(2 + key.size() + 4, 32) + value.size() + 2 + 16;

  WriteLockT lock{this->mutex_};

  if (size_estimate + this->current_byte_size_ > this->max_byte_size_) {
    return batt::StatusCode::kResourceExhausted;
  }
  this->current_byte_size_ += size_estimate;

  StorageImpl storage{*this, OkStatus()};
  MemTableEntryInserter<StorageImpl> inserter{storage, key, value, ++this->version_};

  auto [iter, inserted] = this->hashed_.emplace(inserter);
  if (!inserted) {
    iter->update(inserter);
  } else {
    this->ordered_.emplace(get_key(*iter));
  }

  return storage.status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get(const KeyView& key) noexcept
{
  ReadLockT lock{this->mutex_};

  return this->get_impl(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan(const KeyView& min_key,
                     const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  ReadLockT lock{this->mutex_};

  return this->scan_impl(min_key, items_out);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::finalized_get(const KeyView& key) noexcept
{
  BATT_CHECK(!this->context_);

  return this->get_impl(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::finalized_scan(const KeyView& min_key,
                               const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  BATT_CHECK(!this->context_);

  return this->scan_impl(min_key, items_out);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<ValueView> MemTable::get_impl(const KeyView& key) noexcept
{
  auto iter = this->hashed_.find(key);
  if (iter == this->hashed_.end()) {
    return None;
  }

  // TODO [tastolfi 2025-02-25] compact this value if necessary (see MemTable::compact for example).
  //
  return iter->value_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize MemTable::scan_impl(const KeyView& min_key,
                          const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept
{
  auto first = this->ordered_.lower_bound(min_key);
  auto ordered_end = this->ordered_.end();
  auto hashed_end = this->hashed_.end();

  usize k = 0;
  while (first != ordered_end && k != items_out.size()) {
    auto iter = this->hashed_.find(*first);
    BATT_CHECK_NE(iter, hashed_end);

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
  WriteLockT lock{this->mutex_};

  this->context_ = None;
  return std::move(this->writer_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MergeCompactor::ResultSet</*decay_to_items=*/false> MemTable::compact() noexcept
{
  BATT_CHECK(!this->context_);

  std::vector<EditView> edits_out;
  {
    const usize num_edits = this->hashed_.size();
    edits_out.reserve(num_edits);

    for (const KeyView& key : this->ordered_) {
      auto iter = this->hashed_.find(key);

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
