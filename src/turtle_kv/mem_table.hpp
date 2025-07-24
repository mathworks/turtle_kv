#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/concurrent_hash_index.hpp>
#include <turtle_kv/kv_store_metrics.hpp>
#include <turtle_kv/mem_table_entry.hpp>
#include <turtle_kv/scan_metrics.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/art.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>

#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/utility.hpp>

#include <algorithm>
#include <string_view>
#include <vector>

namespace turtle_kv {

namespace {
BATT_STATIC_ASSERT_TYPE_EQ(KeyView, std::string_view);
}

class MemTable : public batt::RefCounted<MemTable>
{
 public:
  using Self = MemTable;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct RuntimeOptions {
    /** \brief If true, then only count the latest version of each key update towards the size limit
     * of the MemTable; otherwise, count all edits (including key overwrites).
     */
    bool limit_size_by_latest_updates_only;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static RuntimeOptions with_default_values() noexcept;
  };

  class Scanner;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The base-2 log of the maximum number of hash index lock shards.
   */
  static constexpr i32 kMaxShardsLog2 = 8;

  /** \brief The maximum number of hash index lock shards.  The actual number of shards is the
   * minimum of this value and the number of available hardware threads at runtime.
   */
  static constexpr usize kMaxShards = usize{1} << MemTable::kMaxShardsLog2;

  /** \brief The number of change log block slots to pre-allocate in this object.
   */
  static constexpr usize kBlockListPreAllocSize = 4096;

  //----- --- -- -  -  -   -

  static constexpr u32 kCompactionState_Todo = 0;
  static constexpr u32 kCompactionState_InProgress = 1;
  static constexpr u32 kCompactionState_Complete = 3;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u64 first_id()
  {
    return 0x10000;
  }

  static u64 next_id()
  {
    static std::atomic<u64> next{MemTable::first_id()};
    return next.fetch_add(0x10000);
  }

  static u64 batch_id_from(u64 id)
  {
    return id & ~u64{0xffff};
  }

  static u64 block_id_from(u64 id)
  {
    return id & u64{0xffff};
  }

  static u64 next_id_for(u64 id)
  {
    return id + 0x10000;
  }

  static u64 prev_id_for(u64 id)
  {
    return id - 0x10000;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemTable(llfs::PageCache& page_cache,
                    KVStoreMetrics& metrics,
                    usize max_byte_size,
                    Optional<u64> id = None) noexcept;

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  ~MemTable() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  u64 id() const noexcept
  {
    return this->self_id_;
  }

  Status put(ChangeLogWriter::Context& context,
             const KeyView& key,
             const ValueView& value) noexcept;

  Optional<ValueView> get(const KeyView& key) noexcept;

  usize scan(const KeyView& min_key,
             const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  [[nodiscard]] bool finalize() noexcept;

  bool is_finalized() const
  {
    return this->is_finalized_.load();
  }

  MergeCompactor::ResultSet</*decay_to_items=*/false> compact() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  usize finalized_scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool has_hash_index() const noexcept
  {
    return bool{this->hash_index_};
  }

  ConcurrentHashIndex& hash_index()
  {
    BATT_CHECK(this->hash_index_);
    return *this->hash_index_;
  }

  bool has_ordered_index() const noexcept
  {
    return bool{this->ordered_index_};
  }

  ART<void>& ordered_index()
  {
    BATT_CHECK(this->ordered_index_);
    return *this->ordered_index_;
  }

  bool has_art_index() const noexcept
  {
    return bool{this->art_index_};
  }

  ART<MemTableValueEntry>& art_index()
  {
    BATT_CHECK(this->art_index_);
    return *this->art_index_;
  }

  /** \brief Returns the sorted, compacted edits of this MemTable as a single Slice, if compact()
   * has completed; None otherwise.  This function does not block.
   */
  Optional<Slice<const EditView>> poll_compacted_edits() const;

  /** \brief Waits for compacted edits to become available, then returns them.  Does not trigger
   * compaction; only waits for it to be completed.
   */
  Slice<const EditView> await_compacted_edits() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct StorageImpl {
    MemTable& mem_table;
    ChangeLogWriter::Context& context;
    Status status;

    template <typename SerializeFn = void(u32 /*locator*/, const MutableBuffer&)>
    void store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> get_impl(const MemTableQuery& query, u64 shard_i) noexcept;

  usize scan_keys_impl(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  u64 get_next_block_owner_id() const noexcept
  {
    return this->self_id_ | (this->blocks_.size() & 0xffff);
  }

  ConstBuffer fetch_slot(u32 locator) const noexcept;

  Slice<const EditView> compacted_edits_slice_impl() const;

  void reserve_cache_space(usize byte_count)
  {
    this->cache_allocs_.emplace_back(this->page_cache_.allocate_external(byte_count));
  }

  std::vector<EditView> compact_hash_index();

  std::vector<EditView> compact_art_index();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageCache& page_cache_;

  KVStoreMetrics& metrics_;

  std::atomic<bool> is_finalized_;

  RuntimeOptions runtime_options_ = RuntimeOptions::with_default_values();

  //----- --- -- -  -  -   -
  //
  Optional<ConcurrentHashIndex> hash_index_;
  Optional<ART<void>> ordered_index_;
  //
  //----- --- -- -  -  -   -

  Optional<ART<MemTableValueEntry>> art_index_;

  const i64 max_byte_size_;

  std::atomic<i64> current_byte_size_;

  u64 self_id_;

  u64 next_block_owner_id_;

  std::atomic<u32> version_;

  absl::Mutex block_list_mutex_;

  batt::SmallVec<ChangeLogBlock*, MemTable::kBlockListPreAllocSize> blocks_;

  std::atomic<u32> compaction_state_{0};

  MergeCompactor::ResultSet</*decay_to_items=*/false> compacted_edits_;

  batt::SmallVec<llfs::PageCache::ExternalAllocation, 16> cache_allocs_;
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename SerializeFn>
void MemTable::StorageImpl::store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept
{
  this->status = batt::to_status(this->context.append_slot(
      this->mem_table.next_block_owner_id_,
      n_bytes,
      [&](ChangeLogWriter::BlockBuffer* buffer, const MutableBuffer& dst) {
        MemTable& mem_table = this->mem_table;

        BATT_CHECK_EQ(MemTable::batch_id_from(buffer->owner_id()), mem_table.self_id_);

        if (buffer->ref_count() == 1) {
          buffer->add_ref(1);
          absl::MutexLock lock{&this->mem_table.block_list_mutex_};
          mem_table.blocks_.emplace_back(buffer);
          mem_table.next_block_owner_id_ = mem_table.get_next_block_owner_id();
        }

        const u32 block_id = MemTable::block_id_from(buffer->owner_id());
        const u32 slot_index = buffer->slot_count();
        const u32 slot_locator = (block_id << 16) | (slot_index & 0xffff);

        serialize_fn(slot_locator, dst);
      }));
}

}  // namespace turtle_kv
