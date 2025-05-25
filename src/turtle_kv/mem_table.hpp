#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/concurrent_hash_index.hpp>
#include <turtle_kv/mem_table_entry.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/locks.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>

#include <string_view>
#include <vector>

namespace turtle_kv {

#define TURTLE_KV_NEW_MEM_TABLE 1

namespace {
BATT_STATIC_ASSERT_TYPE_EQ(KeyView, std::string_view);
}

class MemTable : public batt::RefCounted<MemTable>
{
 public:
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

  explicit MemTable(usize max_byte_size, Optional<u64> id = None) noexcept;

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

  MergeCompactor::ResultSet</*decay_to_items=*/false> compact() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> finalized_get(const KeyView& key) noexcept;

  usize finalized_scan(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct StorageImpl {
    MemTable& mem_table;
    ChangeLogWriter::Context& context;
    Status status;

    template <typename SerializeFn = void(u32 /*locator*/, const MutableBuffer&)>
    void store_data(usize n_bytes, SerializeFn&& serialize_fn) noexcept;
  };

#if !TURTLE_KV_NEW_MEM_TABLE
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  using MutexStorage = std::aligned_storage_t<sizeof(batt::CpuCacheLineIsolated<absl::Mutex>),
                                              alignof(batt::CpuCacheLineIsolated<absl::Mutex>)>;
  //+++++++++++-+-+--+----- --- -- -  -  -   -
#endif  // TURTLE_KV_NEW_MEM_TABLE

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<ValueView> get_impl(const MemTableQuery& query, u64 shard_i) noexcept;

  usize scan_keys_impl(const KeyView& min_key,
                       const Slice<std::pair<KeyView, ValueView>>& items_out) noexcept;

  u64 get_next_block_owner_id() const noexcept
  {
    return this->self_id_ | (this->blocks_.size() & 0xffff);
  }

  ConstBuffer fetch_slot(u32 locator) const noexcept;

#if !TURTLE_KV_NEW_MEM_TABLE
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  u64 shard_for_hash_val(u64 hash_val) const
  {
    return hash_val & this->shard_mask_;
  }

  absl::Mutex* hashed_mutex_for_shard(u64 shard_i)
  {
    return std::addressof(*this->hash_mutex_[shard_i]);
  }

  absl::Mutex* ordered_mutex()
  {
    return std::addressof(*this->ordered_mutex_);
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -
#endif  // TURTLE_KV_NEW_MEM_TABLE

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool is_finalized_;

#if TURTLE_KV_NEW_MEM_TABLE

  ConcurrentHashIndex hash_index_;

#else
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  const i32 n_shards_log2_;

  const usize n_shards_;

  const u64 shard_mask_;

  batt::SmallVec<MutexStorage, MemTable::kMaxShards> hash_mutex_storage_;

  Slice<batt::CpuCacheLineIsolated<absl::Mutex>> hash_mutex_;

  batt::CpuCacheLineIsolated<absl::Mutex> ordered_mutex_;
  //+++++++++++-+-+--+----- --- -- -  -  -   -
#endif  // TURTLE_KV_NEW_MEM_TABLE

  const usize max_byte_size_;

  std::atomic<usize> current_byte_size_;

  u64 self_id_;

  u64 next_block_owner_id_;

  std::atomic<u32> version_;

  absl::Mutex block_list_mutex_;

  batt::SmallVec<ChangeLogBlock*, MemTable::kBlockListPreAllocSize> blocks_;

#if TURTLE_KV_NEW_MEM_TABLE
#else
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  batt::SmallVec<absl::flat_hash_set<MemTableEntry, DefaultStrHash, DefaultStrEq>,
                 MemTable::kMaxShards>
      hashed_;

  absl::btree_set<KeyView> ordered_;
  //+++++++++++-+-+--+----- --- -- -  -  -   -
#endif  // TURTLE_KV_NEW_MEM_TABLE
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
