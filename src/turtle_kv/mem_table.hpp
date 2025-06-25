#pragma once

#include <turtle_kv/change_log_writer.hpp>
#include <turtle_kv/concurrent_hash_index.hpp>
#include <turtle_kv/mem_table_entry.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/art.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <absl/synchronization/mutex.h>

#include <batteries/async/worker_pool.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::atomic<bool> is_finalized_;

  RuntimeOptions runtime_options_ = RuntimeOptions::with_default_values();

  ConcurrentHashIndex hash_index_;

  ART ordered_index_;

  const i64 max_byte_size_;

  std::atomic<i64> current_byte_size_;

  u64 self_id_;

  u64 next_block_owner_id_;

  std::atomic<u32> version_;

  absl::Mutex block_list_mutex_;

  batt::SmallVec<ChangeLogBlock*, MemTable::kBlockListPreAllocSize> blocks_;
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

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

class MemTable::Scanner
{
 public:
  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Scanner(MemTable& mem_table, const KeyView& lower_bound_key) noexcept
      : mem_table_{mem_table}
      , ordered_index_scanner_{mem_table.ordered_index_, lower_bound_key, mem_table.is_finalized()}
      , lower_bound_key_{lower_bound_key}
  {
    this->set_next_item();
  }

  Scanner(const Scanner&) = delete;
  Scanner& operator=(const Scanner&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const KeyView& lower_bound_key() const
  {
    return this->lower_bound_key_;
  }

  const Optional<Item>& peek() const
  {
    return this->next_item_;
  }

  Optional<Item> next()
  {
    Optional<Item> item;
    std::swap(item, this->next_item_);
    this->ordered_index_scanner_.advance();
    this->set_next_item();
    return item;
  }

  bool is_done() const
  {
    return this->ordered_index_scanner_.is_done();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  friend inline const KeyView& get_key(const MemTable::Scanner& scanner)
  {
    return scanner.ordered_index_scanner_.get_key();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void set_next_item()
  {
    if (this->ordered_index_scanner_.is_done()) {
      return;
    }

    const std::string_view& key = this->ordered_index_scanner_.get_key();

    if (this->ordered_index_scanner_.is_synchronized()) {
      MemTableEntry entry;
      const bool found = this->mem_table_.hash_index_.find_key(key, entry);
      BATT_CHECK(found);

      this->next_item_.emplace(entry.key_, entry.value_);

    } else {
      const MemTableEntry* entry = this->mem_table_.hash_index_.unsynchronized_find_key(key);
      BATT_CHECK_NOT_NULLPTR(entry);

      this->next_item_.emplace(entry->key_, entry->value_);
    }

    if (this->next_item_) {
      BATT_CHECK_LE(this->lower_bound_key_, get_key(*this->next_item_));
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MemTable& mem_table_;

  ART::Scanner<ART::Synchronized::kDynamic> ordered_index_scanner_;

  KeyView lower_bound_key_;

  Optional<Item> next_item_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class DeltasScanner
{
 public:
  struct DeltaScannerHeapOrder {
    bool operator()(MemTable::Scanner* left, MemTable::Scanner* right) const
    {
      batt::Order order = batt::compare(get_key(*left), get_key(*right));
      return (order == batt::Order::Greater)    //
             || ((order == batt::Order::Equal)  //
                 && left < right);
    }
  };

  using Item = EditView;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // `delta_scanners` MUST be in oldest-to-newest order, just like the deltas stack.
  //
  explicit DeltasScanner(const Slice<MemTable::Scanner>& delta_scanners,
                         const KeyView& lower_bound_key) noexcept
      : heap_{}
      , lower_bound_key_{lower_bound_key}
  {
    this->heap_.resize(delta_scanners.size());

    std::transform(delta_scanners.begin(),
                   delta_scanners.end(),
                   this->heap_.begin(),
                   [](MemTable::Scanner& scanner) -> MemTable::Scanner* {
                     return &scanner;
                   });

    std::make_heap(this->heap_.begin(), this->heap_.end(), DeltaScannerHeapOrder{});
    this->set_next_item();
  }

  const Optional<Item>& peek() const
  {
    return this->next_item_;
  }

  Optional<Item> next()
  {
    Optional<Item> item;
    std::swap(item, this->next_item_);
    this->set_next_item();
    return item;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void pop(MemTable::Scanner* top)
  {
    BATT_CHECK_EQ(top, this->heap_.front());
    std::pop_heap(this->heap_.begin(), this->heap_.end(), DeltaScannerHeapOrder{});
  }

  void push_unless_empty(MemTable::Scanner* top)
  {
    if (top->is_done()) {
      this->heap_.pop_back();
    } else {
      std::push_heap(this->heap_.begin(), this->heap_.end(), DeltaScannerHeapOrder{});
    }
  }

  void set_next_item()
  {
    auto on_scope_exit = batt::finally([&] {
      if (this->next_item_) {
        BATT_CHECK_LE(this->lower_bound_key_, get_key(*this->next_item_));
      }
    });

    for (;;) {
      if (this->heap_.empty()) {
        return;
      }

      MemTable::Scanner* const top = this->heap_.front();

      if (!this->next_item_) {
        this->pop(top);
        this->next_item_ = top->next();
        this->push_unless_empty(top);

      } else if (get_key(*this->next_item_) == get_key(*top)) {
        this->pop(top);
        Optional<EditView> older_item = top->next();
        if (this->next_item_->needs_combine()) {
          *this->next_item_ = combine(*this->next_item_, *older_item);
        }
        this->push_unless_empty(top);

      } else {
        return;
      }
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  SmallVec<MemTable::Scanner*, 32> heap_;

  Optional<EditView> next_item_;

  KeyView lower_bound_key_;
};

}  // namespace turtle_kv
