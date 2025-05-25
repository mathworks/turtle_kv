#pragma once

#include <turtle_kv/mem_table_entry.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>

#include <batteries/assert.hpp>
#include <batteries/math.hpp>

#include <atomic>
#include <memory>
#include <vector>

namespace turtle_kv {

class ConcurrentHashIndex
{
 public:
  struct Bucket {
    std::atomic<u64> hash_val;
    MemTableEntry entry;

    /** \brief Sequence (Optimistic) Read/Write Spin Lock.
     *
     * Lowest bit (0x1) is the current 'locked' status (1 == locked, 0 == not locked)
     * Remaining bits hold the current sequence number.  When this number is odd (meaning `state /
     * 2` is odd), there is a thread currently operating on this bucket.
     */
    std::atomic<u32> state;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief To lock the bucket for write, we must first acquire the spin lock by successfully
     * setting the lowest bit, changing it from 0 to 1.  Once a thread knows it has acquired the
     * lock in this way, it must increment the sequence counter by adding `2` to `state_`.
     */
    void lock()
    {
      for (;;) {
        if ((this->state.fetch_or(1) & 1) == 1) {
          continue;
        }
        break;
      }
      this->state.fetch_add(2);
    }

    /** \brief To unlock the bucket, we must accomplish both of:
     *
     * - Setting the lock bit to 0
     * - Incrementing the sequence counter to make it even
     *
     * When a thread current holds the lock, it can accomplish both steps atomically by doing a
     * fetch add of 1 on `state_`.
     */
    void unlock()
    {
      // Adding 1 when we have the lock will always unset the lsb (the lock bit) and increment the
      // sequence counter in a single instruction.
      //
      this->state.fetch_add(1);
    }

    /** \brief Returns true iff this bucket is empty or it contains the same hash value as `key`.
     * NOTE: multiple buckets in the same chain may hold the same hash value.
     */
    [[nodiscard]] bool is_writable(u64 key_hash_val) const
    {
      const u64 observed_hash_val = this->hash_val.load();
      return observed_hash_val == 0 || observed_hash_val == key_hash_val;
    }

    /** \brief Attempts to insert or update the entry held by this bucket.
     *
     * \return true iff the write succeeded; if false, we can probe ahead to the next bucket and try
     * again.
     */
    template <typename StorageT>
    [[nodiscard]] bool write(MemTableEntryInserter<StorageT>& inserter,
                             std::atomic<usize>& size_out)
    {
      // IMPORTANT!  We must hold the lock in order to modify this bucket.
      //
      this->lock();
      auto on_scope_exit = batt::finally([&] {
        this->unlock();
      });
      //----- --- -- -  -  -   -

      // Now that this thread holds an exclusive (write) lock on the bucket, re-test the hash value
      // to make sure we can proceed.
      //
      const u64 observed_hash_val = this->hash_val.load();

      // If the bucket is empty, we do an assignment on entry.
      //
      if (observed_hash_val == 0) {
        this->hash_val.store(inserter.hash_val);
        this->entry.assign(inserter);
        size_out.fetch_add(1);
        return true;
      }

      const DefaultStrEq key_eq;

      // If the bucket currently holds the given key, we just update the value.
      //
      if (observed_hash_val == inserter.hash_val && key_eq(inserter, entry)) {
        this->entry.update(inserter);
        return true;
      }

      // Key does not match!  Indicate failure.
      //
      return false;
    }

    /** \brief Attempts to read the given key.  NOTE: readers may be starved under heavy contention!
     * (We believe this is unlikely to occur since each bucket gets its own lock).
     */
    MemTableEntry read() const
    {
      for (;;) {
        const u32 before_state = this->state.load();

        // If either of the low two bits is non-zero, this means the bucket is currently being
        // modified; we must retry.
        //
        if ((before_state & u32{0b011}) != 0) {
          continue;
        }
        //----- --- -- -  -  -   -
        MemTableEntry view = this->entry;
        //----- --- -- -  -  -   -
        const u32 after_state = this->state.load();

        // If the state is unchanged, then we know the value read into `view` wasn't torn because
        // `this->entry` wasn't modified in the critical section above.
        //
        if (before_state == after_state) {
          return view;
        }
      }
      BATT_UNREACHABLE();
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of buckets in the initial (primary) tier of the hash table.
   */
  const usize bucket_count_;

  /** \brief Functor that selects the correct bucket from a given hash value; primary tier only.
   */
  const batt::fixed_point::LinearProjection<u64, usize> bucket_from_hash_val_{this->bucket_count_};

  /** \brief The hash table can be grown by adding more tiers; these are called overflow buckets.
   * This is the number of buckets in an overflow tier.
   */
  const usize overflow_bucket_count_;

  /** \brief Functor that selects the correct bucket from a given hash value; overflow tiers only.
   */
  const batt::fixed_point::LinearProjection<u64, usize> overflow_bucket_from_hash_val_{
      this->overflow_bucket_count_};

  /** \brief The tiers of the hash table; underlying bucket storage arrays.
   */
  std::vector<std::unique_ptr<Bucket[]>> bucket_storage_;

  /** \brief Buckets are accessed through slices. this->bucket_[0] is the primary tier,
   * this->bucket_[1] (if present) is the first overflow tier, etc.
   */
  std::vector<Slice<Bucket>> buckets_;

  /** \brief The number of unique keys in this index.
   */
  std::atomic<usize> size_{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a new hash index with the given number of buckets in the primary tier.
   */
  explicit ConcurrentHashIndex(usize n_buckets) noexcept
      : bucket_count_{n_buckets}
      , overflow_bucket_count_{std::max<usize>(4096, this->bucket_count_ / 16)}
  {
    this->bucket_storage_.emplace_back(new Bucket[n_buckets]);
    this->buckets_.emplace_back(batt::as_slice(this->bucket_storage_.back().get(), n_buckets));

    // Yes, this is sketchy, but MemTableEntry *is* valid/empty when we blast all its bits to zero.
    //
    std::memset((void*)this->bucket_storage_.back().get(), 0, sizeof(Bucket) * n_buckets);
  }

  usize size() const
  {
    return this->size_.load();
  }

  template <typename StorageT>
  void insert(MemTableEntryInserter<StorageT>& inserter)
  {
    this->probe(inserter.hash_val, [&inserter, this](Bucket& bucket) -> bool {
      if (bucket.is_writable(inserter.hash_val)) {
        if (bucket.write(inserter, this->size_)) {
          return true;
        }
      }  // else - go to the next bucket and retry.
      return false;
    });
  }

  Optional<ValueView> find_key(const KeyView& key)
  {
    Optional<ValueView> result;

    const u64 key_hash_val = get_key_hash_val(key);

    this->probe(key_hash_val, [&result, &key, key_hash_val](Bucket& bucket) -> bool {
      const DefaultStrEq str_eq;
      const u64 observed_hash_val = bucket.hash_val.load();

      if (observed_hash_val == 0) {
        result = None;
        return true;
      }

      if (observed_hash_val == key_hash_val) {
        MemTableEntry entry = bucket.read();
        if (str_eq(key, entry)) {
          result = entry.value_;
          return true;
        }
      }

      return false;
    });

    return result;
  }

  template <typename EntryFn>
  void for_each(EntryFn&& fn)
  {
    for (Bucket& bucket : this->buckets_.back()) {
      if (bucket.hash_val != 0) {
        fn(bucket.entry);
      }
    }
  }

  template <typename BucketFn /* bool (Bucket&) */>
  void probe(u64 key_hash_val, BucketFn&& bucket_fn)
  {
    usize depth = 0;
    usize bucket_i = this->bucket_from_hash_val_(key_hash_val);
    for (;;) {
      Bucket& bucket = this->buckets_.back()[bucket_i];

      if (bucket_fn(bucket)) {
        break;
      }

      bucket_i += 1;
      if (bucket_i == this->buckets_.back().size()) {
        bucket_i = 0;
      }

      ++depth;
      BATT_CHECK_LT(depth, 500);
    }
  }
};

}  // namespace turtle_kv
