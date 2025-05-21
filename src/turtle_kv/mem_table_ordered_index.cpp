#include <turtle_kv/mem_table_ordered_index.hpp>
//

#include <batteries/async/task.hpp>
#include <batteries/checked_cast.hpp>

#include <xxhash.h>

namespace turtle_kv {

namespace {

struct Hasher {
  std::aligned_storage_t<4096, 64> state_buffer_;

  explicit Hasher(u64 hash_seed) noexcept
  {
    XXH3_64bits_reset_withSeed(this->state(), hash_seed);
  }

  XXH3_state_t* state()
  {
    return (XXH3_state_t*)&this->state_buffer_;
  }

  XXH3_state_t* state() const
  {
    return (XXH3_state_t*)&this->state_buffer_;
  }

  Hasher& update(char c)
  {
    XXH3_64bits_update(this->state(), &c, 1);
    return *this;
  }

  Hasher& update(const char* cs, usize len)
  {
    XXH3_64bits_update(this->state(), cs, len);
    return *this;
  }

  u64 get() const
  {
    return XXH3_64bits_digest(this->state());
  }
};

void clamp_min(std::atomic<u8>& var, u8 lower_bound)
{
  u8 observed = var.load();
  for (;;) {
    if (observed >= lower_bound) {
      break;
    }
    if (var.compare_exchange_weak(observed, lower_bound)) {
      break;
    }
  }
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ MemTableOrderedIndex::MemTableOrderedIndex(usize bucket_count) noexcept
    : bucket_storage_{new BucketStorage[bucket_count]}
    , buckets_{as_slice(reinterpret_cast<Bucket*>(this->bucket_storage_.get()), bucket_count)}
    , hash_to_bucket_index_{bucket_count}
{
  std::memset(this->bucket_storage_.get(), 0, bucket_count * sizeof(BucketStorage));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MemTableOrderedIndex::~MemTableOrderedIndex() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void MemTableOrderedIndex::insert(std::string_view key)
{
  if (key.empty()) {
    return;
  }

  usize prefix_length = 0;
  Hasher hasher{Self::kHashSeed};
  u64 hash_val = hasher.get();
  Bucket* bucket = &this->root_;
  u64 observed_state = bucket->state.load();

  for (;;) {
    if ((observed_state & Self::kStateModeMask) == Self::kStateEmpty) {
      observed_state = this->lock_bucket(bucket);

      usize n_copied = 0;

      //----- --- -- -  -  -   -
      {
        auto unlock_bucket = batt::finally([&] {
          observed_state = this->unlock_bucket(bucket);
        });

        // Re-check the state to make sure no one else won the race to initialize this bucket.
        //
        if ((observed_state & Self::kStateModeMask) != Self::kStateEmpty) {
          continue;
        }

        KeyFragment& fragment = bucket->as_fragment();

        // We have locked an empty bucket; copy as much of `key` into it as we can.
        //
        fragment.length = BATT_CHECKED_CAST(u8, std::min(key.size(), fragment.bytes.size()));
        std::memcpy(fragment.bytes.data(), key.data(), fragment.length);
        n_copied = fragment.length;

        // Set the bucket's state to fragment mode.
        //
        bucket->state.fetch_or(Self::kStateFragmentMode);
      }
      // (bucket unlocked)
      //----- --- -- -  -  -   -

      // Advance `key` and update the hash_val.
      //
      {
        const char* consumed_data = key.data();
        key = key.substr(n_copied);
        if (key.empty()) {
          return;
        }
        hash_val = hasher.update(consumed_data, n_copied).get();
        prefix_length += n_copied;
      }

      // Jump to the bucket for the new current prefix.
      //
      const usize bucket_index = this->hash_to_bucket_index_(hash_val);
      BATT_CHECK_LT(bucket_index, this->buckets_.size());
      bucket = &this->buckets_[bucket_index];
      observed_state = bucket->state.load();

      // Update max_prefix_length.
      //
      clamp_min(bucket->max_prefix_length[hash_val & 0xf], BATT_CHECKED_CAST(u8, prefix_length));
      continue;
    }

    if ((observed_state & Self::kStateModeMask) == Self::kStateFragmentMode) {
      observed_state = this->lock_bucket(bucket);
      auto on_scope_exit = batt::finally([&] {
        observed_state = this->unlock_bucket(bucket);
      });

      // Re-check the state to make it didn't change.
      //
      if ((observed_state & Self::kStateModeMask) != Self::kStateFragmentMode) {
        continue;
      }

      KeyFragment& fragment = bucket->as_fragment();

      // Update max_prefix_length.
      //
      clamp_min(bucket->max_prefix_length[hash_val & 0xf], BATT_CHECKED_CAST(u8, prefix_length));

      // Find the longest common prefix between the current fragment and the key.
      //
      const auto [key_iter, fragment_iter] =
          std::mismatch(key.begin(), key.end(), fragment.begin(), fragment.end());

      // Case 1: no common prefix; change mode to branched.
      //
      if (key_iter == key.begin()) {
      }

      // Case 2: fragment is a prefix of key; consume fragment from key and continue.
      //
      if (fragment_iter == fragment.end()) {
      }

      // Case 3: branch occurs partway through the fragment.
      //
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 MemTableOrderedIndex::lock_bucket(Bucket* bucket)
{
  for (;;) {
    const u64 prior_state = bucket->state.fetch_or(Self::kStateLocked);
    if ((prior_state & Self::kStateLocked) == 0) {
      bucket->state.fetch_add(Self::kStateSeqIncrement);
      return prior_state | Self::kStateLocked;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 MemTableOrderedIndex::unlock_bucket(Bucket* bucket)
{
  bucket->state.fetch_add(Self::kStateSeqIncrement);
  const u64 prior_state = bucket->state.fetch_and(~Self::kStateLocked);
  return prior_state & ~Self::kStateLocked;
}

}  // namespace turtle_kv
