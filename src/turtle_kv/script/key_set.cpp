//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/key_set.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KeySet::KeySet() noexcept
{
  for (std::atomic<Level*>& p_level : this->levels_) {
    p_level.store(nullptr);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize KeySet::size() const noexcept
{
  return this->next_index_->load();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<KeyView> KeySet::get_key_by_index(usize index) noexcept
{
  // If the passed index has not yet been assigned, then there is no key to get; return None.
  //
  if (index >= this->size()) {
    return None;
  }

  // Find the entry for this index.
  //
  KeyEntry* const entry = this->lookup(index);
  BATT_CHECK_NOT_NULLPTR(entry);

  // KeyEntry::get() will block until the key data is fully initialized.
  //
  return entry->get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::pair<KeyView, usize> KeySet::insert_key(const KeyView& key) noexcept
{
  // Assign a unique index to this insertion.
  //
  const usize index = this->next_index_->fetch_add(1);

  // Retrieve the KeyEntry for the assigned index; this may create a new Level.
  //
  KeyEntry* const entry = this->lookup(index);

  // Sanity check: the entry must not currently hold a key string!
  //
  BATT_CHECK_EQ(entry->data_.load(), nullptr);

  // Lock the mutex and place a copy of `key` in the StableStringStore.
  //
  std::scoped_lock<std::mutex> lock{this->string_store_mutex_};
  entry->set(this->string_store_.store(key));

  // Success!
  //
  return std::make_pair(entry->get(), index);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeySet::KeyEntry* KeySet::lookup(usize index) noexcept
{
  // Find the level where this index resides.  Each level holds double the last; indexes first fill
  // up the first level, then the second, etc.
  //
  i32 level_i = Self::level_for_index(index);
  BATT_CHECK_LT((usize)level_i, this->levels_.size()) << BATT_INSPECT(index);

  // Retrieve the level pointer.  If this is null, we will try to create a new level to hold the
  // entry.
  //  Note: this may race against other threads trying to look up this index (or some other index)
  //  in the same Level.  We use CAS to resolve the race.
  //
  Level* p_level = this->levels_[level_i].load();
  {
    // Cache a pointer to a new Level struct if we create one.
    //
    Level* new_level = nullptr;

    // If we lose the race to install a new Level in the levels_ array, then delete it when this
    // scope exits.
    //
    auto on_scope_exit = batt::finally([&] {
      if (new_level && new_level != p_level) {
        delete new_level;
      }
    });

    // The goal is to have a non-null Level pointer which is known to be installed in the levels_
    // array.  Keep looping while we have not reached our goal.
    //
    while (p_level == nullptr) {
      // First create a new Level struct if we haven't already.
      //
      if (!new_level) {
        new_level = new Level{KeySet::size_of_level(level_i)};
      }

      // We know p_level is nullptr, so try to CAS the new level we created to fix this.
      //
      if (this->levels_[level_i].compare_exchange_weak(p_level, new_level)) {
        // This thread won the race to create this Level!  Set the level and let the loop exit.
        //
        p_level = new_level;
      }
      // else - p_level will be updated with the new value (unless this is a spurious failure)
    }
  }
  BATT_CHECK_NOT_NULLPTR(p_level);

  // Find the offset of this index relative to the start of the level.
  //
  usize offset_i = index - Self::start_of_level(level_i);
  BATT_CHECK_LT(offset_i, p_level->size_);

  // Success!
  //
  return &p_level->entries_[offset_i];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeySet::KeyEntry::KeyEntry() noexcept : data_{nullptr}, size_{0}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void KeySet::KeyEntry::set(std::string_view s)
{
  // Important!  Always update size_ before data_, because once data_ has been observed as non-null,
  // other threads will assume size_ is set as well.
  //
  this->size_ = s.size();
  this->data_.store(s.data());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string_view KeySet::KeyEntry::get()
{
  // Spin until we observe the data_ pointer to be non-null.
  //
  for (;;) {
    const char* data = this->data_.load();
    if (!data) {
      std::this_thread::yield();
      continue;
    }
    return std::string_view{data, this->size_};
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ KeySet::Level::Level(usize n) noexcept : size_{n}, entries_{new KeyEntry[n]}
{
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

static_assert(KeySet::size_of_level(0) == 4096);
static_assert(KeySet::size_of_level(1) == 4096);
static_assert(KeySet::size_of_level(2) == 8192);
static_assert(KeySet::size_of_level(3) == 16384);
static_assert(KeySet::size_of_level(4) == 32768);

static_assert(KeySet::start_of_level(1) - KeySet::start_of_level(0) == KeySet::size_of_level(0));
static_assert(KeySet::start_of_level(2) - KeySet::start_of_level(1) == KeySet::size_of_level(1));
static_assert(KeySet::start_of_level(3) - KeySet::start_of_level(2) == KeySet::size_of_level(2));
static_assert(KeySet::start_of_level(4) - KeySet::start_of_level(3) == KeySet::size_of_level(3));
static_assert(KeySet::start_of_level(5) - KeySet::start_of_level(4) == KeySet::size_of_level(4));
static_assert(KeySet::start_of_level(6) - KeySet::start_of_level(5) == KeySet::size_of_level(5));

static_assert(KeySet::level_for_index(0) == 0);
static_assert(KeySet::level_for_index(1) == 0);
static_assert(KeySet::level_for_index(4095) == 0);
static_assert(KeySet::level_for_index(4096) == 1);

static_assert(KeySet::level_for_index(KeySet::start_of_level(0)) == 0);
static_assert(KeySet::level_for_index(KeySet::start_of_level(1)) == 1);
static_assert(KeySet::level_for_index(KeySet::start_of_level(2)) == 2);
static_assert(KeySet::level_for_index(KeySet::start_of_level(3)) == 3);
static_assert(KeySet::level_for_index(KeySet::start_of_level(4)) == 4);
static_assert(KeySet::level_for_index(KeySet::start_of_level(5)) == 5);
static_assert(KeySet::level_for_index(KeySet::start_of_level(6)) == 6);

static_assert(KeySet::level_for_index(KeySet::start_of_level(0) + KeySet::size_of_level(0) - 1) ==
              0);
static_assert(KeySet::level_for_index(KeySet::start_of_level(1) + KeySet::size_of_level(1) - 1) ==
              1);
static_assert(KeySet::level_for_index(KeySet::start_of_level(2) + KeySet::size_of_level(2) - 1) ==
              2);
static_assert(KeySet::level_for_index(KeySet::start_of_level(3) + KeySet::size_of_level(3) - 1) ==
              3);
static_assert(KeySet::level_for_index(KeySet::start_of_level(4) + KeySet::size_of_level(4) - 1) ==
              4);

}  // namespace turtle_kv
