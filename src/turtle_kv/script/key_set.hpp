//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_KEY_SET_HPP

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <batteries/async/types.hpp>
#include <batteries/cpu_align.hpp>
#include <batteries/math.hpp>
#include <batteries/stable_string_store.hpp>

#include <atomic>
#include <mutex>

namespace turtle_kv {

class KeySet
{
 public:
  using Self = KeySet;

  static constexpr usize size_of_level(i32 level_i) noexcept
  {
    return (level_i == 0) ? usize{4096} : (usize{1} << (level_i + 11));
  }

  static constexpr usize start_of_level(i32 level_i) noexcept
  {
    return (level_i == 0) ? usize{0} : (usize{1} << (level_i + 11));
  }

  static constexpr i32 level_for_index(usize index) noexcept
  {
    return (index == 0) ? 0 : (std::max<i32>(11, batt::log2_floor(index)) - 11);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit KeySet() noexcept;

  ~KeySet() noexcept;

  KeySet(const KeySet&) = delete;
  KeySet& operator=(const KeySet&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The number of keys that have been created in this set.
   */
  usize size() const noexcept;

  /** \brief Creates a new key, assigning it the first available index.
   */
  std::pair<KeyView, usize> create_key(const KeyView& key, bool inserted = false) noexcept;

  /** \brief Sets the inserted status of the given key. Panics if the key has not been created.
   */
  void set_key_inserted(usize index, bool b = true) noexcept;

  /** \brief Gets the key with the given index; if this key has not yet been created, returns None.
   */
  Optional<KeyView> get_key(usize index) noexcept;

  /** \brief Returns true iff a key with the given index has been created and is also inserted.
   */
  bool is_key_inserted(usize index) noexcept;

  /** \brief Waits for the given key to be inserted; panics if the key has not been created.
   */
  KeyView wait_for_key_inserted(usize index) noexcept;

  /** \brief Returns the highest index for which a key has been inserted into this set.
   */
  usize inserted_upper_bound() noexcept;

  /** \brief Updates the cached value for inserted upper bound.
   */
  void update_inserted_upper_bound() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief A single key in the set.
   */
  struct KeyEntry {
    /** \brief Set to non-null to indicate the key has been created; pointer to non-null-terminated
     * key data.
     */
    std::atomic<const char*> data_;

    /** \brief Set to the size of the key, in bytes; *must* be set before `this->data_` (code
     * assumes once data is set, size is valid).
     */
    usize size_;

    /** \brief Set to true when the key is known to be inserted.
     */
    std::atomic<bool> inserted_;

    //----- --- -- -  -  -   -

    KeyEntry() noexcept;

    bool is_created() const noexcept
    {
      return this->data_.load() != nullptr;
    }

    bool is_inserted() const noexcept
    {
      return this->inserted_.load();
    }

    void set(KeyView s) noexcept;

    Optional<KeyView> get() noexcept;

    KeyView await_created() const noexcept;

    KeyView await_inserted() const noexcept;

    void set_inserted(bool b = true) noexcept
    {
      this->inserted_.store(b);
      this->inserted_.notify_all();
    }
  };

  struct Level {
    const usize size_;
    std::unique_ptr<KeyEntry[]> entries_;

    //----- --- -- -  -  -   -

    explicit Level(usize n) noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <bool kCreateLevel>
  KeyEntry* lookup(usize index, std::integral_constant<bool, kCreateLevel>) noexcept;

  void invalidate_inserted_upper_bound() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr u32 kIsValid = 1;
  static constexpr u32 kIsLocked = 2;

  std::array<std::atomic<Level*>, 26> levels_;

  batt::CpuCacheLineIsolated<std::atomic<usize>> next_index_{0};
  batt::CpuCacheLineIsolated<std::atomic<usize>> inserted_upper_bound_{0};
  batt::CpuCacheLineIsolated<std::atomic<u32>> inserted_upper_bound_valid_{kIsValid};

  std::mutex string_store_mutex_;
  batt::StableStringStore string_store_;
};

}  // namespace turtle_kv
