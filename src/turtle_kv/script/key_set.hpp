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

  KeySet(const KeySet&) = delete;
  KeySet& operator=(const KeySet&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize size() const noexcept;

  Optional<KeyView> get_key_by_index(usize index) noexcept;

  std::pair<KeyView, usize> insert_key(const KeyView& key) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct KeyEntry {
    std::atomic<const char*> data_;
    usize size_;

    //----- --- -- -  -  -   -

    KeyEntry() noexcept;

    void set(std::string_view s);

    std::string_view get();
  };

  struct Level {
    const usize size_;
    std::unique_ptr<KeyEntry[]> entries_;

    //----- --- -- -  -  -   -

    explicit Level(usize n) noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyEntry* lookup(usize index) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::CpuCacheLineIsolated<std::atomic<usize>> next_index_{0};
  std::mutex string_store_mutex_;
  batt::StableStringStore string_store_;
  std::array<std::atomic<Level*>, 26> levels_;
};

}  // namespace turtle_kv
