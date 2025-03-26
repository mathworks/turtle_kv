#pragma once

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_id.hpp>

#include <atomic>

namespace turtle_kv {

inline const KeyView THE_KEY = "user71786357865683239579";

inline std::atomic<u64>& last_known_page_with_THE_KEY()
{
  static std::atomic<u64> page_id_int{llfs::PageId{}.int_value()};
  return page_id_int;
}

}  // namespace turtle_kv
