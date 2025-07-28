#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_cache_options.hpp>

namespace turtle_kv {

llfs::PageCacheOptions page_cache_options_from(const TreeOptions& tree_options,
                                               usize cache_size_bytes);

}
