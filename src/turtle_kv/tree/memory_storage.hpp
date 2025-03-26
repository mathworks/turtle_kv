#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <llfs/page_arena.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_size.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/async/task_scheduler.hpp>

#include <memory>
#include <string>

namespace turtle_kv {

std::shared_ptr<llfs::PageCache> make_memory_page_cache(batt::TaskScheduler& scheduler,
                                                        const TreeOptions& opts,
                                                        usize byte_capacity);

}  // namespace turtle_kv
