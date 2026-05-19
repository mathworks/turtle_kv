//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ALLOCATION_TRACKER_IMPL_HPP

#include <turtle_kv/mem_table/mem_table_allocation_tracker.hpp>

#include <turtle_kv/on_page_cache_overcommit.hpp>

#include <llfs/page_cache.hpp>

namespace turtle_kv {

class MemTablePageCacheAllocationTracker
{
 public:
  using ExternalAllocation = llfs::PageCache::ExternalAllocation;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MemTablePageCacheAllocationTracker(llfs::PageCache& page_cache,
                                              OvercommitMetrics& metrics) noexcept
      : page_cache_{page_cache}
      , metrics_{metrics}
  {
  }

  MemTablePageCacheAllocationTracker(const MemTablePageCacheAllocationTracker&) = delete;
  MemTablePageCacheAllocationTracker& operator=(const MemTablePageCacheAllocationTracker&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ExternalAllocation allocate_external(usize byte_size, llfs::PageCacheOvercommit& overcommit)
  {
    return this->page_cache_.allocate_external(byte_size, overcommit);
  }

  void on_overcommit(const std::function<void(std::ostream& out)>& context_fn)
  {
    on_page_cache_overcommit(context_fn, this->page_cache_, this->metrics_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  llfs::PageCache& page_cache_;
  OvercommitMetrics& metrics_;
};

static_assert(MemTableAllocationTracker<MemTablePageCacheAllocationTracker>);

}  // namespace turtle_kv
