//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_MOCK_MEM_TABLE_ALLOCATION_TRACKER_HPP

#include <turtle_kv/mem_table/mem_table_allocation_tracker.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_cache_overcommit.hpp>

#include <gmock/gmock.h>

#include <functional>
#include <ostream>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct MockMemTableAllocationTracker {
  using InfoDumperFn = std::function<void(std::ostream&)>;

  struct ExternalAllocation {
    usize byte_size_{0};
    MockMemTableAllocationTracker* parent_mock_;

    void subsume(ExternalAllocation&& other);

    StatusOr<ExternalAllocation> split(usize byte_count);

    usize size() const;
  };

  MOCK_METHOD(ExternalAllocation,
              allocate_external,
              (usize byte_size, llfs::PageCacheOvercommit& overcommit),
              ());

  MOCK_METHOD(void, allocation_subsumed, (ExternalAllocation * to, ExternalAllocation* from), ());

  MOCK_METHOD(StatusOr<ExternalAllocation>,
              allocation_split,
              (ExternalAllocation * obj, usize byte_size),
              ());

  MOCK_METHOD(usize, allocation_size, (const ExternalAllocation* obj), ());

  MOCK_METHOD(void, on_overcommit, (const InfoDumperFn&), ());
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void MockMemTableAllocationTracker::ExternalAllocation::subsume(ExternalAllocation&& other)
{
  this->parent_mock_->allocation_subsumed(this, &other);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline StatusOr<MockMemTableAllocationTracker::ExternalAllocation>
MockMemTableAllocationTracker::ExternalAllocation::split(usize byte_count)
{
  return this->parent_mock_->allocation_split(this, byte_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize MockMemTableAllocationTracker::ExternalAllocation::size() const
{
  return this->parent_mock_->allocation_size(this);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

static_assert(MemTableExternalAllocation<MockMemTableAllocationTracker::ExternalAllocation>);
static_assert(MemTableAllocationTracker<MockMemTableAllocationTracker>);

}  // namespace testing
}  // namespace turtle_kv
