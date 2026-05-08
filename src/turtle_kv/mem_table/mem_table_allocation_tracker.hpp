//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_MEM_TABLE_ALLOCATION_TRACKER_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache_overcommit.hpp>

#include <batteries/suppress.hpp>

#include <ostream>
#include <type_traits>
#include <utility>

namespace turtle_kv {

BATT_SUPPRESS_IF_GCC("-Wself-move")

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept MemTableExternalAllocation =                        //
    requires(T& obj, const T& const_obj, usize byte_count)  //
{
  // Must be move-constructible.
  //
  T{std::move(obj)};

  // Must be move-assignable.
  //
  obj = std::move(obj);

  // Must be destructible.
  //
  obj.~T();

  // Must have subsume member function.
  //
  obj.subsume(std::move(obj));

  // Must have split member function.
  //
  { obj.split(byte_count) } -> std::convertible_to<StatusOr<T>>;

  // Must have size member function.
  //
  { const_obj.size() } -> std::convertible_to<usize>;
};

BATT_UNSUPPRESS_IF_GCC()

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
concept MemTableAllocationTracker =  //
    requires(T& obj,
             usize byte_size,
             llfs::PageCacheOvercommit& overcommit,
             void (*info_dumper)(std::ostream&))  //
{
  // Must have member type ExternalAllocation, which satisfies the requirements for
  // MemTableExternalAllocation.
  //
  requires MemTableExternalAllocation<typename T::ExternalAllocation>;

  // Must have an allocate_external member function, which creates an ExternalAllocation.
  //
  {
    obj.allocate_external(byte_size, overcommit)
  } -> std::convertible_to<typename T::ExternalAllocation>;

  // Must have an on_overcommit member function to report over-allocation.
  //
  obj.on_overcommit(info_dumper);
};

}  // namespace turtle_kv
