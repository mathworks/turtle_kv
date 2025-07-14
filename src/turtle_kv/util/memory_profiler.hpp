#pragma once

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/logging.hpp>
#include <turtle_kv/import/metrics.hpp>

#include <batteries/assert.hpp>
#include <batteries/finally.hpp>

#include <gperftools/malloc_hook.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

namespace turtle_kv {

struct HeapMetrics {
  CountMetric<i64> new_count{0};
  CountMetric<i64> delete_count{0};
  StatsMetric<i64> active_stats{0};
  CountMetric<i64> large_count{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i64 active_count() const
  {
    return this->new_count.get() - this->delete_count.get();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static HeapMetrics& instance()
  {
    static HeapMetrics instance_;
    return instance_;
  }
};

namespace detail {

bool& inside_memory_hook();

extern "C" {

void turtlekv_new_hook(const void* ptr, usize size);

void turtlekv_delete_hook(const void* ptr [[maybe_unused]]);

}  // extern "C"

}  // namespace detail

struct StackTrace {
  boost::stacktrace::stacktrace impl_;

  template <typename H>
  friend H AbslHashValue(H h, const StackTrace& st)
  {
    for (const boost::stacktrace::frame& frame : st.impl_) {
      h = H::combine(std::move(h), frame.address());
    }
    return h;
  }
};

inline bool operator==(const StackTrace& l, const StackTrace& r)
{
  return l.impl_ == r.impl_;
}

class MemoryProfiler
{
 public:
  using Self = MemoryProfiler;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static void set_enabled(bool b);

  static Self& instance();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MemoryProfiler() noexcept;

  ~MemoryProfiler() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void on_new(const void* ptr, usize size);

  void on_delete(const void* ptr, usize size);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct SiteInfo {
    i64 new_count = 0;
    i64 delete_count = 0;
    i64 new_size = 0;
    i64 delete_size = 0;
  };

  struct ThreadState {
    absl::flat_hash_map<StackTrace, SiteInfo> info;
  };

  ThreadState& thread_state();
};

}  // namespace turtle_kv
