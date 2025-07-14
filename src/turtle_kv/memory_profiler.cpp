#include <turtle_kv/util/memory_profiler.hpp>
//

#include <absl/hash/hash.h>

namespace turtle_kv {

namespace detail {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool& inside_memory_hook()
{
  thread_local bool inside_ = false;
  return inside_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct MemoryHookGuard {
  bool was_inside_ = inside_memory_hook();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  MemoryHookGuard() noexcept
  {
    inside_memory_hook() = true;
  }

  ~MemoryHookGuard() noexcept
  {
    inside_memory_hook() = this->was_inside_;
  }

  explicit operator bool() const
  {
    return this->was_inside_;
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void turtlekv_new_hook(const void* ptr, usize size)
{
  MemoryHookGuard guard;
  if (guard) {
    return;
  }

  HeapMetrics& m = HeapMetrics::instance();

  m.new_count.add(1);
  m.active_stats.update(m.new_count.get() - m.delete_count.get());

  if (size > 255) {
    m.large_count.add(1);
    LOG_EVERY_N(INFO, 1) << BATT_INSPECT(m.active_stats) << BATT_INSPECT(ptr) << BATT_INSPECT(size)
                         << boost::stacktrace::stacktrace{} << std::endl;
    /*
                         << [](std::ostream& out) {
                              boost::stacktrace::stacktrace trace;
                              for (const boost::stacktrace::frame& frame : trace) {
                                out << frame.source_file() << ":" << frame.source_line() <<
       std::endl;
                              }
                              };*/
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void turtlekv_delete_hook(const void* ptr [[maybe_unused]])
{
  MemoryHookGuard guard;
  if (guard) {
    return;
  }

  HeapMetrics::instance().delete_count.add(1);
}

}  // namespace detail

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ void MemoryProfiler::set_enabled(bool b)
{
  if (b) {
    MallocHook_AddNewHook(&detail::turtlekv_new_hook);
    MallocHook_AddDeleteHook(&detail::turtlekv_delete_hook);
  }
}

}  // namespace turtle_kv
