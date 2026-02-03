#pragma once
#define TURTLE_KV_UTIL_MEMORY_STATS_HPP

#include <turtle_kv/config.hpp>
//
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/slice.hpp>

#if TURTLE_KV_ENABLE_TCMALLOC
#include <gperftools/malloc_extension.h>
#endif  // TURTLE_KV_ENABLE_TCMALLOC

#include <batteries/stream_util.hpp>

#include <malloc.h>

namespace turtle_kv {

inline auto print_size(usize n)
{
  return [n](std::ostream& out) {
    out << n << "(" << batt::dump_size(n) << ")";
  };
}

inline auto dump_memory_stats()
{
  return [](std::ostream& out) {
#if TURTLE_KV_ENABLE_TCMALLOC && TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING
    int n_blocks = 0;
    usize total = 0;
    int hist[kMallocHistogramSize];

    auto* malloc_ext = MallocExtension::instance();
    {
      const bool ok = malloc_ext->MallocMemoryStats(&n_blocks, &total, hist);
      if (!ok) {
        out << "(MallocMemoryStats failed)";
      } else {
        if (n_blocks == 0 && total == 0 &&
            std::all_of(hist, hist + kMallocHistogramSize, [](int n) {
              return n == 0;
            })) {
          out << "\n memory_stats == {0}";
        } else {
          out << " memory_stats.n_blocks == " << n_blocks                 //
              << "\n memory_stats.total == " << total                     //
              << "\n memory_stats.histogram[   0..1   ] == " << hist[0]   //
              << "\n memory_stats.histogram[   2..4   ] == " << hist[1]   //
              << "\n memory_stats.histogram[   4..8   ] == " << hist[2]   //
              << "\n memory_stats.histogram[   8..16  ] == " << hist[3]   //
              << "\n memory_stats.histogram[  16..32  ] == " << hist[4]   //
              << "\n memory_stats.histogram[  32..64  ] == " << hist[5]   //
              << "\n memory_stats.histogram[  64..128 ] == " << hist[6]   //
              << "\n memory_stats.histogram[ 128..256 ] == " << hist[7]   //
              << "\n memory_stats.histogram[ 256..512 ] == " << hist[8]   //
              << "\n memory_stats.histogram[ 512..1K  ] == " << hist[9]   //
              << "\n memory_stats.histogram[  1K..2K  ] == " << hist[10]  //
              << "\n memory_stats.histogram[  2K..4K  ] == " << hist[11]  //
              << "\n memory_stats.histogram[  4K..8K  ] == " << hist[12]  //
              << "\n memory_stats.histogram[  8K..16K ] == " << hist[13]  //
              << "\n memory_stats.histogram[ 16K..32K ] == " << hist[14]  //
              << "\n memory_stats.histogram[ 32K..64K ] == " << hist[15]  //
              << "\n memory_stats.histogram[ 64K..128K] == " << hist[16]  //
              << "\n memory_stats.histogram[128K..256K] == " << hist[17]  //
              << "\n memory_stats.histogram[256K..512K] == " << hist[18]  //
              << "\n memory_stats.histogram[512K..1M  ] == " << hist[19]  //
              << "\n memory_stats.histogram[  1M..2M  ] == " << hist[20]  //
              << "\n memory_stats.histogram[  2M..4M  ] == " << hist[21]  //
              << "\n memory_stats.histogram[  4M..8M  ] == " << hist[22]  //
              << "\n memory_stats.histogram[  8M..16M ] == " << hist[23]  //
              << "\n memory_stats.histogram[ 16M..32M ] == " << hist[24]  //
              << "\n memory_stats.histogram[ 32M..64M ] == " << hist[25]  //
              << "\n memory_stats.histogram[ 64M..128M] == " << hist[26]  //
              << "\n memory_stats.histogram[128M..256M] == " << hist[27]  //
              << "\n memory_stats.histogram[256M..512M] == " << hist[28]  //
              << "\n memory_stats.histogram[512M..1G  ] == " << hist[29]  //
              << "\n memory_stats.histogram[  1G..2G  ] == " << hist[30]  //
              << "\n memory_stats.histogram[  2G..4G  ] == " << hist[31]  //
              << "\n memory_stats.histogram[  4G..8G  ] == " << hist[32]  //
              << "\n memory_stats.histogram[  8G..16G ] == " << hist[33]  //
              << "\n memory_stats.histogram[ 16G..32G ] == " << hist[34]  //
              << "\n memory_stats.histogram[ 32G..64G ] == " << hist[35]  //
              << "\n memory_stats.histogram[ 64G..128G] == " << hist[36]  //
              << "\n memory_stats.histogram[128G..256G] == " << hist[37]  //
              << "\n memory_stats.histogram[256G..512G] == " << hist[38]  //
              << "\n memory_stats.histogram[512G..1T  ] == " << hist[39]  //
              ;
        }
      }
    }

    const auto get_malloc_stat = [&](const char* name) -> Optional<usize> {
      usize value = 0;
      if (malloc_ext->GetNumericProperty(name, &value)) {
        return value;
      }
      return None;
    };

    out << "\n" << BATT_INSPECT(get_malloc_stat("generic.current_allocated_bytes"));
    out << "\n" << BATT_INSPECT(get_malloc_stat("generic.heap_size"));
    out << "\n" << BATT_INSPECT(get_malloc_stat("generic.total_physical_bytes"));

#endif  // TURTLE_KV_ENABLE_TCMALLOC && TURTLE_KV_ENABLE_TCMALLOC_HEAP_PROFILING

    struct mallinfo2 mi2 = mallinfo2();
    out << "\n non-mapped allocated bytes: " << print_size(mi2.arena)     //
        << "\n                free chunks: " << print_size(mi2.ordblks)   //
        << "\n        free fastbin blocks: " << print_size(mi2.smblks)    //
        << "\n             mapped regions: " << print_size(mi2.hblks)     //
        << "\n               mapped bytes: " << print_size(mi2.hblkhd)    //
        << "\n         free fastbin bytes: " << print_size(mi2.fsmblks)   //
        << "\n      total allocated bytes: " << print_size(mi2.uordblks)  //
        << "\n           total free bytes: " << print_size(mi2.fordblks)  //
        << "\n           releasable bytes: " << print_size(mi2.keepcost)  //
        ;
  };
}

}  // namespace turtle_kv
