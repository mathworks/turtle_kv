# TurtleKV: High Performance, Dynamically Tuned Key-Value Storage

[Link to Paper: "Dynamic read &amp; write optimization with TurtleKV"](https://arxiv.org/abs/2509.10714)

TurtleKV is a general-purpose, embedded key-value storage engine
designed with the goal of being the best choice for the widest
possible range of applications and workloads.

Key-Value storage offers an extremely general and powerful interface
for durably stored data: the dictionary.  Ideally, one would like to
have an implementation of this interface which simultaneously
optimizes for updates, queries, and memory usage.  However, theory has
proven
([1](https://perso.ens-lyon.fr/loris.marchal/docs-data-aware/papers/paper3.pdf))
and practice as shown
([2](https://stratos.seas.harvard.edu/sites/g/files/omnuum4611/files/stratos/files/rum.pdf))
that this is impossible.  Current state-of-the-art key-value engines,
when they offer tuning parameters to dial in the right balance for a
given application, usually alter the structure of stored data to trade
between updates (writes) and queries (reads): better write performance
means worse read performance (and vice versa).  This can be
compensated to an extent by page caching and AMQ filters (e.g., Bloom
filters), but a fundamental problem with this approach is that
_optimized writes in the past increase the cost of future reads for
the lifetime of stored data_.

TurtleKV is different.  Instead of using a write- or read-optimized
on-disk structure, TurtleKV uses a balanced data structure, the
TurtleTree.  TurtleTrees can be read or write optimized by allocating
memory to either page caching (reads) or update/checkpoint buffering
(writes) without changing their durable structure.  This means
applications can pay to optimize the most important operations
dynamically_, without locking in future costs.

Today, TurtleKV offers excellent performance relative to other
state-of-the-art key-value stores.  Because its tuning optimization
strategy is based on memory allocation rather than on-disk structure,
applications that use it to store data can continue to run faster and
more efficiently as TurtleKV is updated and improved, with no
requirement to migrate to a new data format.

TurtleKV uses the [Conan package management
system](https://conan.io/), and is built on MathWorks' [Low Level File
System (LLFS)](https://github.com/mathworks/llfs/) library.

## Getting Started

***The build instructions are currently broken, see #1 for updates***

**Note: TurtleKV is currently only supported on x86_64 architecture, on the GNU/Linux operating system.**

***Please read the [Limitations](#limitations) section before using TurtleKV!***

1. Install the `cor` command line tool ([instructions here](https://gitlab.com/batteriesincluded/batt-cli#cor-launcher-cor-toolkit-launcher-front-end))
2. Clone the TurtleKV repo and `cd` into the cloned directory
3. Build (`cor build`)
4. (Optional) Run tests (`cor test`)
5. Export to the local Conan cache (`cor export` or `cor export-pkg`)
6. Include a dependency to `turtle_kv/[>=0.0.20 <1]` in your conanfile.py or conanfile.txt

## Example Usage

```c++
#include <turtle_kv/kv_store.hpp>

#include <batteries/assert.hpp>

int main()
{
  namespace fs = std::filesystem;

  using turtle_kv::KVStore;
  using turtle_kv::RemoveExisting;
  using turtle_kv::Status;
  using turtle_kv::StatusOr;
  using turtle_kv::TreeOptions;
  using turtle_kv::ValueView;

  // First create a database instance.
  //
  Status create_status =
      KVStore::create(fs::path{"path/to/my/data"},
                      KVStore::Config::with_default_values(),
                      RemoveExisting{true});

  BATT_CHECK_OK(create_status);

  // Now open and use the new instance.
  //
  StatusOr<std::unique_ptr<KVStore>> opened_kv_store =
      KVStore::open(fs::path{"path/to/my/data"},
                    TreeOptions::with_default_values());

  BATT_CHECK_OK(opened_kv_store);

  // Use the key-value store.
  //
  KVStore& kv = **opened_kv_store;

  BATT_CHECK_OK(kv.put("hello", ValueView::from_str("world"));

  StatusOr<ValueView> value = kv.get("hello");
  BATT_CHECK_OK(value);
  BATT_CHECK_EQ(value->as_str(), "world");

  // Query a key that doesn't exist.
  //
  value = kv.get("no such key");
  BATT_CHECK(!value.ok());
  BATT_CHECK_EQ(value.status(), batt::StatusCode::kNotFound);

  return 0;
}
```

## Limitations

TurtleKV is a work-in-progress.  It should currently be considered a
research prototype developed to demonstrate proof-of-concept for
several novel ideas, including the TurtleTree data structure and
dynamic (on-line) trade-off tuning using checkpoint distance.  The
on-disk data layouts may change in backwards-incompatible ways prior
to version 1.0.0.  We are actively working to improve this code base
and add missing features; check back often for updates!

The current version of TurtleKV should **NOT** be considered a stable
storage system for critical data.  Releases of the Conan package are
generated from release tags in the Git repo (e.g., `release-0.0.21`).
**Any release starting with `0.` is considered experimental/alpha
grade software, subject to change and (probably) bugs when it comes to
storing and retrieving data.**  DO NOT put your only copy of any
important data into a `0.` release of TurtleKV!  Stable versions of
the software suitable for production use will begin with version
`1.0.0` (tag: `release-1.0.0`).

We currently support:

- Creating/opening an empty K/V store at runtime
- Multiple independent K/V stores open at from the same process
- Concurrent get/put/scan from multiple application threads
- Dynamic control of page cache size and checkpoint distance (to tune
  overall memory usage and control how much of the memory budget is
  used to optimize writes)
- Clean or crash-only shutdown
- Bloom Filters or Vector Quotient Filters (compile-time selectable),
  each with configurable bits-per-key rate
- Many stats/metrics to help analyze performance and debug

Currently missing/under development:

- Removing deletion tombstones from checkpoint trees
- Recovery of existing KVStore instances after shutdown
- Time-consistent isolated multi-key reads
- Key-only queries (i.e., `has_key`/`contains`; note, key-only scans
  *are* currently supported)
- Saving/loading full point-in-time snapshots
- `fsync` like blocking call to ensure updates are durable (in the WAL)

Limitations (out-of-scope):

- A K/V store may only be opened by one process at a time
