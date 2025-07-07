# TurtleKV: High Performance, Dynamically Tuned Key-Value Storage

TurtleKV is a general-purpose, embedded key-value storage engine designed with the goal of being the best choice for the widest possible range of applications and workloads.

Key-Value storage offers an extremely general and powerful interface for durably stored data: the dictionary.  Ideally, one would like to have an implementation of this interface which simultaneously optimizes for updates, queries, and memory usage.  However, theory has proven ([1](https://perso.ens-lyon.fr/loris.marchal/docs-data-aware/papers/paper3.pdf)) and practice as shown ([2](https://stratos.seas.harvard.edu/sites/g/files/omnuum4611/files/stratos/files/rum.pdf)) that this is impossible.  Current state-of-the-art key-value engines, when they offer tuning parameters to dial in the right balance for a given application, usually alter the structure of stored data to trade between updates (writes) and queries (reads): better write performance means worse read performance (and vice versa).  This can be compensated to an extent by page caching and AMQ filters (e.g., Bloom filters), but a fundamental problem with this approach is that _optimized writes in the past increase the cost of future reads for the lifetime of stored data_.

TurtleKV is different.  Instead of using a write- or read-optimized on-disk structure, TurtleKV uses a balanced data structure, the Turtle Tree.  Turtle Trees can be read or write optimized by allocating memory to either page caching (reads) or update/checkpoint buffering (writes) without changing their durable structure.  This means applications can pay to optimize the most important operations _dynamically_, without locking in future costs.

Today, TurtleKV offers excellent performance relative to other state-of-the-art key-value stores.  Because its tuning optimization strategy is based on memory allocation rather than on-disk structure, applications that use it to store data can continue to run faster and more efficiently as TurtleKV is updated and improved, with no requirement to migrate to a new data format.

TurtleKV uses the [Conan package management system](https://conan.io/), and is built on MathWorks' [Low Level File System (LLFS)](https://github.com/mathworks/llfs/) library.

## Getting Started

**Note: TurtleKV is currently only supported on x86_64 architecture, on the GNU/Linux operating system.**

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

  BATT_CHECK_OK(kv.put("hello"), ValueView::from_str("world"));

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
