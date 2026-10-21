# TurtleKV: Go Bindings

> **NOTE: All commands must be run inside the directory containing this README!**

## How to Build

The script `./pkg-config-turtlekv.sh` will create a local folder
`./build/` containing the artifacts needed for CGO to link against
`libturtlekv` (the C interface for TurtleKV).  In order for `go` to be
able to find it, you must set the environment variable `PKG_CONFIG`:

```shell
export PKG_CONFIG=$(pwd)/pkg-config-turtlekv.sh
```

Now we can build the Go package:

```shell
go build
```

The first time you run this command, it may take a little while
because turtlekv and all its dependencies have to be built and
installed under `./build/`.  The `pkg-config-turtlekv.sh` script
caches its result when it succeeds, so subsequent builds will be much
faster.

## Example Usage

```go
package main

import (
	"fmt"
	"mathworks.com/turtle_kv"
)

func main() {
	// Initialize the Runtime.
	//
	runtime, err := turtle_kv.InitRuntime()
	if err != nil {
		panic(err)
	}
	defer runtime.Close()
	
	// Create a KVStoreOptions object, with default values.
	//
	options := turtle_kv.DefaultOptions()
	defer options.Close()

	// Create a new KVStore on the filesystem.
	//
	err = runtime.CreateKVStore("/tmp/turtle_go_demo/kv_store", options, true /*remove_existing*/)
	if err != nil {
		panic(err)
	}
	
	// Now open the KVStore we just created.
	//
	var kv turtle_kv.KVStore
	kv, err = runtime.OpenKVStore("/tmp/turtle_go_demo/kv_store", options)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// IMPORTANT: Each thread/goroutine needs its own KVStoreContext.
	//
	ctx := kv.CreateContext()
	defer ctx.Close()

	// Insert a key/value pair.
	//
	key := "hello"
	ctx.Put(key, "from TurtleKV!")
	
	// Now read it back.
	//
	value, err2 := ctx.Get(key)
	if err2 != nil {
		panic(err2)
	}
	fmt.Printf("The value of key '%s' is '%s'\n", key, value)
	
	// (all deferred Close() statements now run...)
}
```

## User Guide

### Lifecycle

> General Note: TurtleKV's Go bindings create native objects which
> cannot be freed by Go's garbage collector.  These must be released
> by calling their `Close()` method when you are done using them!

The `turtle_kv.Runtime` object must be instantiated exactly once by
your Go program.  This must come before any other usage of the
TurtleKV Go API:

```go
var runtime turtle_kv.Runtime
runtime, err = turtle_kv.InitRuntime()
if err != nil {
	panic(err)
}
defer runtime.Close()
```

A key-value database instance in TurtleKV is called a `KVStore`.  You
can create a KVStore on-disk by calling
`turtle_kv.Runtime.CreateKVStore`.  This does *not* open the database
for use in the current program, however.  After successfully creating
the KVStore on disk, you can open it like so:

```go
var options turtle_kv.KVStoreOptions
options = turtle_kv

var kv turtle_kv.KVStore
kv, err = runtime.OpenKVStore("/tmp/turtle_go_demo/kv_store", options)
if err != nil {
    panic(err)
}
defer kv.Close()
```

A `KVStore` object requires thread-specific state, called a
`KVStoreContext`, to perform updates and queries on the database.  To
create a `KVStoreContext`, each goroutine in your program must do:

```go 
ctx = kv.CreateContext()
defer ctx.Close()
```

> Note: since create a context just allocates memory and initializes
> it, there is no error returned, only the context itself.

#### List of Native-Resource Types That Must Be Close()-d

- `turtle_kv.Runtime`
- `turtle_kv.KVStoreOptions`
- `turtle_kv.KVStore`
- `turtle_kv.KVStoreContext`
- `turtle_kv.KVStoreScanner`

### Error Reporting

The `turtle_kv` Go package reports errors in the idiomatic Go style:
by returning a second `error` result from functions which can fail.
Internally, TurtleKV uses integer error codes to report all errors.
These are wrapped in a C/C++ `Status` struct type.  Status values can
be created and used directly using Go wrapper type `turtle_kv.Status`.
This type copies the integer status code into Go, so it doesn't need
to be freed by calling a `Close()` function.

### Thread-Safety and Concurrency

A single on-disk KVStore instance must only be opened by a single
program at a time.  Within a given program, multiple KVStores can be
open and used concurrently.  A `KVStore` object is safe to use from
multiple goroutines/threads; however, in order to perform updates and
queries, each goroutine/thread must create is own exclusive
`KVStoreContext` object.  `KVStoreContext` is **NOT** thread-safe!  A
goroutine must create its own context and `Close()` it when done.

The overhead of creating a `KVStoreContext` is much lower than opening
a `KVStore`, but it is not zero.  Therefore, goroutines should try to
create a single context and use it many times to amortize this cost.

It is safe, though not necessary, for a single goroutine/thread to
create and use multiple `KVStoreContext` objects, so long as they are
never shared (e.g. over a channel etc.).

### Memory Management of Key/Value Pairs

By default, keys and values are represented as Go `string` values.
This means a copy must be made when getting/putting key/value pairs.
