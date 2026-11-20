package main

import (
	"fmt"
	"mathworks.com/turtle_kv"
	"sync"
)

func main() {

	// Initialize the Runtime.
	//
	runtime, err := turtle_kv.InitRuntime()
	if err != nil {
		panic(err)
	}
	defer runtime.Close()

	options := config_examples() //turtle_kv.DefaultOptions()
	defer options.Close()

	// Create a new status value.
	//
	var status = turtle_kv.NewStatus(turtle_kv.STATUS_CODE_OK)
	fmt.Printf("\nHere is a status: %s\n", status)

	create_and_use(runtime, options)

	fmt.Println("opening")
	kv, err := runtime.OpenKVStore("/tmp/turtle_go_demo/kv_store/", options)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// Do a full scan to make sure everything was saved!
	//
	fmt.Println("re-scanning")
	ctx := kv.CreateContext()
	defer ctx.Close()
	scan_example2(ctx, "", 25 /*limit*/)
	fmt.Println("cleaning up")
}

func config_examples() turtle_kv.KVStoreOptions {

	// Create an Options object, with default values.
	//
	options := turtle_kv.DefaultOptions()

	// Enumerate the available config options.
	//
	fmt.Printf("There are %d config options: ", options.Len())
	for i := 0; i < options.Len(); i++ {
		fmt.Printf("%s, ", options.Name(i))
	}
	fmt.Printf("\n\n")

	saved := options.GetInitialCapacityBytes()
	defer options.SetInitialCapacityBytes(saved)

	options.SetBTreeMode(options.GetBTreeMode())
	options.SetBufferLevelTrim(options.GetBufferLevelTrim())
	options.SetCacheSizeBytes(options.GetCacheSizeBytes())
	options.SetCacheSizeMB(options.GetCacheSizeMB())
	options.SetCheckpointPipeline(options.GetCheckpointPipeline())
	options.SetCheckpointDistance(options.GetCheckpointDistance())
	options.SetFilterBits(options.GetFilterBits())
	options.SetInitialCapacityBytes(options.GetInitialCapacityBytes())
	options.SetInitialCapacityGB(max(1, options.GetInitialCapacityGB()))
	options.SetKeySizeHint(options.GetKeySizeHint())
	options.SetLeafSizeBytes(options.GetLeafSizeBytes())
	options.SetLeafSizeKB(options.GetLeafSizeKB())
	options.SetMaxCapacityBytes(options.GetMaxCapacityBytes())
	options.SetMaxCapacityGB(max(1, options.GetMaxCapacityGB()))
	options.SetMaxFlush(options.GetMaxFlush())
	options.SetMinFlush(options.GetMinFlush())
	options.SetNodeSizeBytes(options.GetNodeSizeBytes())
	options.SetNodeSizeKB(options.GetNodeSizeKB())
	options.SetSizeTiered(options.GetSizeTiered())
	options.SetValueSizeHint(options.GetValueSizeHint())
	options.SetWALSizeBytes(options.GetWALSizeBytes())
	options.SetWALSizeMB(options.GetWALSizeMB())

	fmt.Println("b_tree_mode =", options.GetBTreeMode())
	fmt.Println("buffer_level_trim =", options.GetBufferLevelTrim())
	fmt.Println("cache_size_bytes =", options.GetCacheSizeBytes())
	fmt.Println("cache_size_mb =", options.GetCacheSizeMB())
	fmt.Println("checkpoint_pipeline =", options.GetCheckpointPipeline())
	fmt.Println("checkpoint_distance =", options.GetCheckpointDistance())
	fmt.Println("filter_bits =", options.GetFilterBits())
	fmt.Println("initial_capacity_bytes =", options.GetInitialCapacityBytes())
	fmt.Println("initial_capacity_gb =", options.GetInitialCapacityGB())
	fmt.Println("key_size_hint =", options.GetKeySizeHint())
	fmt.Println("leaf_size_bytes =", options.GetLeafSizeBytes())
	fmt.Println("leaf_size_kb =", options.GetLeafSizeKB())
	fmt.Println("max_capacity_bytes =", options.GetMaxCapacityBytes())
	fmt.Println("max_capacity_gb =", options.GetMaxCapacityGB())
	fmt.Println("max_flush =", options.GetMaxFlush())
	fmt.Println("min_flush =", options.GetMinFlush())
	fmt.Println("node_size_bytes =", options.GetNodeSizeBytes())
	fmt.Println("node_size_kb =", options.GetNodeSizeKB())
	fmt.Println("size_tiered =", options.GetSizeTiered())
	fmt.Println("value_size_hint =", options.GetValueSizeHint())
	fmt.Println("wal_size_bytes =", options.GetWALSizeBytes())
	fmt.Println("wal_size_mb =", options.GetWALSizeMB())

	return options

}

func create_and_use(runtime turtle_kv.Runtime, options turtle_kv.KVStoreOptions) {
	// Create a KVStore.
	//
	err := runtime.CreateKVStore("/tmp/turtle_go_demo/kv_store", options, true /*remove_existing*/)
	if err != nil {
		panic(err)
	}

	// Open the KVStore.
	//
	var kv turtle_kv.KVStore
	kv, err = runtime.OpenKVStore("/tmp/turtle_go_demo/kv_store", options)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// Run some usage examples.
	//
	usage_examples(kv)
}

func usage_examples(kv turtle_kv.KVStore) {
	// Each thread/goroutine needs its own KVStoreContext object!
	//
	ctx := kv.CreateContext()
	defer ctx.Close()

	fmt.Println("\nPutting some keys...")
	ctx.Put("alpha", "The letter A")
	ctx.Put("bravo", "The letter B")
	ctx.Put("charlie", "The letter C")

	ctx.PutBytes(([]byte)("bytes"), []byte{101, 102, 103, 104, 105, 106, 107, 108, 109})

	fmt.Println("  Ok\n\nGetting some keys...")
	value, _ := ctx.Get("alpha")
	fmt.Println("  alpha =", value)

	value, _ = ctx.Get("bravo")
	fmt.Println("  bravo =", value)

	value, _ = ctx.Get("charlie")
	fmt.Println("  charlie =", value)

	raw_bytes, _ := ctx.GetBytes(([]byte)("charlie"))
	fmt.Println("  (raw bytes)", raw_bytes)

	var err error
	_, err = ctx.Get("tango")
	fmt.Println("  tango =", err)

	// Do some scanning, from various starting keys.
	//
	fmt.Println("\nAnd finally, let's scan over everything")
	scan_example(ctx, "")

	fmt.Println("\nScan from 'a'...")
	scan_example(ctx, "a")

	fmt.Println("\nScan from 'b'...")
	scan_example(ctx, "b")

	fmt.Println("\nScan from 'c'...")
	scan_example(ctx, "c")

	fmt.Println("\nScan from 'd'...")
	scan_example(ctx, "d")

	fmt.Println()

	stress_example(kv)
	fmt.Println()
}

func scan_example(ctx turtle_kv.KVStoreContext, min_key string) {
	for item, err := range ctx.Scan(min_key) {
		if err != nil {
			fmt.Println("  Scan ended with error:", err)
			break
		}
		fmt.Println(" - ", item.Key, " -> ", item.Value)
	}
}

func scan_example2(ctx turtle_kv.KVStoreContext, min_key string, limit int) {
	count := 0
	for item, err := range ctx.Scan(min_key) {
		if err != nil {
			fmt.Println("  Scan ended with error:", err)
			break
		}

		// We can unpack the key and value like this.
		//
		key, value := item.Parts()

		fmt.Println(" - ", key, " -> ", value)
		count += 1
		if count >= limit {
			fmt.Println(" (reached limit; stopping scan)")
			break
		}
	}
}

func stress_worker(kv turtle_kv.KVStore, round int, op_count int, worker_i int, n_workers int, group *sync.WaitGroup) {
	defer group.Done()

	// Each thread/goroutine needs its own KVStoreContext object!
	//
	ctx := kv.CreateContext()
	defer ctx.Close()

	// After round 0, we do two operations per loop iteration (a Get followed by a Put/update).
	//
	loop_count := op_count
	if round > 0 {
		loop_count /= 2
	}

	// The workers divide the work as evenly as possible.
	//
	for i := worker_i; i < loop_count; i += n_workers {
		if round > 0 {
			_, err := ctx.Get(fmt.Sprintf("key_%d", i))
			if err != nil {
				panic(err)
			}
		}

		err := ctx.Put(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_round-%d_count-%d_worker-%d-of-%d",
			round, loop_count, worker_i, n_workers))

		if err != nil {
			panic(err)
		}
	}
}

func stress_example(kv turtle_kv.KVStore) {
	const count = 1000 * 1000
	const n_workers = 32
	const n_rounds = 10

	fmt.Printf("Running stress test with %d workers, %d operations per round...\n", n_workers, count)

	for round := 0; round < n_rounds; round += 1 {
		var group sync.WaitGroup
		for worker_i := 0; worker_i < n_workers; worker_i += 1 {
			group.Add(1)
			go stress_worker(kv, round, count, worker_i, n_workers, &group)
		}
		group.Wait()
		fmt.Printf("   Done with round %d of %d\n", round+1, n_rounds)
	}
}
