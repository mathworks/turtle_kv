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

	// Create a new status value.
	//
	var status = turtle_kv.NewStatus(turtle_kv.STATUS_CODE_OK)
	fmt.Println(status)

	// Create an Options object, with default values.
	//
	options := turtle_kv.DefaultOptions()

	// How many options are there?
	//
	fmt.Println("options:", options, " len:", options.Len())

	// What are the options' names?
	//
	for i := 0; i < options.Len(); i++ {
		fmt.Println(options.Name(i))
	}

	// Get option values, various types.
	//
	default_b_tree_mode, _ := options.GetBool("b_tree_mode")
	fmt.Println("default b_tree_mode =", default_b_tree_mode)

	default_leaf_size, _ := options.GetUInt("leaf_size")
	fmt.Println("default leaf_size =", default_leaf_size)

	// Create a KVStore.
	//
	err = runtime.CreateKVStore("/tmp/turtle_go_demo/kv_store", options, true /*remove_existing*/)
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

	// Each thread/goroutine needs its own KVStoreContext object!
	//
	ctx := kv.CreateContext()
	defer ctx.Close()

	fmt.Println("\nPutting some keys...")
	ctx.Put("alpha", "The letter A")
	ctx.Put("bravo", "The letter B")
	ctx.Put("charlie", "The letter C")

	fmt.Println("  Ok\n\nGetting some keys...")
	value, _ := ctx.Get("alpha")
	fmt.Println("  alpha =", value)

	value, _ = ctx.Get("bravo")
	fmt.Println("  bravo =", value)

	value, _ = ctx.Get("charlie")
	fmt.Println("  charlie =", value)

	_, err = ctx.Get("tango")
	fmt.Println("  tango =", err)

	// Do some scanning, from various starting keys.
	//
	fmt.Println("\nAnd finally, let's scan over everything")
	for pair, err2 := range ctx.Scan("") {
		if err2 != nil {
			fmt.Println("  Scan ended with error:", err2)
			break
		}
		fmt.Println(" - ", pair.Key, " -> ", pair.Value)
	}

	fmt.Println("\nScan from 'a'...")
	for pair, err2 := range ctx.Scan("a") {
		if err2 != nil {
			fmt.Println("  Scan ended with error:", err2)
			break
		}
		// We can destructure the pair using Parts func
		//
		key, value := pair.Parts()
		fmt.Println(" - ", key, " -> ", value)
	}

	fmt.Println("\nScan from 'b'...")
	for pair, err2 := range ctx.Scan("b") {
		if err2 != nil {
			fmt.Println("  Scan ended with error:", err2)
			break
		}
		key, value := pair.Parts()
		fmt.Println(" - ", key, " -> ", value)
	}

	fmt.Println("\nScan from 'c'...")
	for pair, err2 := range ctx.Scan("c") {
		if err2 != nil {
			fmt.Println("  Scan ended with error:", err2)
			break
		}
		key, value := pair.Parts()
		fmt.Println(" - ", key, " -> ", value)
	}

	fmt.Println("\nScan from 'd'...")
	for pair, err2 := range ctx.Scan("d") {
		if err2 != nil {
			fmt.Println("  Scan ended with error:", err2)
			break
		}
		key, value := pair.Parts()
		fmt.Println(" - ", key, " -> ", value)
	}
}
