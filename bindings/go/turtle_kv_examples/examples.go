package main

// #cgo CFLAGS: -march=native -I../install/include
// #cgo LDFLAGS: -L../install/lib -lturtlekv_bundle -lm -lstdc++
// #include "turtlekv.h"
//
import "C"

//import "unsafe"

import (
	"fmt"
	"mathworks.com/turtle_kv"
)

func main() {

	var status = turtle_kv.NewStatus(turtle_kv.STATUS_CODE_OK)

	options := turtle_kv.DefaultOptions()
	fmt.Println("options:", options, " len:", options.Len())

	for i := 0; i < options.Len(); i++ {
		fmt.Println(options.Name(i))
	}

	fmt.Println(status)

	fmt.Println("Hello, TurtleKV!")
	fmt.Println(turtle_kv.Hello("example"))
}
