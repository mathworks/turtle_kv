package turtle_kv

//
//

// #cgo CFLAGS: -march=native -I../install/include
// #cgo LDFLAGS: -L../install/lib -lturtlekv_bundle -lm -lstdc++
// #include "turtlekv.h"
//
import "C"
import "unsafe"
import "fmt"
import "errors"

type StrView = C.turtlekv_StrView

func ToGoString(str StrView) string {
	return string(C.GoBytes(unsafe.Pointer(str.data), (C.int)(str.size)))
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
type Status struct {
	impl C.turtlekv_Status
}

type StatusCode = C.turtlekv_StatusCode

type IStatus interface {
	Ok() bool
	Code() int
	Message() string
}

func NewStatus(code StatusCode) Status {
	var status Status
	status.impl.code = code
	return status
}

func (status Status) Ok() bool {
	return (bool)(C.turtlekv_status_is_ok(status.impl))
}

func (status Status) Message() string {
	return ToGoString(C.turtlekv_status_get_message(status.impl))
}

func (status Status) Code() int {
	return int((C.int)(status.impl.code))
}

func (status Status) String() string {
	return fmt.Sprintf("%d:%s", status.Code(), status.Message())
}

func (status Status) ToError() error {
	return errors.New(status.String())
}

func (status Status) Error() string {
	return status.String()
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

const STATUS_CODE_OK = C.TURTLEKV_STATUS_CODE_OK

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStoreOptions struct {
	impl *C.turtlekv_KVStoreOptions
}

func DefaultOptions() KVStoreOptions {
	var options KVStoreOptions
	options.impl = C.turtlekv_kv_store_options_create_default()
	return options
}

func (options KVStoreOptions) Len() int {
	return (int)(C.turtlekv_kv_store_get_num_options())
}

func (options KVStoreOptions) Name(i int) string {
	return C.GoString(C.turtlekv_kv_store_option_name_for_index((C.size_t)(i)))
}

func (options KVStoreOptions) Get(name string) (string, error) {
	index_for_name := C.turtlekv_kv_store_option_index_for_name(C.CString(name))
	if !NewStatus(index_for_name.status).Ok() {
		return "", NewStatus(index_for_name.status).ToError()
	}
	return "foo", nil
}

func (result C.turtlekv_StatusOrInt64) ConvertResult() (int64, error) {
	if result.status.code == STATUS_CODE_OK {
		return (int64)(result.status.i), nil
	}
	return (int64)(-1), NewStatus(result.status.code).ToError()
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStore struct {
	impl C.turtlekv_KVStore
}

func Hello(name string) string {
	message := fmt.Sprintf("Hi, %v! from TurtleKV", name)
	return message
}
