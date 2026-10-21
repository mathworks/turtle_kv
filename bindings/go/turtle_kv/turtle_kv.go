package turtle_kv

// #cgo pkg-config: turtlekv
// #include "turtlekv.h"
//
import "C"
import (
	"errors"
	"fmt"
	"iter"
	"unsafe"
)

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//  TODO [tastolfi 2026-10-20] Remaining Features:
//
//  - Log event handler callbacks (see https://stackoverflow.com/questions/37157379/passing-function-pointer-to-the-c-code-using-cgo for the general idea)
//  - Support Scanner read (again, using slices)
//  - Doc comments for everything in this file
//
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

type StrView = C.turtlekv_StrView

func ToGoString(str StrView) string {
	return string(C.GoBytes(unsafe.Pointer(str.data), (C.int)(str.size)))
}

func ToGoSlice(str StrView) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(str.data)), (int)(str.size))
}

func KeyToGoString(key StrView) string {
	return ToGoString(key)
}

func KeyToGoSlice(key StrView) []byte {
	return ToGoSlice(key)
}

func ValueToGoString(value C.turtlekv_ValueView) string {
	if value.kind == C.TURTLEKV_VALUE_KIND_PUT {
		return string(C.GoBytes(unsafe.Pointer(value.data), (C.int)(value.size)))
	}
	return ""
}

func ValueToGoSlice(value C.turtlekv_ValueView) []byte {
	if value.kind == C.TURTLEKV_VALUE_KIND_PUT {
		return unsafe.Slice((*byte)(unsafe.Pointer(value.data)), (int)(value.size))
	}
	return nil
}

func StrToKey(str string) C.turtlekv_KeyView {
	var key C.turtlekv_KeyView
	key.data = C.CString(str)
	key.size = (C.size_t)(len(str))
	return key
}

func BytesToKey(raw_bytes []byte) C.turtlekv_KeyView {
	var key C.turtlekv_KeyView
	key.data = (*C.char)(unsafe.Pointer(&raw_bytes[0]))
	key.size = (C.size_t)(len(raw_bytes))
	return key
}

func BytesToValue(raw_bytes []byte) C.turtlekv_ValueView {
	var value C.turtlekv_ValueView
	value.kind = C.TURTLEKV_VALUE_KIND_PUT
	value.data = (*C.char)(unsafe.Pointer(&raw_bytes[0]))
	value.size = (C.size_t)(len(raw_bytes))
	return value
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
const STATUS_CODE_CANCELLED = C.TURTLEKV_STATUS_CODE_CANCELLED
const STATUS_CODE_UNKNOWN = C.TURTLEKV_STATUS_CODE_UNKNOWN
const STATUS_CODE_INVALID_ARGUMENT = C.TURTLEKV_STATUS_CODE_INVALID_ARGUMENT
const STATUS_CODE_DEADLINE_EXCEEDED = C.TURTLEKV_STATUS_CODE_DEADLINE_EXCEEDED
const STATUS_CODE_NOT_FOUND = C.TURTLEKV_STATUS_CODE_NOT_FOUND
const STATUS_CODE_ALREADY_EXISTS = C.TURTLEKV_STATUS_CODE_ALREADY_EXISTS
const STATUS_CODE_PERMISSION_DENIED = C.TURTLEKV_STATUS_CODE_PERMISSION_DENIED
const STATUS_CODE_RESOURCE_EXHAUSTED = C.TURTLEKV_STATUS_CODE_RESOURCE_EXHAUSTED
const STATUS_CODE_FAILED_PRECONDITION = C.TURTLEKV_STATUS_CODE_FAILED_PRECONDITION
const STATUS_CODE_ABORTED = C.TURTLEKV_STATUS_CODE_ABORTED
const STATUS_CODE_OUT_OF_RANGE = C.TURTLEKV_STATUS_CODE_OUT_OF_RANGE
const STATUS_CODE_UNIMPLEMENTED = C.TURTLEKV_STATUS_CODE_UNIMPLEMENTED
const STATUS_CODE_INTERNAL = C.TURTLEKV_STATUS_CODE_INTERNAL
const STATUS_CODE_UNAVAILABLE = C.TURTLEKV_STATUS_CODE_UNAVAILABLE
const STATUS_CODE_DATA_LOSS = C.TURTLEKV_STATUS_CODE_DATA_LOSS
const STATUS_CODE_UNAUTHENTICATED = C.TURTLEKV_STATUS_CODE_UNAUTHENTICATED
const STATUS_CODE_CLOSED = C.TURTLEKV_STATUS_CODE_CLOSED
const STATUS_CODE_GRANT_UNAVAILABLE = C.TURTLEKV_STATUS_CODE_GRANT_UNAVAILABLE
const STATUS_CODE_LOOP_BREAK = C.TURTLEKV_STATUS_CODE_LOOP_BREAK
const STATUS_CODE_END_OF_STREAM = C.TURTLEKV_STATUS_CODE_END_OF_STREAM
const STATUS_CODE_CLOSED_BEFORE_END_OF_STREAM = C.TURTLEKV_STATUS_CODE_CLOSED_BEFORE_END_OF_STREAM
const STATUS_CODE_GRANT_REVOKED = C.TURTLEKV_STATUS_CODE_GRANT_REVOKED
const STATUS_CODE_POKE = C.TURTLEKV_STATUS_CODE_POKE

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

func ConvertStatus(status C.turtlekv_Status) error {
	if status.code == STATUS_CODE_OK {
		return nil
	}
	return NewStatus(status.code).ToError()
}

func ConvertStatusOrInt64(result C.turtlekv_StatusOrInt64) (int64, error) {
	if result.status.code == STATUS_CODE_OK {
		return (int64)(result.i), nil
	}
	return (int64)(-1), NewStatus(result.status.code).ToError()
}

func ConvertStatusOrUInt64(result C.turtlekv_StatusOrUInt64) (uint64, error) {
	if result.status.code == STATUS_CODE_OK {
		return (uint64)(result.n), nil
	}
	return (uint64)(0), NewStatus(result.status.code).ToError()
}

func ConvertStatusOrDouble(result C.turtlekv_StatusOrDouble) (float64, error) {
	if result.status.code == STATUS_CODE_OK {
		return (float64)(result.d), nil
	}
	return (float64)(0), NewStatus(result.status.code).ToError()
}

func ConvertStatusOrBool(result C.turtlekv_StatusOrBool) (bool, error) {
	if result.status.code == STATUS_CODE_OK {
		return (bool)(result.b), nil
	}
	return false, NewStatus(result.status.code).ToError()
}

func ConvertStatusOrStrView(result C.turtlekv_StatusOrStrView) (string, error) {
	if result.status.code == STATUS_CODE_OK {
		return ToGoString(result.str), nil
	}
	return "", NewStatus(result.status.code).ToError()
}

func ConvertStatusOrValueView(result C.turtlekv_StatusOrValueView) (string, error) {
	if result.status.code == STATUS_CODE_OK {
		return string(C.GoBytes(unsafe.Pointer(result.value.data), (C.int)(result.value.size))), nil
	}
	return "", NewStatus(result.status.code).ToError()
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStoreOptions struct {
	impl *C.turtlekv_KVStoreOptions
}

func DefaultOptions() KVStoreOptions {
	var options KVStoreOptions
	options.impl = C.turtlekv_kv_store_options_create_default()
	return options
}

func (options KVStoreOptions) Close() {
	if options.impl != nil {
		C.turtlekv_kv_store_options_destroy(options.impl)
		options.impl = nil
	}
}

func (options KVStoreOptions) Len() int {
	return (int)(C.turtlekv_kv_store_get_num_options())
}

func (options KVStoreOptions) Name(i int) string {
	return C.GoString(C.turtlekv_kv_store_option_name_for_index((C.size_t)(i)))
}

func (options KVStoreOptions) GetStr(name string) (string, error) {
	return ConvertStatusOrStrView(C.turtlekv_kv_store_options_get_str_view(options.impl, C.CString(name)))
}

func (options KVStoreOptions) GetInt(name string) (int64, error) {
	return ConvertStatusOrInt64(C.turtlekv_kv_store_options_get_int64(options.impl, C.CString(name)))
}

func (options KVStoreOptions) GetUInt(name string) (uint64, error) {
	return ConvertStatusOrUInt64(C.turtlekv_kv_store_options_get_uint64(options.impl, C.CString(name)))
}

func (options KVStoreOptions) GetFloat(name string) (float64, error) {
	return ConvertStatusOrDouble(C.turtlekv_kv_store_options_get_double(options.impl, C.CString(name)))
}

func (options KVStoreOptions) GetBool(name string) (bool, error) {
	return ConvertStatusOrBool(C.turtlekv_kv_store_options_get_bool(options.impl, C.CString(name)))
}

func (options KVStoreOptions) SetStr(name string, value string) error {
	return ConvertStatus(C.turtlekv_kv_store_options_set_str(options.impl, C.CString(name), C.CString(value)))
}

func (options KVStoreOptions) SetInt(name string, value int64) error {
	return ConvertStatus(C.turtlekv_kv_store_options_set_int64(options.impl, C.CString(name), (C.int64_t)(value)))
}

func (options KVStoreOptions) SetUInt(name string, value uint64) error {
	return ConvertStatus(C.turtlekv_kv_store_options_set_uint64(options.impl, C.CString(name), (C.uint64_t)(value)))
}

func (options KVStoreOptions) SetFloat(name string, value float64) error {
	return ConvertStatus(C.turtlekv_kv_store_options_set_double(options.impl, C.CString(name), (C.double)(value)))
}

func (options KVStoreOptions) SetBool(name string, value bool) error {
	return ConvertStatus(C.turtlekv_kv_store_options_set_bool(options.impl, C.CString(name), (C.bool)(value)))
}

func (options KVStoreOptions) GetBTreeMode() bool {
	b, err := options.GetBool("b_tree_mode")
	if err != nil {
		panic(err)
	}
	return b
}
func (options KVStoreOptions) SetBTreeMode(b bool) {
	err := options.SetBool("b_tree_mode", b)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetBufferLevelTrim() uint64 {
	n, err := options.GetUInt("buffer_level_trim")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetBufferLevelTrim(n uint64) {
	err := options.SetUInt("buffer_level_trim", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetCacheSizeBytes() uint64 {
	n, err := options.GetUInt("cache_size_bytes")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetCacheSizeBytes(n uint64) {
	err := options.SetUInt("cache_size_bytes", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetCacheSizeMB() uint64 {
	n, err := options.GetUInt("cache_size_mb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetCacheSizeMB(n uint64) {
	err := options.SetUInt("cache_size_mb", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetCheckpointDistance() uint64 {
	n, err := options.GetUInt("chi")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetCheckpointDistance(n uint64) {
	err := options.SetUInt("chi", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetCheckpointPipeline() bool {
	b, err := options.GetBool("checkpoint_pipeline")
	if err != nil {
		panic(err)
	}
	return b
}
func (options KVStoreOptions) SetCheckpointPipeline(b bool) {
	err := options.SetBool("checkpoint_pipeline", b)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetFilterBits() uint64 {
	n, err := options.GetUInt("filter_bits")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetFilterBits(n uint64) {
	err := options.SetUInt("filter_bits", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetInitialCapacityBytes() uint64 {
	n, err := options.GetUInt("initial_capacity_bytes")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetInitialCapacityBytes(n uint64) {
	err := options.SetUInt("initial_capacity_bytes", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetInitialCapacityGB() uint64 {
	n, err := options.GetUInt("initial_capacity_gb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetInitialCapacityGB(n uint64) {
	err := options.SetUInt("initial_capacity_gb", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetKeySizeHint() uint64 {
	n, err := options.GetUInt("key_size_hint")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetKeySizeHint(n uint64) {
	err := options.SetUInt("key_size_hint", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetLeafSizeBytes() uint64 {
	n, err := options.GetUInt("leaf_size")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetLeafSizeBytes(n uint64) {
	err := options.SetUInt("leaf_size", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetLeafSizeKB() uint64 {
	n, err := options.GetUInt("leaf_size_kb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetLeafSizeKB(n uint64) {
	err := options.SetUInt("leaf_size_kb", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetMaxCapacityBytes() uint64 {
	n, err := options.GetUInt("max_capacity_bytes")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetMaxCapacityBytes(n uint64) {
	err := options.SetUInt("max_capacity_bytes", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetMaxCapacityGB() uint64 {
	n, err := options.GetUInt("max_capacity_gb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetMaxCapacityGB(n uint64) {
	err := options.SetUInt("max_capacity_gb", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetMaxFlush() float64 {
	d, err := options.GetFloat("max_flush")
	if err != nil {
		panic(err)
	}
	return d
}
func (options KVStoreOptions) SetMaxFlush(d float64) {
	err := options.SetFloat("max_flush", d)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetMinFlush() float64 {
	d, err := options.GetFloat("min_flush")
	if err != nil {
		panic(err)
	}
	return d
}
func (options KVStoreOptions) SetMinFlush(d float64) {
	err := options.SetFloat("min_flush", d)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetNodeSizeBytes() uint64 {
	n, err := options.GetUInt("node_size")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetNodeSizeBytes(n uint64) {
	err := options.SetUInt("node_size", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetNodeSizeKB() uint64 {
	n, err := options.GetUInt("node_size_kb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetNodeSizeKB(n uint64) {
	err := options.SetUInt("node_size_kb", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetSizeTiered() bool {
	b, err := options.GetBool("size_tiered")
	if err != nil {
		panic(err)
	}
	return b
}
func (options KVStoreOptions) SetSizeTiered(b bool) {
	err := options.SetBool("size_tiered", b)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetValueSizeHint() uint64 {
	n, err := options.GetUInt("value_size_hint")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetValueSizeHint(n uint64) {
	err := options.SetUInt("value_size_hint", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetWALSizeBytes() uint64 {
	n, err := options.GetUInt("wal_size_bytes")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetWALSizeBytes(n uint64) {
	err := options.SetUInt("wal_size_bytes", n)
	if err != nil {
		panic(err)
	}
}
func (options KVStoreOptions) GetWALSizeMB() uint64 {
	n, err := options.GetUInt("wal_size_mb")
	if err != nil {
		panic(err)
	}
	return n
}
func (options KVStoreOptions) SetWALSizeMB(n uint64) {
	err := options.SetUInt("wal_size_mb", n)
	if err != nil {
		panic(err)
	}
}

// TODO [tastolfi 2026-10-21] fill in the rest of these.

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type Runtime struct {
	impl *C.turtlekv_Runtime
}

func NewRuntime(impl *C.turtlekv_Runtime) Runtime {
	var runtime Runtime
	runtime.impl = impl
	return runtime
}

func InitRuntime() (Runtime, error) {
	var result C.turtlekv_StatusOrRuntime
	result = C.turtlekv_runtime_initialize()
	if result.status.code == STATUS_CODE_OK {
		return NewRuntime(result.runtime), nil
	}
	return NewRuntime(nil), NewStatus(result.status.code).ToError()
}

func (runtime Runtime) Close() {
	if runtime.impl != nil {
		C.turtlekv_runtime_shutdown(runtime.impl)
		runtime.impl = nil
	}
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStore struct {
	impl *C.turtlekv_KVStore
}

func (runtime Runtime) CreateKVStore(file_path string, options KVStoreOptions, remove_existing bool) error {
	var result C.turtlekv_Status
	result = C.turtlekv_kv_store_create(runtime.impl, C.CString(file_path), options.impl, (C.bool)(remove_existing))
	return ConvertStatus(result)
}

func (runtime Runtime) OpenKVStore(file_path string, options KVStoreOptions) (KVStore, error) {
	var kv KVStore
	var result C.turtlekv_StatusOrKVStore

	kv.impl = nil
	result = C.turtlekv_kv_store_open(runtime.impl, C.CString(file_path), options.impl)
	if result.status.code == STATUS_CODE_OK {
		kv.impl = result.kv_store
		return kv, nil
	}
	return kv, NewStatus(result.status.code).ToError()
}

func (kv KVStore) Close() {
	if kv.impl != nil {
		C.turtlekv_kv_store_close(kv.impl)
		kv.impl = nil
	}
}

func (kv KVStore) GetCheckpointDistance() int {
	return (int)(C.turtlekv_kv_store_get_checkpoint_distance(kv.impl))
}

func (kv KVStore) SetCheckpointDistance(n_leaves int) {
	C.turtlekv_kv_store_set_checkpoint_distance(kv.impl, (C.size_t)(n_leaves))
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStoreContext struct {
	impl *C.turtlekv_KVStoreContext
}

func (kv KVStore) CreateContext() KVStoreContext {
	var ctx KVStoreContext
	ctx.impl = C.turtlekv_kv_store_create_context(kv.impl)
	return ctx
}

func (ctx KVStoreContext) Close() {
	if ctx.impl != nil {
		C.turtlekv_kv_store_destroy_context(ctx.impl)
		ctx.impl = nil
	}
}

type WriteOptions struct {
	sync   bool
	urgent bool
}

func (ctx KVStoreContext) Sync(edit_offset int64, write_opts WriteOptions) error {
	var c_opts C.turtlekv_WriteOptions
	c_opts.sync = (C.bool)(write_opts.sync)
	c_opts.urgent = (C.bool)(write_opts.urgent)

	return ConvertStatus(C.turtlekv_kv_store_sync(ctx.impl, (C.int64_t)(edit_offset), &c_opts))
}

func (ctx KVStoreContext) SyncAll(urgent bool) error {
	return ConvertStatus(C.turtlekv_kv_store_sync_all(ctx.impl, (C.bool)(urgent)))
}

func (ctx KVStoreContext) Put(key string, value string) error {
	return ConvertStatus(C.turtlekv_kv_store_put_simple(ctx.impl, C.CString(key), C.CString(value)))
}

func (ctx KVStoreContext) PutBytes(key []byte, value []byte) error {
	return ConvertStatus(C.turtlekv_kv_store_put(ctx.impl, BytesToKey(key), BytesToValue(value)))
}

func (ctx KVStoreContext) Remove(key string) error {
	return ConvertStatus(C.turtlekv_kv_store_remove_simple(ctx.impl, C.CString(key)))
}

func (ctx KVStoreContext) Get(key string) (string, error) {
	return ConvertStatusOrValueView(C.turtlekv_kv_store_get(ctx.impl, StrToKey(key)))
}

// Zero-copy version of Get.  The returned byte slice is valid until
// the next operation on the current KVStoreContext, or until
// KVStoreContext.Close() is called.
// .
func (ctx KVStoreContext) GetBytes(key []byte) ([]byte, error) {
	var result C.turtlekv_StatusOrValueView
	result = C.turtlekv_kv_store_get(ctx.impl, BytesToKey(key))
	if result.status.code != STATUS_CODE_OK {
		return nil, ConvertStatus(result.status)
	}
	return ValueToGoSlice(result.value), nil
}

// ==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

type KVStoreScanner struct {
	impl *C.turtlekv_KVStoreScanner
}

func (ctx KVStoreContext) CreateScanner(min_key string) (KVStoreScanner, error) {
	var scanner KVStoreScanner
	scanner.impl = C.turtlekv_kv_store_create_scanner(ctx.impl, StrToKey(min_key))

	status := C.turtlekv_kv_store_scanner_start(scanner.impl)
	if status.code != STATUS_CODE_OK {
		C.turtlekv_kv_store_destroy_scanner(scanner.impl)
		scanner.impl = nil
		return scanner, NewStatus(status.code).ToError()
	}

	return scanner, nil
}

func (scanner KVStoreScanner) Close() {
	if scanner.impl != nil {
		C.turtlekv_kv_store_destroy_scanner(scanner.impl)
		scanner.impl = nil
	}
}

type KVPair struct {
	Key   string
	Value string
}

func (kvp KVPair) Parts() (string, string) {
	return kvp.Key, kvp.Value
}

func (ctx KVStoreContext) Scan(min_key string) iter.Seq2[KVPair, error] {
	return func(yield func(KVPair, error) bool) {
		scanner, err := ctx.CreateScanner(min_key)
		defer scanner.Close()

		var item C.turtlekv_ItemView

		for {
			if err != nil {
				yield(KVPair{Key: "", Value: ""}, err)
				return
			}

			if !(bool)(C.turtlekv_kv_store_scanner_next(scanner.impl, &item)) {
				err = ConvertStatus(C.turtlekv_kv_store_scanner_status(scanner.impl))
				if err != nil {
					yield(KVPair{Key: "", Value: ""}, err)
				}
				return
			}

			if !yield(KVPair{Key: KeyToGoString(item.key), Value: ValueToGoString(item.value)}, nil) {
				return
			}
		}
	}
}
