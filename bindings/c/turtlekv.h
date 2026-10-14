/*=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
 *
 * Part of the TurtleKV Project, under Apache License v2.0.
 * See https://www.apache.org/licenses/LICENSE-2.0 for license information.
 * SPDX short identifier: Apache-2.0
 *
 *+++++++++++-+-+--+----- --- -- -  -  -   -*/

#ifndef _TURTLEKV_H_
#define _TURTLEKV_H_

#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * Error Reporting & Handling.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief The set of common error code values reported by TurtleKV.
 */
typedef enum turtlekv_StatusCode_ {
  TURTLEKV_STATUS_CODE_OK = 0,
  TURTLEKV_STATUS_CODE_CANCELLED = 1,
  TURTLEKV_STATUS_CODE_UNKNOWN = 2,
  TURTLEKV_STATUS_CODE_INVALID_ARGUMENT = 3,
  TURTLEKV_STATUS_CODE_DEADLINE_EXCEEDED = 4,
  TURTLEKV_STATUS_CODE_NOT_FOUND = 5,
  TURTLEKV_STATUS_CODE_ALREADY_EXISTS = 6,
  TURTLEKV_STATUS_CODE_PERMISSION_DENIED = 7,
  TURTLEKV_STATUS_CODE_RESOURCE_EXHAUSTED = 8,
  TURTLEKV_STATUS_CODE_FAILED_PRECONDITION = 9,
  TURTLEKV_STATUS_CODE_ABORTED = 10,
  TURTLEKV_STATUS_CODE_OUT_OF_RANGE = 11,
  TURTLEKV_STATUS_CODE_UNIMPLEMENTED = 12,
  TURTLEKV_STATUS_CODE_INTERNAL = 13,
  TURTLEKV_STATUS_CODE_UNAVAILABLE = 14,
  TURTLEKV_STATUS_CODE_DATA_LOSS = 15,
  TURTLEKV_STATUS_CODE_UNAUTHENTICATED = 16,
  /* ...
   * This range reserved for future allocation of Abseil status codes.
   */
  TURTLEKV_STATUS_CODE_CLOSED = 100,
  TURTLEKV_STATUS_CODE_GRANT_UNAVAILABLE = 101,
  TURTLEKV_STATUS_CODE_LOOP_BREAK = 102,
  TURTLEKV_STATUS_CODE_END_OF_STREAM = 103,
  TURTLEKV_STATUS_CODE_CLOSED_BEFORE_END_OF_STREAM = 104,
  TURTLEKV_STATUS_CODE_GRANT_REVOKED = 105,
  TURTLEKV_STATUS_CODE_POKE = 106,

} turtlekv_StatusCode;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief A Status value indicating either success or some error condition.
 *
 * \see turtlekv_status_get_message
 * \see turtlekv_status_is_ok
 */
typedef struct turtlekv_Status_ {
  turtlekv_StatusCode code;
} turtlekv_Status;

/* Used internally to declare the Status-with-value types used by the rest of the API.
 */
#define TURTLEKV_DEFINE_STATUS_OR_TYPE(name, type, data)                                           \
  typedef struct turtlekv_StatusOr##name##_ {                                                      \
    turtlekv_Status status;                                                                        \
    type data;                                                                                     \
  } turtlekv_StatusOr##name

TURTLEKV_DEFINE_STATUS_OR_TYPE(Int64, int64_t, i);
TURTLEKV_DEFINE_STATUS_OR_TYPE(Uint64, uint64_t, n);
TURTLEKV_DEFINE_STATUS_OR_TYPE(Double, double, v);
TURTLEKV_DEFINE_STATUS_OR_TYPE(Bool, bool, b);

typedef turtlekv_StatusOrUint64 turtlekv_StatusOrUInt64;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief The size to allocate for a buffer to receive turtlekv_Status message strings.
 */
#define TURTLEKV_STATUS_MESSAGE_MAX_SIZE 256

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief Writes a human-readable message, null-terminated string for the passed status into the
 * passed buffer.
 *
 * If the buffer is too small, returns a status with code TURTLEKV_STATUS_CODE_OUT_OF_RANGE; in
 * this case, the `n` data member is set to the required buffer size so the caller can allocate
 * and try again.
 */
turtlekv_StatusOrUInt64 turtlekv_status_get_message(turtlekv_Status status,
                                                    char* buffer,
                                                    size_t buffer_size);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief Returns true iff the passed status indicates success (no error).
 */
bool turtlekv_status_is_ok(turtlekv_Status status);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * Global initialize/shutdown.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

typedef struct turtlekv_Runtime_ turtlekv_Runtime;

TURTLEKV_DEFINE_STATUS_OR_TYPE(Runtime, turtlekv_Runtime*, runtime);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrRuntime turtlekv_runtime_initialize();

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_shutdown(turtlekv_Runtime* runtime);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * Logging.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

typedef struct turtlekv_LogEvent_ {
  const char* level;
  const char* filename;
  const char* basename;
  int line;
  time_t timestamp;
  long time_usec;
  const char* message;
  size_t message_size;
} turtlekv_LogEvent;

typedef void turtlekv_LogHandlerSignature(void*, turtlekv_LogEvent);
typedef turtlekv_LogHandlerSignature* turtlekv_LogHandler;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
typedef struct turtlekv_LogHandlerAttachment_ {
  size_t id;
} turtlekv_LogHandlerAttachment;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_LogHandlerAttachment turtlekv_runtime_attach_log_handler(turtlekv_Runtime* runtime,
                                                                  turtlekv_LogHandler handler,
                                                                  void* handler_data);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_detach_log_handler(turtlekv_Runtime* runtime,
                                         turtlekv_LogHandlerAttachment attachment);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * Core Data Model.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

typedef enum turtlekv_ConfigOptionType_ {
  /* For options whose value is represented by turtlekv_StrView/const char*
   */
  TURTLEKV_CONFIG_OPTION_STRING,

  /* For options whose value is represented by uint64_t
   */
  TURTLEKV_CONFIG_OPTION_INT64,

  /* For options whose value is represented by int64_t
   */
  TURTLEKV_CONFIG_OPTION_UINT64,

  /* For options whose value is represented by double
   */
  TURTLEKV_CONFIG_OPTION_DOUBLE,

  /* For options whose value is represented by bool
   */
  TURTLEKV_CONFIG_OPTION_BOOL,

} turtlekv_ConfigOptionType;

TURTLEKV_DEFINE_STATUS_OR_TYPE(ConfigOptionType, turtlekv_ConfigOptionType, type_);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief A non-owning byte string.
 */
typedef struct turtlekv_StrView_ {
  const char* data;
  size_t size;
} turtlekv_StrView;

typedef turtlekv_StrView turtlekv_KeyView;

TURTLEKV_DEFINE_STATUS_OR_TYPE(StrView, turtlekv_StrView, str);
TURTLEKV_DEFINE_STATUS_OR_TYPE(KeyView, turtlekv_KeyView, key);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief The kind of value; regular byte-string values have kind TURTLEKV_VALUE_KIND_PUT;
 * tombstones have type TURTLEKV_VALUE_KIND_DELETE.
 */
typedef enum turtlekv_ValueKind_ {
  TURTLEKV_VALUE_KIND_PUT = 0,
  TURTLEKV_VALUE_KIND_DELETE = 1,
  TURTLEKV_VALUE_KIND_REMOVE = TURTLEKV_VALUE_KIND_DELETE,
} turtlekv_ValueKind;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief A non-owning value; if kind is TURTLEKV_VALUE_KIND_PUT, then data and size point to a
 * byte string; otherwise (if kind is TURTLEKV_VALUE_KIND_DELETE), these fields are not used.
 */
typedef struct turtlekv_ValueView_ {
  turtlekv_ValueKind kind;
  const char* data;
  size_t size;
} turtlekv_ValueView;

TURTLEKV_DEFINE_STATUS_OR_TYPE(ValueView, turtlekv_ValueView, value);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief A key/value pair.
 */
typedef struct turtlekv_ItemView_ {
  turtlekv_KeyView key;
  turtlekv_ValueView value;
} turtlekv_ItemView;

TURTLEKV_DEFINE_STATUS_OR_TYPE(ItemView, turtlekv_ItemView, item);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * KVStore Configuration.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief Opaque type representing all possible options.
 */
typedef struct turtlekv_KVStoreOptions_ turtlekv_KVStoreOptions;

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
/** \brief Creates and returns a new options object with default values.
 *
 * Must be destroyed by turtlekv_kv_store_options_destroy when no longer needed.
 */
turtlekv_KVStoreOptions* turtlekv_kv_store_options_create_default();

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_options_destroy(turtlekv_KVStoreOptions* options);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrConfigOptionType turtlekv_get_config_option_type(const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
size_t turtlekv_kv_store_get_num_options(const turtlekv_KVStoreOptions* options);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
const char* turtlekv_kv_store_options_name_for_index(const turtlekv_KVStoreOptions* options,
                                                     size_t index);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_options_index_for_name(
    const turtlekv_KVStoreOptions* options,
    const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_format_option(const turtlekv_KVStoreOptions* options,
                                                        size_t index,
                                                        char* value_buffer,
                                                        size_t value_buffer_size);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_parse(turtlekv_KVStoreOptions* options,
                                                const char* name,
                                                const char* value);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_int64(turtlekv_KVStoreOptions* options,
                                                    const char* name,
                                                    int64_t value);

turtlekv_StatusOrInt64 turtlekv_kv_store_options_get_int64(const turtlekv_KVStoreOptions* options,
                                                           const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_uint64(turtlekv_KVStoreOptions* options,
                                                     const char* name,
                                                     uint64_t value);

turtlekv_StatusOrUInt64 turtlekv_kv_store_options_get_uint64(const turtlekv_KVStoreOptions* options,
                                                             const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_bool(turtlekv_KVStoreOptions* options,
                                                   const char* name,
                                                   bool value);

turtlekv_StatusOrBool turtlekv_kv_store_options_get_bool(const turtlekv_KVStoreOptions* options,
                                                         const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_double(turtlekv_KVStoreOptions* options,
                                                     const char* name,
                                                     double value);

turtlekv_StatusOrDouble turtlekv_kv_store_options_get_double(const turtlekv_KVStoreOptions* options,
                                                             const char* name);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_str(turtlekv_KVStoreOptions* options,
                                                  const char* name,
                                                  const char* value);

turtlekv_Status turtlekv_kv_store_options_set_str_view(turtlekv_KVStoreOptions* options,
                                                       const char* name,
                                                       turtlekv_StrView value);

turtlekv_StatusOrStrView turtlekv_kv_store_options_get_str_view(
    const turtlekv_KVStoreOptions* options,
    const char* name);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 *
 * KVStore.
 *
 =#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

typedef struct turtlekv_KVStore_ turtlekv_KVStore;
typedef struct turtlekv_KVStoreThreadContext_ turtlekv_KVStoreThreadContext;
typedef struct turtlekv_KVStoreScanner_ turtlekv_KVStoreScanner;

TURTLEKV_DEFINE_STATUS_OR_TYPE(KVStore, turtlekv_KVStore*, kv_store);

turtlekv_Status turtlekv_kv_store_create(turtlekv_Runtime* runtime,
                                         const char* file_path,
                                         const turtlekv_KVStoreOptions* options,
                                         bool remove_existing);

turtlekv_StatusOrKVStore turtlekv_kv_store_open(turtlekv_Runtime* runtime,
                                                const char* file_path,
                                                const turtlekv_KVStoreOptions* options);

void turtlekv_kv_store_close(turtlekv_KVStore* kv_store);

size_t turtlekv_kv_store_get_checkpoint_distance(const turtlekv_KVStore* kv_store);

void turtlekv_kv_store_set_checkpoint_distance(const turtlekv_KVStore* kv_store,
                                               size_t checkpoint_distance);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
typedef struct turtlekv_WriteOptions_ {
  bool sync;
  bool urgent;
} turtlekv_WriteOptions;

turtlekv_Status turtlekv_kv_store_put(turtlekv_KVStore* kv_store,
                                      turtlekv_KeyView key,
                                      turtlekv_ValueView value);

turtlekv_Status turtlekv_kv_store_put_opt(turtlekv_KVStore* kv_store,
                                          turtlekv_KeyView key,
                                          turtlekv_ValueView value,
                                          const turtlekv_WriteOptions* opt);

turtlekv_Status turtlekv_kv_store_remove(turtlekv_KVStore* kv_store, turtlekv_KeyView key);

turtlekv_Status turtlekv_kv_store_remove_opt(turtlekv_KVStore* kv_store,
                                             turtlekv_KeyView key,
                                             const turtlekv_WriteOptions* opt);

turtlekv_StatusOrValueView turtlekv_kv_store_get(turtlekv_KVStore* kv_store, turtlekv_KeyView key);

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 *
 * Simple KVStore API -- uses null-terminated C-strings instead of KeyView/ValueView.
 *
 *==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -*/

turtlekv_Status turtlekv_kv_store_put_simple(turtlekv_KVStore* kv_store,
                                             const char* key,
                                             const char* value);

turtlekv_Status turtlekv_kv_store_remove_simple(turtlekv_KVStore* kv_store, const char* key);

const char* turtlekv_kv_store_get_simple(turtlekv_KVStore* kv_store, const char* key);

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------*/

#undef TURTLEKV_DEFINE_STATUS_OR_TYPE

#ifdef __cplusplus
}
#endif

#endif /* _TURTLEKV_H_ */
