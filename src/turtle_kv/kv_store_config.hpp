#pragma once
#define TURTLE_KV_KV_STORE_CONFIG_HPP

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/operators.hpp>
#include <batteries/require.hpp>
#include <batteries/stream_util.hpp>

#include <string_view>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief The configuration for a KVStore object.  This includes all parameters that have some
 * durable effect; this is in contrast to KVStoreRuntimeOptions, which only affects in the runtime
 * state of a KVStore.
 */
struct KVStoreConfig {
  using Self = KVStoreConfig;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  TreeOptions tree_options;
  u64 initial_capacity_bytes;
  u64 max_capacity_bytes;
  u64 change_log_size_bytes;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self with_default_values() noexcept;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Runtime options for a KVStore object.  This includes only the non-durable configuration
 * parameters (in contrast to KVStoreConfig), i.e. those which only affect the runtime state of a
 * KVStore.
 */
struct KVStoreRuntimeOptions {
  using Self = KVStoreRuntimeOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The initial value for checkpoint distance, in number of MemTables/leaf-page-sized
   * segments.
   */
  usize initial_checkpoint_distance;

  /** \brief If true, then create checkpoints in a multi-stage pipeline of background threads
   * instead of in the foreground (i.e., when a MemTable is finalized during a put operation).
   */
  bool use_threaded_checkpoint_pipeline;

  /** \brief The LLFS page cache size for the KVStore instance.
   */
  usize cache_size_bytes;

  /** \brief If this->use_threaded_checkpoint_pipeline is true, then this parameter controls how
   * many concurrent threads to use to sort/deduplicate the keys within a finalized MemTable.
   */
  usize memtable_compact_threads;

  /** \brief If true, then a single MemTable will be used for all deltas between checkpoints (i.e.,
   * MemTable size will scale with checkpoint distance); otherwise larger checkpoint distance
   * will produce more MemTables, and MemTables will be 1:1 with update batches.
   *
   * Support for big MemTables must be enabled via TURTLE_KV_BIG_MEM_TABLES == 1 (config.hpp)
   */
  bool use_big_mem_tables;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self with_default_values() noexcept;
};

BATT_OBJECT_PRINT_IMPL((inline),
                       KVStoreRuntimeOptions,
                       (initial_checkpoint_distance,
                        use_threaded_checkpoint_pipeline,
                        cache_size_bytes,
                        memtable_compact_threads))

/** \brief Sets the given name/value configuration parameter pair in the passed config objects.
 *
 * If `name` is known not to apply to either of `KVStoreConfig` or `KVStoreRuntimeOptions`, then
 * nullptr can be passed for these args.
 */
Status parse_config(std::string_view name,
                    std::string_view value,
                    KVStoreConfig* config,
                    KVStoreRuntimeOptions* runtime_options);

/** \brief Formats all config options as string, string pairs and returns the result.
 */
std::vector<std::pair<std::string, std::string>> config_to_string_list(
    const KVStoreConfig* config,
    const KVStoreRuntimeOptions* runtime_options);

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

class ConfigParam
{
 public:
  using TypedValue = std::variant<std::string, i64, u64, double, bool>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Getter
  {
   public:
    Getter(const Getter&) = delete;
    Getter& operator=(const Getter&) = delete;

    virtual ~Getter() = default;

    virtual TypedValue get(const KVStoreConfig* config,
                           const KVStoreRuntimeOptions* runtime_options) = 0;

   protected:
    Getter() = default;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Setter
  {
   public:
    Setter(const Setter&) = delete;
    Setter& operator=(const Setter&) = delete;

    virtual ~Setter() = default;

    virtual Status set(TypedValue value,
                       KVStoreConfig* config,
                       KVStoreRuntimeOptions* runtime_options) = 0;

   protected:
    Setter() = default;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Parser
  {
   public:
    Parser(const Parser&) = delete;
    Parser& operator=(const Parser&) = delete;

    virtual ~Parser() = default;

    virtual StatusOr<TypedValue> parse(std::istream& is) = 0;

    virtual StatusOr<TypedValue> parse_string(const std::string& str)
    {
      std::istringstream iss{str};
      return this->parse(iss);
    }

    virtual StatusOr<TypedValue> parse_string_view(const std::string_view& str)
    {
      return this->parse_string(std::string{str});
    }

   protected:
    Parser() = default;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class Formatter
  {
   public:
    Formatter(const Formatter&) = delete;
    Formatter& operator=(const Formatter&) = delete;

    virtual ~Formatter() = default;

    virtual Status format(std::ostream& os, TypedValue value) = 0;

   protected:
    Formatter() = default;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ConfigParam() noexcept
      : name_{}
      , getter_{nullptr}
      , setter_{nullptr}
      , parser_{nullptr}
      , formatter_{nullptr}
  {
  }

  explicit ConfigParam(std::string_view name,
                       Getter& getter,
                       Setter& setter,
                       Parser& parser,
                       Formatter& formatter) noexcept
      : name_{name}
      , getter_{&getter}
      , setter_{&setter}
      , parser_{&parser}
      , formatter_{&formatter}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view name() const
  {
    return this->name_;
  }

  TypedValue typed_value(const KVStoreConfig* config,
                         const KVStoreRuntimeOptions* runtime_options) const
  {
    return this->getter_->get(config, runtime_options);
  }

  StatusOr<std::string> string_value(const KVStoreConfig* config,
                                     const KVStoreRuntimeOptions* runtime_options) const
  {
    std::ostringstream oss;
    BATT_REQUIRE_OK(this->formatter_->format(oss, this->typed_value(config, runtime_options)));
    return {std::move(oss).str()};
  }

  Status set(TypedValue value, KVStoreConfig* config, KVStoreRuntimeOptions* runtime_options) const
  {
    return this->setter_->set(value, config, runtime_options);
  }

  StatusOr<TypedValue> parse(const std::string_view& str) const
  {
    return this->parser_->parse_string_view(str);
  }

  Status parse_and_set(const std::string_view& str,
                       KVStoreConfig* config,
                       KVStoreRuntimeOptions* runtime_options) const
  {
    StatusOr<TypedValue> status_or_value = this->parse(str);
    BATT_REQUIRE_OK(status_or_value) << BATT_INSPECT(str);
    return this->set(std::move(*status_or_value), config, runtime_options);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::string name_;
  Getter* getter_;
  Setter* setter_;
  Parser* parser_;
  Formatter* formatter_;
};

namespace config_params {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
class DefaultParser : public ConfigParam::Parser
{
 public:
  static DefaultParser& instance()
  {
    static DefaultParser instance_;
    return instance_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultParser() = default;

  StatusOr<ConfigParam::TypedValue> parse(std::istream& is) override
  {
    auto value = batt::make_default<T>();
    is >> value;
    if (!is.good() && !(is.eof() && !is.fail())) {
      return {batt::StatusCode::kInvalidArgument};
    }
    return {ConfigParam::TypedValue{std::move(value)}};
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class DefaultFormatter : public ConfigParam::Formatter
{
 public:
  static DefaultFormatter& instance()
  {
    static DefaultFormatter instance_;
    return instance_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  DefaultFormatter() = default;

  Status format(std::ostream& os, ConfigParam::TypedValue value) override
  {
    batt::case_of(value, [&os](auto&& value_case) {
      os << batt::make_printable(BATT_FORWARD(value_case));
    });

    return OkStatus();
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Returns `config`.
 */
inline const KVStoreConfig* select_object(batt::StaticType<const KVStoreConfig>,
                                          const KVStoreConfig* config,
                                          const KVStoreRuntimeOptions*)
{
  return config;
}

/** \brief Returns `config`.
 */
inline KVStoreConfig* select_object(batt::StaticType<KVStoreConfig>,
                                    KVStoreConfig* config,
                                    KVStoreRuntimeOptions*)
{
  return config;
}

/** \brief Returns `&config->tree_options`.
 */
inline const TreeOptions* select_object(batt::StaticType<const TreeOptions>,
                                        const KVStoreConfig* config,
                                        const KVStoreRuntimeOptions*)
{
  BATT_CHECK_NOT_NULLPTR(config);
  return &config->tree_options;
}

/** \brief Returns `&config->tree_options`.
 */
inline TreeOptions* select_object(batt::StaticType<TreeOptions>,
                                  KVStoreConfig* config,
                                  KVStoreRuntimeOptions*)
{
  BATT_CHECK_NOT_NULLPTR(config);
  return &config->tree_options;
}

/** \brief Returns `runtime_options`.
 */
inline const KVStoreRuntimeOptions* select_object(batt::StaticType<const KVStoreRuntimeOptions>,
                                                  const KVStoreConfig*,
                                                  const KVStoreRuntimeOptions* runtime_options)
{
  return runtime_options;
}

/** \brief Returns `&config->tree_options`.
 */
inline KVStoreRuntimeOptions* select_object(batt::StaticType<KVStoreRuntimeOptions>,
                                            KVStoreConfig*,
                                            KVStoreRuntimeOptions* runtime_options)
{
  return runtime_options;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename RecordT, typename ValueT, typename FieldT = ValueT>
class TypeFieldParam
    : public ConfigParam::Getter
    , public ConfigParam::Setter
{
 public:
  using FieldPointer = FieldT(RecordT::*);

  explicit TypeFieldParam(FieldPointer field_ptr) noexcept : field_ptr_{field_ptr}
  {
  }

  ConfigParam::TypedValue get(const KVStoreConfig* config,
                              const KVStoreRuntimeOptions* runtime_options) override
  {
    const RecordT* object =
        select_object(batt::StaticType<const RecordT>{}, config, runtime_options);

    BATT_CHECK_NOT_NULLPTR(object);

    return ConfigParam::TypedValue{object->*(this->field_ptr_)};
  }

  Status set(ConfigParam::TypedValue value,
             KVStoreConfig* config,
             KVStoreRuntimeOptions* runtime_options) override
  {
    RecordT* object = select_object(batt::StaticType<RecordT>{}, config, runtime_options);

    BATT_REQUIRE_NE(object, nullptr);

    return batt::case_of(
        value,
        //----- --- -- -  -  -   -
        [&](ValueT value_case) -> Status {
          object->*(this->field_ptr_) = BATT_FORWARD(value_case);
          return OkStatus();
        },
        //----- --- -- -  -  -   -
        [](auto&& /*wrong_type*/) -> Status {
          return batt::StatusCode::kInvalidArgument;
        });
  }

 private:
  FieldPointer field_ptr_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename RecordT, typename ValueT, typename ArgT = ValueT, typename ReturnT = RecordT&>
class TypePropertySetter : public ConfigParam::Setter
{
 public:
  using MemFnPointer = ReturnT (RecordT::*)(ArgT);

  explicit TypePropertySetter(MemFnPointer mem_fn_ptr) noexcept : mem_fn_ptr_{mem_fn_ptr}
  {
  }

  Status set(ConfigParam::TypedValue value,
             KVStoreConfig* config,
             KVStoreRuntimeOptions* runtime_options) override
  {
    RecordT* object = select_object(batt::StaticType<RecordT>{}, config, runtime_options);

    BATT_REQUIRE_NE(object, nullptr);

    return batt::case_of(
        value,
        //----- --- -- -  -  -   -
        [&](ValueT value_case) -> Status {
          (object->*(this->mem_fn_ptr_))(ArgT(value_case));
          return OkStatus();
        },
        //----- --- -- -  -  -   -
        [](auto&& /*wrong_type*/) -> Status {
          return batt::StatusCode::kInvalidArgument;
        });
  }

 private:
  MemFnPointer mem_fn_ptr_;
};

template <typename RecordT, typename ValueT, typename ReturnT = ValueT>
class TypePropertyGetter : public ConfigParam::Getter
{
 public:
  using MemFnPointer = ReturnT (RecordT::*)() const;

  explicit TypePropertyGetter(MemFnPointer mem_fn_ptr) noexcept : mem_fn_ptr_{mem_fn_ptr}
  {
  }

  ConfigParam::TypedValue get(const KVStoreConfig* config,
                              const KVStoreRuntimeOptions* runtime_options) override
  {
    const RecordT* object =
        select_object(batt::StaticType<const RecordT>{}, config, runtime_options);

    BATT_CHECK_NOT_NULLPTR(object);

    return ConfigParam::TypedValue{ValueT{(object->*(this->mem_fn_ptr_))()}};
  }

 private:
  MemFnPointer mem_fn_ptr_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
class ScaleConversion
    : public ConfigParam::Getter
    , public ConfigParam::Setter
{
 public:
  explicit ScaleConversion(ConfigParam::Getter& getter,
                           ConfigParam::Setter& setter,
                           T mul,
                           T div = 1) noexcept
      : getter_{getter}
      , setter_{setter}
      , mul_{mul}
      , div_{div}
  {
  }

  ConfigParam::TypedValue get(const KVStoreConfig* config,
                              const KVStoreRuntimeOptions* runtime_options) override
  {
    return batt::case_of(
        this->getter_.get(config, runtime_options),
        //----- --- -- -  -  -   -
        [this](T value_case) -> ConfigParam::TypedValue {
          return value_case * this->div_ / this->mul_;
        },
        //----- --- -- -  -  -   -
        [](auto&& /*wrong_type*/) -> ConfigParam::TypedValue {
          BATT_PANIC() << "Wrong type!";
          BATT_UNREACHABLE();
        });
  }

  Status set(ConfigParam::TypedValue value,
             KVStoreConfig* config,
             KVStoreRuntimeOptions* runtime_options) override
  {
    return batt::case_of(
        value,
        //----- --- -- -  -  -   -
        [this, config, runtime_options](T value_case) -> Status {
          return this->setter_.set(ConfigParam::TypedValue{value_case * this->mul_ / this->div_},
                                   config,
                                   runtime_options);
        },
        //----- --- -- -  -  -   -
        [](auto&& /*wrong_type*/) -> Status {
          return batt::StatusCode::kInvalidArgument;
        });
  }

 private:
  ConfigParam::Getter& getter_;
  ConfigParam::Setter& setter_;
  T mul_;
  T div_;
};

}  // namespace config_params
}  // namespace turtle_kv
