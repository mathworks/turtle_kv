#include <turtle_kv/kv_store_config.hpp>
//
#include <turtle_kv/config.hpp>

#include <functional>
#include <unordered_map>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto KVStoreConfig::with_default_values() noexcept -> Self
{
  return Self{
      .tree_options = TreeOptions::with_default_values(),
      .initial_capacity_bytes = 512 * kMiB,
      .max_capacity_bytes = 4 * kGiB,
      .change_log_size_bytes = 256 * kMiB,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto KVStoreRuntimeOptions::with_default_values() noexcept -> Self
{
  return Self{
      .initial_checkpoint_distance = 1,
      .use_threaded_checkpoint_pipeline = true,
      .cache_size_bytes = 4 * kGiB,
  };
}

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
namespace {

using config_params::DefaultFormatter;
using config_params::DefaultParser;
using config_params::ScaleConversion;
using config_params::TypeFieldParam;
using config_params::TypePropertyGetter;
using config_params::TypePropertySetter;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<ConfigParam> initialize_config_params()
{
  std::vector<ConfigParam> params;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // KVStoreConfig

  //----- --- -- -  -  -   -
  // initial_capacity_bytes / initial_capacity_gb
  //
  static TypeFieldParam<KVStoreConfig, u64> initial_capacity_bytes_field{
      &KVStoreConfig::initial_capacity_bytes};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "initial_capacity_bytes",
      initial_capacity_bytes_field,
      initial_capacity_bytes_field,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> initial_capacity_gb_get_set{initial_capacity_bytes_field,
                                                          initial_capacity_bytes_field,
                                                          kGiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "initial_capacity_gb",
      initial_capacity_gb_get_set,
      initial_capacity_gb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // initial_capacity_bytes / capacity_gb
  //
  static TypeFieldParam<KVStoreConfig, u64> max_capacity_bytes_field{
      &KVStoreConfig::max_capacity_bytes};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "max_capacity_bytes",
      max_capacity_bytes_field,
      max_capacity_bytes_field,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> max_capacity_gb_get_set{max_capacity_bytes_field,
                                                      max_capacity_bytes_field,
                                                      kGiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "max_capacity_gb",
      max_capacity_gb_get_set,
      max_capacity_gb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // wal_size_mb
  //
  static TypeFieldParam<KVStoreConfig, u64> wal_size_bytes_field{
      &KVStoreConfig::change_log_size_bytes};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "wal_size_bytes",
      wal_size_bytes_field,
      wal_size_bytes_field,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> wal_size_mb_get_set{wal_size_bytes_field, wal_size_bytes_field, kMiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "wal_size_mb",
      wal_size_mb_get_set,
      wal_size_mb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // TreeOptions

  //----- --- -- -  -  -   -
  // node_size / node_size_kb
  //
  static TypePropertyGetter<TreeOptions, u64, llfs::PageSize> node_size_getter{
      &TreeOptions::node_size};
  static TypePropertySetter<TreeOptions, u64> node_size_setter{&TreeOptions::set_node_size};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "node_size",
      node_size_getter,
      node_size_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> node_size_kb_get_set{node_size_getter, node_size_setter, kKiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "node_size_kb",
      node_size_kb_get_set,
      node_size_kb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // leaf_size / leaf_size_kb
  //
  static TypePropertyGetter<TreeOptions, u64, llfs::PageSize> leaf_size_getter{
      &TreeOptions::leaf_size};
  static TypePropertySetter<TreeOptions, u64> leaf_size_setter{&TreeOptions::set_leaf_size};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "leaf_size",
      leaf_size_getter,
      leaf_size_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> leaf_size_kb_get_set{leaf_size_getter, leaf_size_setter, kKiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "leaf_size_kb",
      leaf_size_kb_get_set,
      leaf_size_kb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // min_flush
  //
  static TypePropertyGetter<TreeOptions, double> min_flush_getter{&TreeOptions::min_flush_factor};
  static TypePropertySetter<TreeOptions, double> min_flush_setter{
      &TreeOptions::set_min_flush_factor};

  params.push_back(ConfigParam{
      batt::StaticType<double>{},
      "min_flush",
      min_flush_getter,
      min_flush_setter,
      DefaultParser<double>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // max_flush
  //
  static TypePropertyGetter<TreeOptions, double> max_flush_getter{&TreeOptions::max_flush_factor};
  static TypePropertySetter<TreeOptions, double> max_flush_setter{
      &TreeOptions::set_max_flush_factor};

  params.push_back(ConfigParam{
      batt::StaticType<double>{},
      "max_flush",
      max_flush_getter,
      max_flush_setter,
      DefaultParser<double>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // buffer_level_trim
  //
  static TypePropertyGetter<TreeOptions, u64, u16> buffer_level_trim_getter{
      &TreeOptions::buffer_level_trim};
  static TypePropertySetter<TreeOptions, u64, u16> buffer_level_trim_setter{
      &TreeOptions::set_buffer_level_trim};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "buffer_level_trim",
      buffer_level_trim_getter,
      buffer_level_trim_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // key_size_hint
  //
  static TypePropertyGetter<TreeOptions, u64, u32> key_size_hint_getter{
      &TreeOptions::key_size_hint};
  static TypePropertySetter<TreeOptions, u64, u32> key_size_hint_setter{
      &TreeOptions::set_key_size_hint};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "key_size_hint",
      key_size_hint_getter,
      key_size_hint_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // value_size_hint
  //
  static TypePropertyGetter<TreeOptions, u64, u32> value_size_hint_getter{
      &TreeOptions::value_size_hint};
  static TypePropertySetter<TreeOptions, u64, u32> value_size_hint_setter{
      &TreeOptions::set_value_size_hint};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "value_size_hint",
      value_size_hint_getter,
      value_size_hint_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // filter_bits
  //
  static TypePropertyGetter<TreeOptions, u64, usize> filter_bits_getter{
      &TreeOptions::filter_bits_per_key};
  static TypePropertySetter<TreeOptions, u64, Optional<u16>> filter_bits_setter{
      &TreeOptions::set_filter_bits_per_key};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "filter_bits",
      filter_bits_getter,
      filter_bits_setter,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // size_tiered
  //
  static TypePropertyGetter<TreeOptions, bool, IsSizeTiered> size_tiered_getter{
      &TreeOptions::is_size_tiered};
  static TypePropertySetter<TreeOptions, bool> size_tiered_setter{&TreeOptions::set_size_tiered};

  params.push_back(ConfigParam{
      batt::StaticType<bool>{},
      "size_tiered",
      size_tiered_getter,
      size_tiered_setter,
      DefaultParser<bool>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // b-tree mode
  //
  static TypePropertyGetter<TreeOptions, bool> b_tree_mode_getter{
      &TreeOptions::is_b_tree_mode_enabled};
  static TypePropertySetter<TreeOptions, bool> b_tree_mode_setter{
      &TreeOptions::set_b_tree_mode_enabled};

  params.push_back(ConfigParam{
      batt::StaticType<bool>{},
      "b_tree_mode",
      b_tree_mode_getter,
      b_tree_mode_setter,
      DefaultParser<bool>::instance(),
      DefaultFormatter::instance(),
  });

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // KVStoreRuntimeOptions

  //----- --- -- -  -  -   -
  // cache_size_bytes / cache_size_mb
  //
  static TypeFieldParam<KVStoreRuntimeOptions, u64, usize> cache_size_bytes_field{
      &KVStoreRuntimeOptions::cache_size_bytes};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "cache_size_bytes",
      cache_size_bytes_field,
      cache_size_bytes_field,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  static ScaleConversion<u64> cache_size_mb_get_set{cache_size_bytes_field,
                                                    cache_size_bytes_field,
                                                    kMiB};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "cache_size_mb",
      cache_size_mb_get_set,
      cache_size_mb_get_set,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // chi / initial_checkpoint_distance
  //
  static TypeFieldParam<KVStoreRuntimeOptions, u64, usize> initial_checkpoint_distance_field{
      &KVStoreRuntimeOptions::initial_checkpoint_distance};

  params.push_back(ConfigParam{
      batt::StaticType<u64>{},
      "chi",
      initial_checkpoint_distance_field,
      initial_checkpoint_distance_field,
      DefaultParser<u64>::instance(),
      DefaultFormatter::instance(),
  });

  //----- --- -- -  -  -   -
  // checkpoint_pipeline
  //
  static TypeFieldParam<KVStoreRuntimeOptions, bool> use_threaded_checkpoint_pipeline_field{
      &KVStoreRuntimeOptions::use_threaded_checkpoint_pipeline};

  params.push_back(ConfigParam{
      batt::StaticType<bool>{},
      "checkpoint_pipeline",
      use_threaded_checkpoint_pipeline_field,
      use_threaded_checkpoint_pipeline_field,
      DefaultParser<bool>::instance(),
      DefaultFormatter::instance(),
  });

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Done!
  //
  return params;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::vector<ConfigParam>& get_config_params()
{
  static const std::vector<ConfigParam> cache_ = initialize_config_params();
  return cache_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unordered_map<std::string_view, ConfigParam> initialize_config_param_map()
{
  std::unordered_map<std::string_view, ConfigParam> result;

  for (const ConfigParam& param : get_config_params()) {
    result[param.name()] = param;
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::unordered_map<std::string_view, ConfigParam>& config_param_map()
{
  static const std::unordered_map<std::string_view, ConfigParam> cache_ =
      initialize_config_param_map();

  return cache_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::vector<std::pair<std::string, const ConfigParam*>>& sorted_params()
{
  // We copy the param names to std::string so they can be accessed as c-strings for the extern "C"
  // api.
  //
  static const std::vector<std::pair<std::string, const ConfigParam*>> cache_ = [] {
    std::vector<std::pair<std::string, const ConfigParam*>> names;

    for (const auto& [name, param] : config_param_map()) {
      names.push_back(std::make_pair(std::string{name}, &param));
    }

    std::sort(names.begin(), names.end());

    return names;
  }();

  return cache_;
}

}  // namespace
// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status parse_config(std::string_view name [[maybe_unused]],
                    std::string_view value [[maybe_unused]],
                    KVStoreConfig* config [[maybe_unused]],
                    KVStoreRuntimeOptions* runtime_options [[maybe_unused]])
{
  VLOG(1) << BATT_INSPECT_STR(name) << BATT_INSPECT_STR(value);

  auto iter = config_param_map().find(name);
  if (iter == config_param_map().end()) {
    LOG(WARNING) << " -- name not found in config_param_map()";
    return batt::StatusCode::kInvalidArgument;
  }

  Status status = iter->second.parse_and_set(value, config, runtime_options);
  if (!status.ok()) {
    LOG(WARNING) << " -- parse error: " << BATT_INSPECT(name) << BATT_INSPECT(value);
  }

  return status;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize num_config_params()
{
  return config_param_map().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const ConfigParam* get_config_param_for_index(usize i)
{
  const std::vector<std::pair<std::string, const ConfigParam*>>& names = sorted_params();
  if (i < names.size()) {
    return names[i].second;
  }
  return nullptr;
}

namespace {

std::string_view get_param_name(const std::string_view& s) noexcept
{
  return s;
}

std::string_view get_param_name(const std::pair<std::string, const ConfigParam*>& p) noexcept
{
  return p.first;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<usize> get_index_for_config_param_name(const std::string_view& name)
{
  const std::vector<std::pair<std::string, const ConfigParam*>>& names = sorted_params();
  auto iter = std::lower_bound(names.begin(), names.end(), name, [](const auto& l, const auto& r) {
    return get_param_name(l) < get_param_name(r);
  });
  if (iter == names.end() || iter->first != name) {
    return {batt::StatusCode::kNotFound};
  }
  return std::distance(names.begin(), iter);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<std::pair<std::string, std::string>> config_to_string_list(
    const KVStoreConfig* config,
    const KVStoreRuntimeOptions* runtime_options)
{
  std::vector<std::pair<std::string, std::string>> result;

  for (const ConfigParam& param : get_config_params()) {
    std::string name{param.name()};
    std::string value = BATT_OK_RESULT_OR_PANIC(param.string_value(config, runtime_options));
    result.emplace_back(std::move(name), std::move(value));
  }

  std::sort(result.begin(), result.end());

  return result;
}

}  // namespace turtle_kv
