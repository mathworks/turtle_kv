#include <turtle_kv/kv_store_config.hpp>
//
#include <turtle_kv/kv_store_config.hpp>

#include <turtle_kv/config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace turtle_kv;
using namespace turtle_kv::int_types;
using namespace batt::constants;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Default values for all config structs
//
TEST(KVStoreConfigTest, DefaultValues_AllStructs)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  EXPECT_EQ(config.initial_capacity_bytes, 512 * kMiB);
  EXPECT_EQ(config.max_capacity_bytes, 4 * kGiB);
  EXPECT_EQ(config.change_log_size_bytes, 256 * kMiB);

  KVStoreRuntimeOptions opts = KVStoreRuntimeOptions::with_default_values();
  EXPECT_EQ(opts.initial_checkpoint_distance, 1u);
  EXPECT_TRUE(opts.use_threaded_checkpoint_pipeline);
  EXPECT_EQ(opts.cache_size_bytes, 4 * kGiB);

  KVStoreWriteOptions write_opts;
  EXPECT_FALSE(write_opts.sync);
  EXPECT_FALSE(write_opts.urgent_sync);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// parse_config: KVStoreConfig fields
//
TEST(KVStoreConfigTest, ParseConfig_KVStoreConfigFields)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  ASSERT_TRUE(parse_config("initial_capacity_bytes", "1073741824", &config, &runtime).ok());
  EXPECT_EQ(config.initial_capacity_bytes, u64{1073741824});

  ASSERT_TRUE(parse_config("initial_capacity_gb", "2", &config, &runtime).ok());
  EXPECT_EQ(config.initial_capacity_bytes, 2 * kGiB);

  ASSERT_TRUE(parse_config("max_capacity_bytes", "8589934592", &config, &runtime).ok());
  EXPECT_EQ(config.max_capacity_bytes, 8 * kGiB);

  ASSERT_TRUE(parse_config("max_capacity_gb", "8", &config, &runtime).ok());
  EXPECT_EQ(config.max_capacity_bytes, 8 * kGiB);

  ASSERT_TRUE(parse_config("wal_size_bytes", "134217728", &config, &runtime).ok());
  EXPECT_EQ(config.change_log_size_bytes, 128 * kMiB);

  ASSERT_TRUE(parse_config("wal_size_mb", "128", &config, &runtime).ok());
  EXPECT_EQ(config.change_log_size_bytes, 128 * kMiB);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// parse_config: TreeOptions fields
//
TEST(KVStoreConfigTest, ParseConfig_TreeOptionsFields)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  ASSERT_TRUE(parse_config("node_size", "8192", &config, &runtime).ok());
  EXPECT_EQ(u64{config.tree_options.node_size()}, u64{8192});

  ASSERT_TRUE(parse_config("node_size_kb", "8", &config, &runtime).ok());
  EXPECT_EQ(u64{config.tree_options.node_size()}, 8 * kKiB);

  ASSERT_TRUE(parse_config("leaf_size", "4194304", &config, &runtime).ok());
  EXPECT_EQ(u64{config.tree_options.leaf_size()}, 4 * kMiB);

  ASSERT_TRUE(parse_config("leaf_size_kb", "4096", &config, &runtime).ok());
  EXPECT_EQ(u64{config.tree_options.leaf_size()}, 4 * kMiB);

  ASSERT_TRUE(parse_config("min_flush", "0.25", &config, &runtime).ok());
  EXPECT_DOUBLE_EQ(config.tree_options.min_flush_factor(), 0.25);

  ASSERT_TRUE(parse_config("max_flush", "0.75", &config, &runtime).ok());
  EXPECT_DOUBLE_EQ(config.tree_options.max_flush_factor(), 0.75);

  ASSERT_TRUE(parse_config("buffer_level_trim", "5", &config, &runtime).ok());
  EXPECT_EQ(config.tree_options.buffer_level_trim(), 5u);

  ASSERT_TRUE(parse_config("key_size_hint", "48", &config, &runtime).ok());
  EXPECT_EQ(config.tree_options.key_size_hint(), 48u);

  ASSERT_TRUE(parse_config("value_size_hint", "256", &config, &runtime).ok());
  EXPECT_EQ(config.tree_options.value_size_hint(), 256u);

  ASSERT_TRUE(parse_config("filter_bits", "16", &config, &runtime).ok());
  EXPECT_EQ(usize{config.tree_options.filter_bits_per_key()}, 16u);

  ASSERT_TRUE(parse_config("size_tiered", "1", &config, &runtime).ok());
  EXPECT_TRUE(bool{config.tree_options.is_size_tiered()});

  ASSERT_TRUE(parse_config("b_tree_mode", "1", &config, &runtime).ok());
  EXPECT_TRUE(config.tree_options.is_b_tree_mode_enabled());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// parse_config: KVStoreRuntimeOptions fields
//
TEST(KVStoreConfigTest, ParseConfig_RuntimeOptionsFields)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  ASSERT_TRUE(parse_config("cache_size_bytes", "2147483648", &config, &runtime).ok());
  EXPECT_EQ(runtime.cache_size_bytes, 2 * kGiB);

  ASSERT_TRUE(parse_config("cache_size_mb", "512", &config, &runtime).ok());
  EXPECT_EQ(runtime.cache_size_bytes, 512 * kMiB);

  ASSERT_TRUE(parse_config("chi", "4", &config, &runtime).ok());
  EXPECT_EQ(runtime.initial_checkpoint_distance, 4u);

  ASSERT_TRUE(parse_config("checkpoint_pipeline", "0", &config, &runtime).ok());
  EXPECT_FALSE(runtime.use_threaded_checkpoint_pipeline);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// parse_config: error cases
//
TEST(KVStoreConfigTest, ParseConfig_ErrorCases)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  EXPECT_FALSE(parse_config("nonexistent_param", "42", &config, &runtime).ok());
  EXPECT_FALSE(parse_config("initial_capacity_bytes", "not_a_number", &config, &runtime).ok());
  EXPECT_FALSE(parse_config("min_flush", "xyz", &config, &runtime).ok());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// config_to_string_list: completeness, sorting, and default value correctness
//
TEST(KVStoreConfigTest, ConfigToStringList_ContentsAndOrdering)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  auto string_list = config_to_string_list(&config, &runtime);
  EXPECT_FALSE(string_list.empty());

  // Verify sorted order.
  for (usize i = 1; i < string_list.size(); ++i) {
    EXPECT_LE(string_list[i - 1].first, string_list[i].first);
  }

  // Build a map for value lookups.
  std::unordered_map<std::string, std::string> param_map;
  for (const auto& [name, value] : string_list) {
    param_map[name] = value;
  }

  // Verify all expected params are present.
  for (const auto& expected : {"initial_capacity_bytes", "max_capacity_bytes", "wal_size_bytes",
                                "cache_size_bytes", "chi", "checkpoint_pipeline", "node_size",
                                "leaf_size", "min_flush", "max_flush"}) {
    EXPECT_NE(param_map.find(expected), param_map.end()) << "Missing: " << expected;
  }

  // Verify default value serialization.
  EXPECT_EQ(param_map["initial_capacity_bytes"], std::to_string(512 * kMiB));
  EXPECT_EQ(param_map["max_capacity_bytes"], std::to_string(4 * kGiB));
  EXPECT_EQ(param_map["wal_size_bytes"], std::to_string(256 * kMiB));
  EXPECT_EQ(param_map["cache_size_bytes"], std::to_string(4 * kGiB));
  EXPECT_EQ(param_map["chi"], "1");
  EXPECT_EQ(param_map["checkpoint_pipeline"], "1");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Round-trip: parse_config -> config_to_string_list
//
TEST(KVStoreConfigTest, RoundTrip_ParseThenSerialize)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  ASSERT_TRUE(parse_config("initial_capacity_gb", "1", &config, &runtime).ok());
  ASSERT_TRUE(parse_config("max_capacity_gb", "16", &config, &runtime).ok());
  ASSERT_TRUE(parse_config("wal_size_mb", "64", &config, &runtime).ok());
  ASSERT_TRUE(parse_config("cache_size_mb", "256", &config, &runtime).ok());
  ASSERT_TRUE(parse_config("chi", "8", &config, &runtime).ok());

  auto string_list = config_to_string_list(&config, &runtime);

  std::unordered_map<std::string, std::string> param_map;
  for (const auto& [name, value] : string_list) {
    param_map[name] = value;
  }

  EXPECT_EQ(param_map["initial_capacity_bytes"], std::to_string(1 * kGiB));
  EXPECT_EQ(param_map["initial_capacity_gb"], "1");
  EXPECT_EQ(param_map["max_capacity_bytes"], std::to_string(16 * kGiB));
  EXPECT_EQ(param_map["max_capacity_gb"], "16");
  EXPECT_EQ(param_map["wal_size_bytes"], std::to_string(64 * kMiB));
  EXPECT_EQ(param_map["wal_size_mb"], "64");
  EXPECT_EQ(param_map["cache_size_bytes"], std::to_string(256 * kMiB));
  EXPECT_EQ(param_map["cache_size_mb"], "256");
  EXPECT_EQ(param_map["chi"], "8");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// DefaultParser: u64, double, bool, and failure cases
//
TEST(KVStoreConfigTest, DefaultParser_ParseAllTypesAndFailures)
{
  {
    auto& parser = config_params::DefaultParser<u64>::instance();
    StatusOr<ConfigParam::TypedValue> result = parser.parse_string_view("12345");
    ASSERT_TRUE(result.ok()) << BATT_INSPECT(result.status());
    EXPECT_EQ(std::get<u64>(*result), u64{12345});

    EXPECT_FALSE(parser.parse_string_view("abc").ok());
  }
  {
    auto& parser = config_params::DefaultParser<double>::instance();
    StatusOr<ConfigParam::TypedValue> result = parser.parse_string_view("3.14");
    ASSERT_TRUE(result.ok()) << BATT_INSPECT(result.status());
    EXPECT_DOUBLE_EQ(std::get<double>(*result), 3.14);

    EXPECT_FALSE(parser.parse_string_view("not_a_double").ok());
  }
  {
    auto& parser = config_params::DefaultParser<bool>::instance();

    StatusOr<ConfigParam::TypedValue> result_true = parser.parse_string_view("1");
    ASSERT_TRUE(result_true.ok()) << BATT_INSPECT(result_true.status());
    EXPECT_TRUE(std::get<bool>(*result_true));

    StatusOr<ConfigParam::TypedValue> result_false = parser.parse_string_view("0");
    ASSERT_TRUE(result_false.ok()) << BATT_INSPECT(result_false.status());
    EXPECT_FALSE(std::get<bool>(*result_false));
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// DefaultFormatter
//
TEST(KVStoreConfigTest, DefaultFormatter_FormatAllTypes)
{
  auto& formatter = config_params::DefaultFormatter::instance();

  {
    std::ostringstream oss;
    ASSERT_TRUE(formatter.format(oss, ConfigParam::TypedValue{u64{42}}).ok());
    EXPECT_EQ(oss.str(), "42");
  }
  {
    std::ostringstream oss;
    ASSERT_TRUE(formatter.format(oss, ConfigParam::TypedValue{true}).ok());
    EXPECT_EQ(oss.str(), "1");
  }
  {
    std::ostringstream oss;
    ASSERT_TRUE(formatter.format(oss, ConfigParam::TypedValue{false}).ok());
    EXPECT_EQ(oss.str(), "0");
  }
  {
    std::ostringstream oss;
    ASSERT_TRUE(formatter.format(oss, ConfigParam::TypedValue{2.5}).ok());
    EXPECT_EQ(oss.str(), "2.5");
  }
  {
    std::ostringstream oss;
    ASSERT_TRUE(formatter.format(oss, ConfigParam::TypedValue{std::string{"hello"}}).ok());
    EXPECT_EQ(oss.str(), "\"hello\"");
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// select_object overloads
//
TEST(KVStoreConfigTest, SelectObject_AllOverloads)
{
  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  EXPECT_EQ(config_params::select_object(batt::StaticType<const KVStoreConfig>{}, &config, &runtime),
            &config);
  EXPECT_EQ(config_params::select_object(batt::StaticType<KVStoreConfig>{}, &config, &runtime),
            &config);

  EXPECT_EQ(config_params::select_object(batt::StaticType<const TreeOptions>{}, &config, &runtime),
            &config.tree_options);
  EXPECT_EQ(config_params::select_object(batt::StaticType<TreeOptions>{}, &config, &runtime),
            &config.tree_options);

  EXPECT_EQ(config_params::select_object(batt::StaticType<const KVStoreRuntimeOptions>{}, &config,
                                          &runtime),
            &runtime);
  EXPECT_EQ(
      config_params::select_object(batt::StaticType<KVStoreRuntimeOptions>{}, &config, &runtime),
      &runtime);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// TypeFieldParam: get, set, and wrong-type error
//
TEST(KVStoreConfigTest, TypeFieldParam_GetSetAndWrongType)
{
  config_params::TypeFieldParam<KVStoreConfig, u64> field{&KVStoreConfig::initial_capacity_bytes};

  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  ASSERT_TRUE(field.set(ConfigParam::TypedValue{u64{999}}, &config, &runtime).ok());
  EXPECT_EQ(std::get<u64>(field.get(&config, &runtime)), u64{999});

  EXPECT_FALSE(field.set(ConfigParam::TypedValue{std::string{"wrong"}}, &config, &runtime).ok());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// ScaleConversion: get scales down, set scales up, wrong type errors
//
TEST(KVStoreConfigTest, ScaleConversion_ScalingAndWrongType)
{
  config_params::TypeFieldParam<KVStoreConfig, u64> field{&KVStoreConfig::initial_capacity_bytes};
  config_params::ScaleConversion<u64> scaled{field, field, kGiB};

  KVStoreConfig config = KVStoreConfig::with_default_values();
  KVStoreRuntimeOptions runtime = KVStoreRuntimeOptions::with_default_values();

  // get scales down
  config.initial_capacity_bytes = 2 * kGiB;
  EXPECT_EQ(std::get<u64>(scaled.get(&config, &runtime)), u64{2});

  // set scales up
  ASSERT_TRUE(scaled.set(ConfigParam::TypedValue{u64{3}}, &config, &runtime).ok());
  EXPECT_EQ(config.initial_capacity_bytes, 3 * kGiB);

  // wrong type
  EXPECT_FALSE(scaled.set(ConfigParam::TypedValue{std::string{"wrong"}}, &config, &runtime).ok());
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// BATT_OBJECT_PRINT_IMPL: verify operator<< outputs field names
//
TEST(KVStoreConfigTest, PrintOutput)
{
  std::ostringstream config_oss;
  config_oss << KVStoreConfig::with_default_values();
  std::string config_output = config_oss.str();
  EXPECT_NE(config_output.find("initial_capacity_bytes"), std::string::npos);
  EXPECT_NE(config_output.find("max_capacity_bytes"), std::string::npos);
  EXPECT_NE(config_output.find("change_log_size_bytes"), std::string::npos);

  std::ostringstream opts_oss;
  opts_oss << KVStoreRuntimeOptions::with_default_values();
  std::string opts_output = opts_oss.str();
  EXPECT_NE(opts_output.find("initial_checkpoint_distance"), std::string::npos);
  EXPECT_NE(opts_output.find("use_threaded_checkpoint_pipeline"), std::string::npos);
  EXPECT_NE(opts_output.find("cache_size_bytes"), std::string::npos);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// ConfigParam default constructor
//
TEST(KVStoreConfigTest, ConfigParam_DefaultConstruct)
{
  ConfigParam param;
  EXPECT_TRUE(param.name().empty());
}

}  // namespace
