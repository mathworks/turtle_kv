//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/handle_insert.hpp>
//

#include <turtle_kv/script/key_distribution.hpp>
#include <turtle_kv/script/uniform_key_distribution.hpp>

#include <memory>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_insert(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  usize count = 1;
  std::unique_ptr<KeyDistribution> key_dist;
  usize key_size = context.config.tree_options.key_size_hint();
  usize value_size = context.config.tree_options.value_size_hint();

  for (const auto& param_pair : params) {
    const std::string param_name = param_pair.first.as<std::string>();
    const std::string param_value = param_pair.second.as<std::string>();

    if (param_name == "count") {
      std::optional<usize> parsed = batt::from_string<usize>(param_value);
      if (!parsed) {
        LOG(ERROR) << "bad value: " << param_value;
        return batt::StatusCode::kInvalidArgument;
      }
      count = *parsed;

    } else if (param_name == "key_dist") {
      if (param_value == "uniform") {
        key_dist = std::make_unique<UniformKeyDistribution>();
      } else {
        LOG(ERROR) << "bad value: " << param_value;
        return batt::StatusCode::kInvalidArgument;
      }

    } else if (param_name == "key_size") {
      std::optional<usize> parsed = batt::from_string<usize>(param_value);
      if (!parsed) {
        LOG(ERROR) << "bad value: " << param_value;
        return batt::StatusCode::kInvalidArgument;
      }
      key_size = *parsed;

    } else if (param_name == "value_size") {
      std::optional<usize> parsed = batt::from_string<usize>(param_value);
      if (!parsed) {
        LOG(ERROR) << "bad value: " << param_value;
        return batt::StatusCode::kInvalidArgument;
      }
      key_size = *parsed;

    } else {
      LOG(ERROR) << "bad param: " << param_value;
      return batt::StatusCode::kInvalidArgument;
    }
  }

  BATT_REQUIRE_NE(key_dist.get(), nullptr);
  BATT_REQUIRE_NE(context.kv_store.get(), nullptr);

  SmallVec<char, 64> key_buffer;
  std::string value;

  LOG(INFO) << "insert(count=" << count << ")";

  for (usize key_ordinal = 0; key_ordinal < count; ++key_ordinal) {
    KeyView key = key_dist->get_key(key_ordinal, key_buffer, key_size);
    value.assign(value_size, (char)'0' + (key_ordinal % 64));

    BATT_REQUIRE_OK(context.kv_store->put(key, ValueView::from_str(value)));

    context.insert_count.fetch_add(1);
  }

  return OkStatus();
}

}  // namespace turtle_kv
