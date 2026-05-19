//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/key_distribution.hpp>
//

#include <turtle_kv/script/script_context.hpp>
#include <turtle_kv/script/uniform_key_distribution.hpp>
#include <turtle_kv/script/zipf_key_distribution.hpp>

namespace turtle_kv {

namespace {

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<KeyDistribution>> KeyDistribution::from_param(
    ScriptContext& context,
    const std::string& param_value,
    const YAML::Node& params) noexcept
{
  const bool workload_is_insert =
      !context.command_stack.empty() && context.command_stack.back() == "insert";

  if (param_value == "uniform") {
    BATT_CHECK(workload_is_insert);
    BATT_ASSIGN_OK_RESULT(
        usize key_size,
        context.parse_param<usize>(params,
                                   "key_size",
                                   /*default=*/context.config.tree_options.key_size_hint()));

    return {std::make_unique<UniformInsertKeyDistribution>(key_size)};

  } else if (param_value == "zipf") {
    BATT_ASSIGN_OK_RESULT(  //
        double alpha,
        context.parse_param<double>(params, "zipf_alpha", /*default=*/1.0));

    BATT_ASSIGN_OK_RESULT(  //
        std::default_random_engine::result_type random_seed,
        context.parse_param<std::default_random_engine::result_type>(params,
                                                                     "random_seed",
                                                                     /*default=*/0));

    const usize max_index = /*max_index=*/context.key_set.inserted_upper_bound() - 1;

    return {std::make_unique<ZipfKeyDistribution>(random_seed, alpha, max_index)};
  }

  LOG(ERROR) << "bad value: " << param_value << BATT_INSPECT(params);
  return {batt::StatusCode::kInvalidArgument};
}

}  // namespace turtle_kv
