//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/point_query_command.hpp>
//

#include <turtle_kv/script/key_distribution.hpp>
#include <turtle_kv/script/uniform_key_distribution.hpp>
#include <turtle_kv/script/zipf_key_distribution.hpp>

#include <chrono>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status point_query_command(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  BATT_ASSIGN_OK_RESULT(  //
      std::unique_ptr<KeyDistribution> key_dist,
      context.parse_param<std::unique_ptr<KeyDistribution>>(params,
                                                            "key_dist",
                                                            /*default=*/nullptr));

  BATT_ASSIGN_OK_RESULT(  //
      usize count,
      context.parse_param<usize>(params, "count", /*default=*/1));

  BATT_REQUIRE_NE(key_dist.get(), nullptr);
  BATT_REQUIRE_NE(context.kv_store.get(), nullptr);

  std::vector<script::Operation> ops;

  for (usize i = 0; i < count; ++i) {
    ops.push_back(script::PointQuery{
        .index = key_dist->get_next(context.key_set).second,
    });
  }

  LOG(INFO) << "point_query(count=" << count << ")";

  BATT_REQUIRE_OK(context.schedule(std::move(ops)));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
