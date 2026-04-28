//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/handle_point_query.hpp>
//

#include <turtle_kv/script/key_distribution.hpp>
#include <turtle_kv/script/uniform_key_distribution.hpp>
#include <turtle_kv/script/zipf_key_distribution.hpp>

#include <chrono>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_point_query(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
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

  std::vector<KeyView> query_keys;
  for (usize i = 0; i < count; ++i) {
    query_keys.push_back(key_dist->get_next(context.inserted_keys));
  }

  LOG(INFO) << "point_query(count=" << count << ")";

  auto start_time = std::chrono::steady_clock::now();

  for (const KeyView& key : query_keys) {
    BATT_REQUIRE_OK(context.kv_store->get(key));
  }

  auto duration = std::chrono::steady_clock::now() - start_time;

  LOG(INFO) << "  elapsed: " << duration << ", "
            << (double)count * 1e6 /
                   (double)std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count()
            << " kops/sec";

  return OkStatus();
}

}  // namespace turtle_kv
