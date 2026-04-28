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

#include <memory>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_insert(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  BATT_ASSIGN_OK_RESULT(  //
      std::unique_ptr<KeyDistribution> key_dist,
      context.parse_param<std::unique_ptr<KeyDistribution>>(params,
                                                            "key_dist",
                                                            /*default=*/nullptr));

  BATT_ASSIGN_OK_RESULT(  //
      usize count,
      context.parse_param<usize>(params, "count", /*default=*/1));

  BATT_ASSIGN_OK_RESULT(  //
      usize value_size,
      context.parse_param<usize>(params,
                                 "value_size",
                                 /*default=*/context.config.tree_options.value_size_hint()));

  BATT_REQUIRE_NE(key_dist.get(), nullptr);
  BATT_REQUIRE_NE(context.kv_store.get(), nullptr);

  std::string value;

  LOG(INFO) << "insert(count=" << count << ")";

  auto start_time = std::chrono::steady_clock::now();

  for (usize i = 0; i < count; ++i) {
    KeyView key = key_dist->get_next(context.inserted_keys);

    value.assign(value_size, (char)'0' + (i % 64));

    BATT_REQUIRE_OK(context.kv_store->put(key, ValueView::from_str(value)));
  }

  auto duration = std::chrono::steady_clock::now() - start_time;
  double elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  LOG(INFO) << "  elapsed: " << elapsed_ns / 1e9 << "s, " << (double)count * 1e6 / elapsed_ns
            << " kops/sec";

  return OkStatus();
}

}  // namespace turtle_kv
