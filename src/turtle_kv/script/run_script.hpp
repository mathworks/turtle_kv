//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_RUN_SCRIPT_HPP

#include <turtle_kv/import/status.hpp>

#include <filesystem>

namespace turtle_kv {

Status run_script(const std::filesystem::path& kv_store_dir,
                  const std::filesystem::path& script_yml);

}  // namespace turtle_kv
