//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_SCRIPT_CONTEXT_HPP

#include <turtle_kv/kv_store.hpp>
#include <turtle_kv/kv_store_config.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <atomic>
#include <filesystem>
#include <memory>

namespace turtle_kv {

struct ScriptContext {
  std::filesystem::path kv_store_dir;

  std::filesystem::path script_yml;

  KVStoreConfig config = KVStoreConfig::with_default_values();

  KVStoreRuntimeOptions runtime_options = KVStoreRuntimeOptions::with_default_values();

  std::unique_ptr<KVStore> kv_store;

  std::atomic<usize> insert_count{0};
};

}  // namespace turtle_kv
