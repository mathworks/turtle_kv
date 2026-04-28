//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_ZIPF_KEY_DISTRIBUTION_HPP

#include <turtle_kv/script/key_distribution.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/random/zipf.hpp>

#include <random>
#include <vector>

namespace turtle_kv {

class ZipfKeyDistribution : public KeyDistribution
{
 public:
  explicit ZipfKeyDistribution(std::default_random_engine::result_type random_seed,
                               double alpha,
                               usize max_index) noexcept;

  KeyView get_next(KeySet& inserted_keys) override;

 private:
  std::default_random_engine rng_;

  batt::ZipfIntDistribution<usize> pick_index_;

  std::vector<usize> shuffle_;
};

}  // namespace turtle_kv
