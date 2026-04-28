//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/zipf_key_distribution.hpp>
//

#include <algorithm>
#include <numeric>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ZipfKeyDistribution::ZipfKeyDistribution(
    std::default_random_engine::result_type random_seed,
    double alpha,
    usize max_index) noexcept
    : rng_{random_seed}
    , pick_index_{alpha, /*min_index=*/0, max_index}
    , shuffle_(max_index + 1)
{
  std::iota(this->shuffle_.begin(), this->shuffle_.end(), 0);
  std::shuffle(this->shuffle_.begin(), this->shuffle_.end(), this->rng_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
KeyView ZipfKeyDistribution::get_next(KeySet& inserted_keys) /*override*/
{
  usize index = this->shuffle_[this->pick_index_(this->rng_)];

  return inserted_keys.get_key_by_index(index).value_or_panic();
}

}  // namespace turtle_kv
