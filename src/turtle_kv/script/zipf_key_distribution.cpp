//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/zipf_key_distribution.hpp>
//

#include <batteries/stream_util.hpp>

#include <algorithm>
#include <numeric>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ZipfKeyDistribution::ZipfKeyDistribution(
    std::default_random_engine::result_type random_seed,
    double alpha,
    usize max_index) noexcept
    : name_{batt::to_string("zipf{seed=", random_seed, ",alpha=", alpha, ",N=", max_index, "}")}
    , rng_{random_seed}
    , pick_index_{alpha, /*min_index=*/0, max_index}
    , shuffle_(max_index + 1)
{
  std::iota(this->shuffle_.begin(), this->shuffle_.end(), 0);
  std::shuffle(this->shuffle_.begin(), this->shuffle_.end(), this->rng_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::pair<KeyView, usize> ZipfKeyDistribution::get_next(KeySet& key_set) /*override*/
{
  const usize index = this->shuffle_[this->pick_index_(this->rng_)];
  BATT_DEBUG_INFO(BATT_INSPECT(index));

  return std::make_pair(key_set.get_key(index).value_or_panic(), index);
}

}  // namespace turtle_kv
