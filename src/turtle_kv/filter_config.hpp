#pragma once

#include <turtle_kv/util/env_param.hpp>

#include <llfs/page_device_pairing.hpp>

namespace turtle_kv {

TURTLE_KV_ENV_PARAM(bool, turtlekv_use_bloom_filters, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_use_vector_quotient_filters, true);

/** \brief The PageDevice pairing relationship from leaf pages to their corresponding
 * filter.
 */
inline const llfs::PageDevicePairing kPairedFilterForLeaf{
    "leaf_filter",
    "AMQ filter pages for leaf pages (1:1)",
};

}  // namespace turtle_kv
