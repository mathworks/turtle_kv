#pragma once

#include <turtle_kv/util/env_param.hpp>

namespace turtle_kv {

TURTLE_KV_ENV_PARAM(bool, turtlekv_use_bloom_filters, false);
TURTLE_KV_ENV_PARAM(bool, turtlekv_use_vector_quotient_filters, true);

}  // namespace turtle_kv
