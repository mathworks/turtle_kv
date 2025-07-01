#pragma once

#include <batteries/suppress.hpp>

BATT_SUPPRESS_IF_GCC("-Wsuggest-override")
//
#include <glog/logging.h>
//
BATT_UNSUPPRESS()

namespace turtle_kv {

// void SetVGLOGLevel(const char* module_spec, int level);
//
using google::SetVLOGLevel;

}  // namespace turtle_kv
