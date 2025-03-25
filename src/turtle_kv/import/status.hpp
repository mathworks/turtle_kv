#pragma once

#include <batteries/status.hpp>

namespace turtle_kv {

using batt::OkStatus;
using batt::RemoveStatusOr;
using batt::Status;
using batt::status_from_errno;
using batt::status_from_retval;
using batt::StatusOr;

enum struct StatusCode {
    kOk = 0,
};

bool initialize_status_codes();

}  // namespace turtle_kv
