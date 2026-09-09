#pragma once

#include <llfs/buffer.hpp>

#include <batteries/buffer.hpp>

namespace turtle_kv {

using batt::advance_pointer;
using batt::buffer_from_struct;
using batt::byte_distance;
using batt::ConstBuffer;
using batt::make_buffer;
using batt::mutable_buffer_from_struct;
using batt::MutableBuffer;
using batt::resize_buffer;

}  // namespace turtle_kv
