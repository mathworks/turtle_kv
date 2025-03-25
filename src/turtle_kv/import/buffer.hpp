#pragma once

#include <llfs/buffer.hpp>

#include <batteries/buffer.hpp>

namespace turtle_kv {

using batt::buffer_from_struct;
using batt::ConstBuffer;
using batt::make_buffer;
using batt::mutable_buffer_from_struct;
using batt::MutableBuffer;
using batt::resize_buffer;

using llfs::advance_pointer;
using llfs::byte_distance;

}  // namespace turtle_kv
