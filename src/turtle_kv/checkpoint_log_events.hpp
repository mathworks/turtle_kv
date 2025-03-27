#pragma once

#include <turtle_kv/packed_checkpoint.hpp>

#include <llfs/packed_variant.hpp>

namespace turtle_kv {

using CheckpointLogEvent = llfs::PackedVariant<PackedCheckpoint>;

}
