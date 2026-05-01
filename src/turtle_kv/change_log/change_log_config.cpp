//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/change_log/change_log_config.hpp>
//

#include <turtle_kv/change_log/change_log_file.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto ChangeLogConfig::with_default_values() noexcept -> ChangeLogConfig
{
  ChangeLogConfig config;

  config.block_size = BlockSize{ChangeLogFile::kDefaultBlockSize};
  config.block_count = BlockCount{ChangeLogFile::kDefaultLogSize / config.block_size};
  config.block0_offset = FileOffset{ChangeLogFile::kDefaultBlock0Offset};

  return config;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ChangeLogConfig::pack_to(PackedChangeLogConfig* packed_config) const noexcept
{
  std::memset(packed_config, 0, sizeof(PackedChangeLogConfig));

  packed_config->magic = PackedChangeLogConfig::kMagic;
  packed_config->block_size = this->block_size;
  packed_config->block_count = this->block_count;
  packed_config->block0_offset = this->block0_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PackedChangeLogConfig::unpack() const noexcept -> ChangeLogConfig
{
  BATT_CHECK_EQ(this->magic, PackedChangeLogConfig::kMagic);

  ChangeLogConfig config;

  config.block_size = BlockSize{this->block_size.value()};
  config.block_count = BlockCount{this->block_count.value()};
  config.block0_offset = FileOffset{this->block0_offset.value()};

  return config;
}

}  // namespace turtle_kv
