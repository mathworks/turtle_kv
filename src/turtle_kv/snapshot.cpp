//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/snapshot.hpp>
//

#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Snapshot::Snapshot(Checkpoint&& checkpoint,
                   EditOffset edit_offset,
                   llfs::PageCache& page_cache,
                   const TreeOptions& tree_options) noexcept
    : checkpoint_{std::move(checkpoint)}
    , page_cache_{page_cache}
    , tree_options_{tree_options}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> Snapshot::get(const KeyView& key)
{
  PinningPageLoader page_loader{this->page_cache_};
  PageSliceStorage result_storage;

  KeyQuery query{
      page_loader,
      result_storage,
      this->tree_options_,
      key,
  };

  return this->checkpoint_.find_key(query);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
EditOffset Snapshot::edit_offset() const noexcept
{
  return this->checkpoint_.edit_offset_upper_bound();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool Snapshot::is_empty() const noexcept
{
  return this->checkpoint_.is_empty();
}

}  // namespace turtle_kv
