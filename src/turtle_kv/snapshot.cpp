//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/snapshot.hpp>
//

#include <turtle_kv/kv_store.hpp>

#include <turtle_kv/tree/key_query.hpp>
#include <turtle_kv/util/page_slice_reader.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Snapshot::Snapshot(Checkpoint&& checkpoint,
                   KVStore* kv_store) noexcept
    : checkpoint_{std::move(checkpoint)}
    , kv_store_{kv_store}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> Snapshot::get(const KeyView& key) const noexcept
{
  KVStore::ThreadContext& thread_context = this->kv_store_->per_thread_.get(this->kv_store_);

  thread_context.query_result_storage.emplace();

  llfs::PageLoader& page_loader = thread_context.get_page_loader();

  KeyQuery query{
      page_loader,
      *thread_context.query_result_storage,
      this->kv_store_->tree_options_,
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Snapshot::operator bool() const noexcept
{
  return !this->is_empty();
}

}  // namespace turtle_kv
