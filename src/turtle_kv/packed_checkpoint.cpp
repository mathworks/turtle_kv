#include <turtle_kv/packed_checkpoint.hpp>
//
#include <turtle_kv/import/slice.hpp>

#include <batteries/require.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedCheckpoint& t)
{
  return out << "PackedCheckpoint{"                                         //
             << ", .edit_offset_upper_bound=" << t.edit_offset_upper_bound  //
             << ", .tree_root=" << t.new_tree_root                          //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedCheckpoint& checkpoint)
{
  return llfs::seq::single_item(checkpoint.new_tree_root.as_page_id())  //
         | llfs::seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PackedActiveCheckpoints::set_base_checkpoint(const PackedCheckpoint& checkpoint)
{
  if (this->has_base_checkpoint) {
    BATT_REQUIRE_GT(checkpoint.edit_offset_upper_bound,
                    this->base_checkpoint.edit_offset_upper_bound);
  }
  this->base_checkpoint = checkpoint;
  this->has_base_checkpoint = true;

  return OkStatus();
}

StatusOr<PackedCheckpoint> PackedActiveCheckpoints::base() const
{
  if (!this->has_base_checkpoint) {
    return {batt::StatusCode::kNotFound};
  }
  return this->base_checkpoint;
}

Slice<const PackedCheckpoint> PackedActiveCheckpoints::pinned() const
{
  return as_slice(this->pinned_checkpoints.data(), this->num_pinned_checkpoints);
}

bool PackedActiveCheckpoints::is_pin_limit_reached() const
{
  return this->num_pinned_checkpoints >= MAX_PINNED_CHECKPOINTS;
}

Status PackedActiveCheckpoints::pin(const PackedCheckpoint& checkpoint)
{
  if (this->is_pin_limit_reached()) {
    return {batt::StatusCode::kResourceExhausted};
  }

  const u8 n = this->num_pinned_checkpoints;
  // Check that the new checkpoint is newer than the last pinned checkpoint.
  //
  if (n > 0) {
    BATT_REQUIRE_GT(checkpoint.edit_offset_upper_bound,
                    this->pinned_checkpoints[n - 1].edit_offset_upper_bound);
  }
  this->pinned_checkpoints[n] = checkpoint;
  this->num_pinned_checkpoints = n + 1;

  return OkStatus();
}

Status PackedActiveCheckpoints::unpin(i64 edit_offset)
{
  for (u8 i = 0; i < this->num_pinned_checkpoints; ++i) {
    if (this->pinned_checkpoints[i].edit_offset_upper_bound == edit_offset) {
      for (u8 j = i; j + 1 < this->num_pinned_checkpoints; ++j) {
        this->pinned_checkpoints[j] = this->pinned_checkpoints[j + 1];
      }
      this->num_pinned_checkpoints = this->num_pinned_checkpoints - 1;
      return OkStatus();
    }
  }
  return {batt::StatusCode::kNotFound};
}

StatusOr<PackedCheckpoint> PackedActiveCheckpoints::find_pinned(i64 edit_offset) const
{
  for (const PackedCheckpoint& checkpoint : this->pinned()) {
    if (checkpoint.edit_offset_upper_bound == edit_offset) {
      return checkpoint;
    }
  }
  return {batt::StatusCode::kNotFound};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::BoxedSeq<llfs::PageId> trace_refs(const PackedActiveCheckpoints& active)
{
  const StatusOr<PackedCheckpoint> base = active.base();

  // The base checkpoint may also be pinned; skip it here so its refs are only traced once.
  //
  auto pinned_refs =
      as_seq(active.pinned())  //
      | llfs::seq::filter([base](const PackedCheckpoint& checkpoint) {
          return !base.ok() || checkpoint.edit_offset_upper_bound != base->edit_offset_upper_bound;
        })  //
      | llfs::seq::map([](const PackedCheckpoint& checkpoint) {
          return trace_refs(checkpoint);
        })  //
      | llfs::seq::flatten();

  if (!base.ok()) {
    return std::move(pinned_refs) | llfs::seq::boxed();
  }

  return std::move(pinned_refs)                 //
         | llfs::seq::chain(trace_refs(*base))  //
         | llfs::seq::boxed();
}

}  // namespace turtle_kv
