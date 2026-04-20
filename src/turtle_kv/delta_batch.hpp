//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_DELTA_BATCH_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/mem_table/mem_table.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/bool_status.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/slot.hpp>
#include <llfs/slot_lock_manager.hpp>

#include <batteries/async/grant.hpp>

#include <vector>

namespace turtle_kv {

class MemTableBase;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief A merged/compacted batch of edits collected from a MemTable, to be applied to a
 * checkpoint tree to produce a new checkpoint.
 *
 * DeltaBatch instances are typically produced by DeltaBatchBuilder.
 */
class DeltaBatch
{
 public:
  using ResultSet = MergeCompactor::ResultSet</*decay_to_items=*/false>;

  /** \brief Constructs a new DeltaBatch.
   */
  explicit DeltaBatch(DeltaBatchId batch_id,
                      boost::intrusive_ptr<MemTable>&& mem_table,
                      ResultSet&& result_set) noexcept;

  /** \brief DeltaBatch objects are not copy-/move-constructible.
   */
  DeltaBatch(const DeltaBatch&) = delete;

  /** \brief DeltaBatch objects are not copy-/move-assignable.
   */
  DeltaBatch& operator=(const DeltaBatch&) = delete;

  /** \brief Returns the number of compacted edits in the result set.  this->merge_compact_edits()
   * must be called before this function, or we panic.
   */
  usize result_set_size() const
  {
    BATT_CHECK(this->result_set_) << "Forgot to call this->merge_compact_edits()?";
    return this->result_set_->size();
  }

  /** \brief Returns the edits for this batch.
   */
  ResultSet consume_result_set()
  {
    BATT_CHECK(this->result_set_) << "Forgot to call this->merge_compact_edits()?";
    auto on_scope_exit = batt::finally([&] {
      this->result_set_ = None;
    });
    return std::move(*this->result_set_);
  }

  /** \brief Identifies the order of this batch relative to others in its group and other groups
   * (with different edit offset upper bounds).
   */
  const DeltaBatchId& batch_id() const noexcept
  {
    return this->batch_id_;
  }

  /** \brief
   */
  const MemTableBase& mem_table() const noexcept
  {
    return *this->mem_table_;
  }

  /** \brief Returns whether the batch contains page references ("big" values).
   */
  HasPageRefs has_page_refs() const noexcept
  {
    return HasPageRefs{false};
  }

  batt::SmallFn<void(std::ostream&)> debug_info() const noexcept
  {
    return [this](std::ostream& out) {
      out << "DeltaBatch{mem_table}";
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const DeltaBatchId batch_id_;

  boost::intrusive_ptr<MemTable> mem_table_;

  /** \brief The merged/compacted edits from the log.
   */
  Optional<ResultSet> result_set_;
};

}  // namespace turtle_kv
