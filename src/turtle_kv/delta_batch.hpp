#pragma once

#include <turtle_kv/api_types.hpp>
#include <turtle_kv/change_log_read_lock.hpp>
#include <turtle_kv/delta_batch_id.hpp>
#include <turtle_kv/mem_table.hpp>

#include <turtle_kv/core/edit_view.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/strong_types.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>

#include <llfs/slot.hpp>
#include <llfs/slot_lock_manager.hpp>

#include <batteries/async/grant.hpp>

#include <vector>

namespace turtle_kv {

class MemTable;

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
  explicit DeltaBatch(boost::intrusive_ptr<MemTable>&& mem_table) noexcept;

  /** \brief DeltaBatch objects are not copy-/move-constructible.
   */
  DeltaBatch(const DeltaBatch&) = delete;

  /** \brief DeltaBatch objects are not copy-/move-assignable.
   */
  DeltaBatch& operator=(const DeltaBatch&) = delete;

  /** \brief Merge and compact edits from the MemTable.
   */
  void merge_compact_edits();

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

  /** \brief
   */
  DeltaBatchId batch_id() const noexcept;

  /** \brief
   */
  const MemTable& mem_table() const noexcept
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
  boost::intrusive_ptr<MemTable> mem_table_;

  /** \brief The merged/compacted edits from the log.
   */
  Optional<ResultSet> result_set_;
};

}  // namespace turtle_kv
