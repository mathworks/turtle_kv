//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_READER_HPP

#include <turtle_kv/change_log/api_types.hpp>
#include <turtle_kv/change_log/change_log_block.hpp>
#include <turtle_kv/change_log/change_log_file.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/util/stack_merger.hpp>

#include <functional>

namespace turtle_kv {

class ChangeLogReader
{
 public:
  // Function responsible for parsing one slot at a time.
  //
  using SlotVisitorFn = std::function<Status(FirstVisitToBlock first_visit,
                                             ChangeLogBlock* block,
                                             EditOffset edit_offset,
                                             ConstBuffer payload)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogReader(const ChangeLogReader&) = delete;
  ChangeLogReader& operator=(const ChangeLogReader&) = delete;

  virtual ~ChangeLogReader() = default;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  static StatusOr<std::unique_ptr<ChangeLogReader>> open(const std::filesystem::path& path) noexcept
  {
    BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

    return {std::make_unique<ChangeLogReader>(std::move(log_file))};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  explicit ChangeLogReader(std::unique_ptr<ChangeLogFile>&& change_log) noexcept
      : change_log_{std::move(change_log)}
  {
  }

  /** \brief Calls the passed SlotVisitor function for each recovered slot, in EditOffset order.
   */
  Status visit_slots(  // TODO [tastolfi 2026-05-01] add a slot lower bound parameter to optimize.
      const SlotVisitorFn& visitor,
      RecoveredChangeLogState* recovered_state = nullptr) noexcept;

 private:
  /** \brief The state of the log file.
   */

  std::unique_ptr<ChangeLogFile> change_log_;
};

}  // namespace turtle_kv
