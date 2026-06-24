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
#include <turtle_kv/change_log/recovered_change_log_state.hpp>

#include <batteries/case_of.hpp>

#include <functional>

namespace turtle_kv {

class ChangeLogReader
{
 public:
  /** \brief Function responsible for visiting one slot at a time.
   */
  using SlotVisitorFn = std::function<Status(FirstVisitToBlock first_visit,
                                             ChangeLogBlock* block,
                                             EditOffset edit_offset,
                                             ConstBuffer payload)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a newly opened ChangeLogReader.
   */
  static StatusOr<std::unique_ptr<ChangeLogReader>> open(const std::filesystem::path& path) noexcept
  {
    BATT_ASSIGN_OK_RESULT(std::unique_ptr<ChangeLogFile> log_file, ChangeLogFile::open(path));

    return {std::make_unique<ChangeLogReader>(std::move(log_file))};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ChangeLogReader(const ChangeLogReader&) = delete;
  ChangeLogReader& operator=(const ChangeLogReader&) = delete;

  virtual ~ChangeLogReader() = default;

  /** \brief Creates a non-owning ChangeLogReader.
   */
  explicit ChangeLogReader(ChangeLogFile* change_log_file) noexcept
      : change_log_file_{change_log_file}
  {
  }

  /** \brief Creates a ChangeLogReader, transferring ownership of the passed file.
   */
  explicit ChangeLogReader(std::unique_ptr<ChangeLogFile>&& change_log_file) noexcept
      : change_log_file_{std::move(change_log_file)}
  {
  }

  /** \brief Calls the passed SlotVisitor function for each recovered slot, in EditOffset order.
   */
  StatusOr<RecoveredChangeLogState> visit_slots(
      const SlotVisitorFn& visitor,
      Optional<EditOffset> new_trim_edit_offset = None) noexcept;

  /** \brief Convenience function; uses visit_slots to recover the log state.
   */
  StatusOr<RecoveredChangeLogState> recover_state() noexcept
  {
    return this->visit_slots([](auto&&...) -> Status {
      return OkStatus();
    });
  }

  /** \brief Returns a reference to the ChangeLogFile we are reading.
   */
  ChangeLogFile& change_log_file() const noexcept
  {
    return *batt::case_of(
        this->change_log_file_,
        [](const std::unique_ptr<ChangeLogFile>& ptr) {
          return ptr.get();
        },
        [](ChangeLogFile* ptr) {
          return ptr;
        });
  }

 private:
  /** \brief The state of the log file.
   */
  std::variant<std::unique_ptr<ChangeLogFile>, ChangeLogFile*> change_log_file_;
};

}  // namespace turtle_kv
