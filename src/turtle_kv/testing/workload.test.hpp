#pragma once

#include <turtle_kv/core/table.hpp>

#include <batteries/small_vec.hpp>
#include <batteries/stream_util.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace turtle_kv {
namespace testing {

struct LatencyMeasurement {
  std::string label;
  double seconds;
  double op_count;
};

struct WorkloadState {
  usize op_count = 0;
  char op_char = 0;
  std::istream& in;
  Table& table;
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  batt::SmallVec<LatencyMeasurement, 32> latency_measurements;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit WorkloadState(std::istream& in, Table& table) noexcept : in{in}, table{table}
  {
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool illegal_operation_handler(WorkloadState& state)
{
  [&] {
    FAIL() << "Illegal opcode: " << batt::c_str_literal(std::string_view{&state.op_char, 1})
           << BATT_INSPECT(state.op_count);
  }();
  return false;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool put_operation_handler(WorkloadState& state)
{
  std::string key, value;

  state.in >> key >> value;

  Status status = state.table.put(key, ValueView::from_str(value));
  if (!status.ok()) {
    ADD_FAILURE() << "Put failed! " << BATT_INSPECT(status) << BATT_INSPECT(key)
                  << BATT_INSPECT(value) << BATT_INSPECT(state.op_count);
    return false;
  }
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool get_and_test_operation_handler(WorkloadState& state)
{
  std::string key, expected_value;

  state.in >> key >> expected_value;

  StatusOr<ValueView> actual_value = state.table.get(key);

  if (!actual_value.ok()) {
    ADD_FAILURE() << "Get failed! " << BATT_INSPECT(actual_value.status()) << BATT_INSPECT(key)
                  << BATT_INSPECT(expected_value);
    return false;
  }

  if (actual_value->as_str() != expected_value) {
    ADD_FAILURE() << "Get value does not match expected! " << BATT_INSPECT(actual_value)
                  << BATT_INSPECT(expected_value);
    return false;
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool get_only_operation_handler(WorkloadState& state)
{
  std::string key;

  state.in >> key;

  StatusOr<ValueView> actual_value = state.table.get(key);

  if (!actual_value.ok()) {
    ADD_FAILURE() << "Get failed! " << BATT_INSPECT(actual_value.status()) << BATT_INSPECT(key);
    return false;
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool scan_and_verify_operation_handler(WorkloadState& state)
{
  std::string min_key;
  int query_count, result_count;

  state.in >> min_key >> query_count >> result_count;

  BATT_CHECK_LE(result_count, query_count);

  std::vector<std::pair<KeyView, ValueView>> results;
  results.resize(query_count);

  StatusOr<usize> status = state.table.scan(min_key, as_slice(results));

  if (!status.ok()) {
    ADD_FAILURE() << "Scan failed! " << BATT_INSPECT(status) << BATT_INSPECT(min_key)
                  << BATT_INSPECT(query_count) << BATT_INSPECT(result_count);
    return false;
  }

  results.resize(*status);

  if (results.size() != (usize)result_count) {
    ADD_FAILURE() << "Scan returned incorrect size! " << BATT_INSPECT(results.size())
                  << BATT_INSPECT(query_count) << BATT_INSPECT(result_count);
    return false;
  }

  for (int i = 0; i < result_count; ++i) {
    std::string expected_key, expected_value;
    state.in >> expected_key >> expected_value;

    if (results[i].first != expected_key) {
      ADD_FAILURE() << "Scan key " << i
                    << " does not match expected: " << BATT_INSPECT(expected_key)
                    << BATT_INSPECT(expected_value) << BATT_INSPECT(results[i].first)
                    << BATT_INSPECT(results[i].second);
      return false;
    }

    if (results[i].second.as_str() != expected_value) {
      ADD_FAILURE() << "Scan value " << i
                    << " does not match expected: " << BATT_INSPECT(expected_key)
                    << BATT_INSPECT(expected_value) << BATT_INSPECT(results[i].first)
                    << BATT_INSPECT(results[i].second) << BATT_INSPECT(results[i].second.as_str());
      return false;
    }
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline bool latency_timer_handler(WorkloadState& state)
{
  std::string label;

  state.in >> label;

  double seconds = (double)(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - state.start_time)
                                .count()) /
                   1e9;

  state.latency_measurements.emplace_back(LatencyMeasurement{
      .label = label,
      .seconds = seconds,
      .op_count = (double)state.op_count,
  });

  return true;
}

inline std::filesystem::path get_project_file(const std::filesystem::path& rel_to_project_root)
{
  std::filesystem::path this_file_path{__FILE__};
  this_file_path = std::filesystem::absolute(this_file_path);
  std::filesystem::path src_turtle_kv_testing_dir = this_file_path.parent_path();
  std::filesystem::path src_turtle_kv_dir = src_turtle_kv_testing_dir.parent_path();
  std::filesystem::path src_dir = src_turtle_kv_dir.parent_path();
  std::filesystem::path top_level_dir = src_dir.parent_path();

  return top_level_dir / rel_to_project_root;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline std::tuple<usize, batt::SmallVec<LatencyMeasurement, 32>> run_workload(
    const std::filesystem::path& workload_file,
    Table& table) noexcept
{
  using OpHandlerFn = bool(WorkloadState&);

  std::array<OpHandlerFn*, 256> handlers;

  handlers.fill(&illegal_operation_handler);

  handlers['G'] = &get_only_operation_handler;
  handlers['L'] = &latency_timer_handler;
  handlers['P'] = &put_operation_handler;
  handlers['T'] = &get_and_test_operation_handler;
  handlers['V'] = &scan_and_verify_operation_handler;

  std::ifstream ifs{workload_file};
  BATT_CHECK(ifs.good()) << BATT_INSPECT_STR(workload_file.string());

  WorkloadState state{ifs, table};

  while (ifs.good()) {
    std::string op_name;

    ifs >> op_name;

    if (!ifs.good()) {
      break;
    }

    BATT_CHECK_EQ(op_name.size(), 1u) << BATT_INSPECT_STR(op_name) << BATT_INSPECT(state.op_count);

    state.op_char = op_name[0];

    if (!handlers[state.op_char](state)) {
      break;
    }
    ++state.op_count;
  }

  return std::make_tuple(state.op_count, std::move(state.latency_measurements));
}

}  // namespace testing
}  // namespace turtle_kv
