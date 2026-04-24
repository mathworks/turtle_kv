#pragma once
#define TURTLE_KV_UTIL_PRINT_CSV_HPP

#include <turtle_kv/import/int_types.hpp>

#include <batteries/stream_util.hpp>

#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace turtle_kv {

inline void print_csv(std::ostream& out,
                      const std::vector<std::vector<std::pair<std::string, std::string>>>& rows)
{
  // Gather column names and map them to indexes.
  //
  std::vector<std::string> column_name;
  std::unordered_map<std::string, usize> column_index;

  for (const auto& row : rows) {
    for (const auto& items : row) {
      if (column_index.count(items.first)) {
        continue;
      }
      column_index.emplace(items.first, column_name.size());
      column_name.push_back(items.first);
    }
  }

  const auto emit_row = [&out](std::string id, const std::vector<std::string>& columns) {
    out << id;
    for (const auto& n : columns) {
      out << "," << n;
    }
    out << "\n";
  };

  // Emit the column names.
  //
  emit_row("id", column_name);

  // Emit the rows.
  //

  usize row_i = 0;
  for (const auto& row : rows) {
    std::vector<std::string> values(column_name.size(), "");
    for (const auto& [name, value] : row) {
      values[column_index[name]] = value;
    }
    emit_row(batt::to_string(row_i), values);
    ++row_i;
  }
}

}  // namespace turtle_kv
