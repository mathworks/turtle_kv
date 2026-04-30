//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/run_script.hpp>
//
#include <turtle_kv/script/run_script.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/data_root.test.hpp>
#include <turtle_kv/testing/workload.test.hpp>

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::filesystem::path get_script_test_dir_path()
{
  return turtle_kv::testing::get_project_file(std::filesystem::path{"tests"});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<std::filesystem::path> find_test_scripts()
{
  std::vector<std::filesystem::path> script_files;

  for (const auto& dir_entry : std::filesystem::directory_iterator{get_script_test_dir_path()}) {
    auto path = dir_entry.path();
    if (path.extension() == ".yml") {
      script_files.push_back(path);
    }
  }

  return script_files;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ScriptTest : public ::testing::TestWithParam<std::filesystem::path>
{
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_P(ScriptTest, Run)
{
  std::filesystem::path script_yml = GetParam();

  LOG(INFO) << BATT_INSPECT(script_yml);

  batt::Status status =
      turtle_kv::run_script(BATT_OK_RESULT_OR_PANIC(turtle_kv::data_root()), script_yml);

  ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string test_name_from(std::string s)
{
  std::vector<char> buffer;

  bool start_of_word = true;
  for (char ch : s) {
    if (buffer.empty() && !std::isalpha(ch)) {
      continue;
    }
    if (!std::isalnum(ch)) {
      start_of_word = true;
      continue;
    }
    if (start_of_word) {
      buffer.push_back(std::toupper(ch));
      start_of_word = false;
    } else {
      buffer.push_back(ch);
    }
  }

  return std::string(buffer.data(), buffer.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string format_script_test_name(::testing::TestParamInfo<std::filesystem::path> info)
{
  return test_name_from(info.param.stem().string());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
INSTANTIATE_TEST_SUITE_P(AllScripts,
                         ScriptTest,
                         testing::ValuesIn(find_test_scripts()),
                         format_script_test_name);

}  // namespace
