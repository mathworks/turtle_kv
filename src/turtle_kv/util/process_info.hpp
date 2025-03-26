#pragma once

#include <turtle_kv/import/logging.hpp>
//
#include <turtle_kv/import/int_types.hpp>

#include <batteries/stream_util.hpp>

#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <string>
#include <thread>

#include <stdio.h>
#include <unistd.h>

namespace turtle_kv {

class ProcessInfo
{
 public:
  using Self = ProcessInfo;

  // Select a prime number near 5 seconds, to try to avoid synchronizing with other periodic tasks.
  //
  static constexpr i64 kDefaultFrequencyMsec = 5153;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ProcessInfo() noexcept : target_pid_{getpid()}
  {
  }

  explicit ProcessInfo(i32 target_pid) noexcept : target_pid_{target_pid}
  {
  }

  ProcessInfo(const ProcessInfo&) = delete;
  ProcessInfo& operator=(const ProcessInfo&) = delete;

  ~ProcessInfo() noexcept
  {
    this->halt();
    this->join();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void update_stats() noexcept
  {
    bool fail = false;

    std::optional<usize> opt_cpu_seconds = this->run_command(this->get_cpu_seconds_command_);
    if (opt_cpu_seconds) {
      this->cpu_seconds_.store(*opt_cpu_seconds);
    } else {
      fail = true;
    }

    std::optional<usize> opt_memory = this->run_command(this->get_memory_command_);
    if (opt_memory) {
      this->memory_usage_.store(*opt_memory);
    } else {
      fail = true;
    }

    if (fail) {
      LOG_FIRST_N(ERROR, 1) << "Failed to update stats! (ProcessInfo)";
    }
  }

  usize cpu_seconds() const noexcept
  {
    return this->cpu_seconds_.load();
  }

  usize memory_usage() const noexcept
  {
    return this->memory_usage_.load();
  }

  void halt() noexcept
  {
    this->halt_requested_.store(true);
  }

  void join() noexcept
  {
    if (this->thread_.joinable()) {
      this->thread_.join();
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void thread_main() noexcept
  {
    LOG(INFO) << "ProcessInfo{" << BATT_INSPECT(this->target_pid_) << "}";
    LOG(INFO) << BATT_INSPECT(this->get_cpu_seconds_command_);
    LOG(INFO) << BATT_INSPECT(this->get_memory_command_);

    while (!this->halt_requested_.load()) {
      this->update_stats();
      std::this_thread::sleep_for(std::chrono::milliseconds(Self::kDefaultFrequencyMsec));
    }
  }

  std::optional<usize> run_command(const std::string& cmd) const
  {
    FILE* fp = popen(cmd.c_str(), /*mode=*/"r");
    if (!fp) {
      VLOG(1) << "Failed to popen(" << batt::c_str_literal(cmd) << ")";
      return std::nullopt;
    }

    auto on_scope_exit = batt::finally([&] {
      pclose(fp);
    });

    std::array<char, 64> buffer;
    buffer.fill('\0');

    char* result = fgets(buffer.data(), buffer.size(), fp);
    if (!result) {
      VLOG(1) << "Failed to read from STDOUT of " << batt::c_str_literal(cmd);
      return std::nullopt;
    }

    std::string str{result};
    while (str.length() > 0 && std::isspace(str[0])) {
      str.erase(0, 1);
    }
    while (str.length() > 0 && std::isspace(str[str.length() - 1])) {
      str.erase(str.length() - 1, 1);
    }

    std::optional<usize> parsed = batt::from_string<usize>(str);
    if (!parsed) {
      VLOG(1) << "Failed to parse output of " << batt::c_str_literal(cmd) << " as type `usize`;"
              << BATT_INSPECT_STR(str) << BATT_INSPECT_STR(result);
    }

    return parsed;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i32 target_pid_;
  std::atomic<bool> halt_requested_{false};
  std::atomic<usize> cpu_seconds_{0};
  std::atomic<usize> memory_usage_{0};

  const std::string get_memory_command_ =
      batt::to_string("expr $(cat /proc/",
                      this->target_pid_,
                      "/status | grep VmRSS | grep kB | awk \'{print $2}\') \\* 1024");

  const std::string get_cpu_seconds_command_ =
      batt::to_string("expr $(cat /proc/",
                      this->target_pid_,
                      "/stat | awk \'{print $14 \" + \" $15}\')");

  std::thread thread_{[this] {
    this->thread_main();
  }};
};

}  // namespace turtle_kv
