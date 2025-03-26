#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

int main(int, char**)
{
  std::map<std::string, std::string> state;

  while (std::cin.good()) {
    std::string op_name;
    std::cin >> op_name;

    if (op_name == "P") {
      std::string key, value;

      std::cin >> key >> value;

      state[key] = value;

      std::cout << "P " << key << " " << value << std::endl;

    } else if (op_name == "G") {
      std::string key;

      std::cin >> key;

      std::cout << "T " << key << " " << state[key] << std::endl;

    } else if (op_name == "S") {
      std::string min_key;
      int count;

      std::cin >> min_key >> count;

      std::cout << "V " << min_key << " " << count << " ";

      auto iter = state.lower_bound(min_key);
      std::vector<std::pair<std::string, std::string>> result;

      while (count && iter != state.end()) {
        result.emplace_back(*iter);
        ++iter;
        --count;
      }

      std::cout << result.size();

      for (const auto& [k, v] : result) {
        std::cout << " " << k << " " << v;
      }

      std::cout << std::endl;

    } else if (!std::cin.good()) {
      break;

    } else {
      std::cerr << "Bad Operation Name: " << op_name << std::endl;
      std::abort();
    }
  }

  return 0;
}
