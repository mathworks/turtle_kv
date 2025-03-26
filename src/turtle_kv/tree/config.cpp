#include <turtle_kv/tree/config.hpp>
//

#include <batteries/env.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
unsigned tree_prefetch_level()
{
  static const unsigned level_ =
      batt::getenv_as<unsigned>("TURTLE_TREE_PREFETCH").value_or(kDefaultPrefetchLevel);
  return level_;
}

}  // namespace turtle_kv
