#include <turtle_kv/tree/tree_options.hpp>
//

#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <turtle_kv/import/logging.hpp>

#include <llfs/page_layout.hpp>

#include <batteries/env.hpp>

#include <cmath>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ TreeOptions TreeOptions::with_default_values()
{
  static const Self instance_ = Self{}  //
                                    .set_node_size(4 * kKiB)
                                    .set_leaf_size(2 * kMiB);

  return instance_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ TreeOptions::GlobalOptions& TreeOptions::global_options()
{
  static constexpr const char* const kVarName = "TURTLE_KV_PAGE_CACHE_OBSOLETE_HINTS";

  static GlobalOptions* const p_global_options_ = []() {
    static GlobalOptions global_options_;
    global_options_.page_cache_obsolete_hints = batt::getenv_as<bool>(kVarName).value_or(false);
    LOG(INFO) << kVarName << "=" << global_options_.page_cache_obsolete_hints;
    return &global_options_;
  }();

  return *p_global_options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const TreeOptions& t)
{
  return out << "TreeOptions{.node_size=" << t.node_size()            //
             << ", .leaf_size=" << t.leaf_size()                      //
             << ", .filter_bits_per_key=" << t.filter_bits_per_key()  //
             << ", .filter_page_size=" << t.filter_page_size()        //
             << ", .max_item_size=" << t.max_item_size()              //
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize TreeOptions::leaf_data_size() const noexcept
{
  return leaf_max_space_from_size(this->leaf_size());
}

}  // namespace turtle_kv
