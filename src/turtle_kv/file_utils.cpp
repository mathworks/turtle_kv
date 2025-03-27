#include <turtle_kv/file_utils.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status remove_existing_path(const std::filesystem::path& path) noexcept
{
  std::error_code ec;
  if (std::filesystem::exists(path, ec) && !ec) {
    std::filesystem::remove_all(path, ec);
  }
  BATT_REQUIRE_OK(ec);

  return OkStatus();
}

}  // namespace turtle_kv
