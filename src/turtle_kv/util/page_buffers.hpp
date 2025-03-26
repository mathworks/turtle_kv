#pragma once

#include <turtle_kv/import/buffer.hpp>

#include <llfs/page_buffer.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>

#include <memory>

namespace turtle_kv {

inline ConstBuffer get_page_const_payload(const llfs::PinnedPage& pinned_page)
{
  return pinned_page.const_payload();
}

inline ConstBuffer get_page_const_payload(const llfs::PageBuffer& page_buffer)
{
  return page_buffer.const_payload();
}

inline ConstBuffer get_page_const_payload(
    const std::shared_ptr<const llfs::PageBuffer>& p_page_buffer)
{
  return get_page_const_payload(*p_page_buffer);
}

inline ConstBuffer get_page_const_payload(const llfs::PageView& page_view)
{
  return page_view.const_payload();
}

inline ConstBuffer get_page_const_payload(const ConstBuffer& buffer)
{
  return buffer;
}

}  // namespace turtle_kv
