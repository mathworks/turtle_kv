#pragma once

#include <turtle_kv/import/buffer.hpp>

#include <llfs/page_buffer.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>

#include <memory>

namespace turtle_kv {

inline ConstBuffer get_page_const_payload(const llfs::PinnedPage& pinned_page) noexcept
{
  return pinned_page.const_payload();
}

inline ConstBuffer get_page_const_payload(const llfs::PageBuffer& page_buffer) noexcept
{
  return page_buffer.const_payload();
}

inline ConstBuffer get_page_const_payload(
    const std::shared_ptr<const llfs::PageBuffer>& p_page_buffer) noexcept
{
  return get_page_const_payload(*p_page_buffer);
}

inline ConstBuffer get_page_const_payload(const llfs::PageView& page_view) noexcept
{
  return page_view.const_payload();
}

inline ConstBuffer get_page_const_payload(const ConstBuffer& buffer) noexcept
{
  return buffer;
}

}  // namespace turtle_kv
