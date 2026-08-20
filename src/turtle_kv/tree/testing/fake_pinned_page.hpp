#pragma once

#include <llfs/page_buffer.hpp>

#include <memory>
#include <utility>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class FakePinnedPage
{
 public:
  explicit FakePinnedPage() noexcept : page_buffer_{}
  {
  }

  explicit FakePinnedPage(std::shared_ptr<llfs::PageBuffer>&& page_buffer) noexcept
      : page_buffer_{std::move(page_buffer)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageId page_id() const
  {
    if (!this->page_buffer_) {
      return llfs::PageId{};
    }

    return llfs::get_page_header(*this->page_buffer_).page_id.unpack();
  }

  explicit operator bool() const
  {
    return (bool)this->page_buffer_;
  }

  std::shared_ptr<llfs::PageBuffer> get_page_buffer() const
  {
    return this->page_buffer_;
  }

  llfs::ConstBuffer const_buffer() const
  {
    return llfs::get_const_buffer(this->page_buffer_);
  }

  llfs::ConstBuffer const_payload() const
  {
    return this->page_buffer_->const_payload();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::shared_ptr<llfs::PageBuffer> page_buffer_;
};

inline llfs::ConstBuffer get_page_const_payload(const FakePinnedPage& page) noexcept
{
  return page.const_payload();
}

}  // namespace testing
}  // namespace turtle_kv