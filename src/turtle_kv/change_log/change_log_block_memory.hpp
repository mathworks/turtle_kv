//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_CHANGE_LOG_BLOCK_MEMORY_HPP

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <utility>

namespace turtle_kv {

class ChangeLogBlockMemory
{
 public:
  using Self = ChangeLogBlockMemory;

  explicit ChangeLogBlockMemory(void* ptr, usize size) noexcept : buffer_{ptr, size}
  {
  }

  ChangeLogBlockMemory(const Self&) = delete;
  Self& operator=(const Self&) = delete;

  ChangeLogBlockMemory(Self&& other) noexcept : buffer_{std::exchange(other.buffer_, {})}
  {
  }

  Self& operator=(Self&& other) noexcept
  {
    if (this != &other) {
      // Free any memory currently owned by *this beore overwriting it.
      //
      if (this->buffer_.data() != nullptr) {
        free(this->buffer_.data());
      }

      this->buffer_ = std::exchange(other.buffer_, {});
    }
    return *this;
  }

  ~ChangeLogBlockMemory() noexcept
  {
    if (this->buffer_.data() != nullptr) {
      free(this->buffer_.data());
    }
  }

  void* data() const
  {
    return this->buffer_.data();
  }

  usize size() const
  {
    return this->buffer_.size();
  }

  MutableBuffer buffer() const
  {
    return this->buffer_;
  }

  void* release_ownership()
  {
    void* released_ptr = this->buffer_.data();
    this->buffer_ = MutableBuffer{};
    return released_ptr;
  }

 private:
  MutableBuffer buffer_;
};

}  // namespace turtle_kv
