#pragma once

#include <turtle_kv/import/buffer.hpp>

namespace turtle_kv {
struct BufferBoundsChecker {
  MutableBuffer buffer;

  const void* buffer_begin() const
  {
    return this->buffer.data();
  }

  const void* buffer_end() const
  {
    return advance_pointer(this->buffer.data(), this->buffer.size());
  }

  template <typename T>
  bool contains(const T* ptr) const
  {
    return ((const void*)(ptr + 0) >= this->buffer_begin()) &&  //
           ((const void*)(ptr + 1) <= this->buffer_end());
  }
};
}  // namespace turtle_kv
