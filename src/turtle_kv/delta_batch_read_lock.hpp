#pragma once

#include <turtle_kv/import/int_types.hpp>

namespace turtle_kv {

class DeltaBatchReadLock
{
 public:
  DeltaBatchReadLock(const DeltaBatchReadLock&) = delete;
  DeltaBatchReadLock& operator=(const DeltaBatchReadLock&) = delete;

  virtual ~DeltaBatchReadLock() = default;

 protected:
  DeltaBatchReadLock() = default;
};

}  // namespace turtle_kv
