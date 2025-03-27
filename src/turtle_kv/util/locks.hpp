#pragma once

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Adapter for RAII-style mutex locks that expect a _pointer_ (vs a reference) to the mutex
 * object.
 *
 * \param T scoped lock guard type that expects a pointer
 */
template <typename T>
struct PtrLock {
  T lock_;

  template <typename M>
  explicit PtrLock(M& mutex) noexcept : lock_{&mutex}
  {
  }
};

}  // namespace turtle_kv
