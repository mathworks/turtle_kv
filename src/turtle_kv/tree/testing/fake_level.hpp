#pragma once

#include <turtle_kv/tree/testing/fake_segment.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/assert.hpp>

#include <vector>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct FakeLevel {
  using Segment = FakeSegment;

  std::vector<Segment> segments_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize segment_count() const noexcept
  {
    return this->segments_.size();
  }

  Segment& get_segment(usize i) noexcept
  {
    BATT_CHECK_LT(i, this->segments_.size());

    return this->segments_[i];
  }

  const Segment& get_segment(usize i) const noexcept
  {
    BATT_CHECK_LT(i, this->segments_.size());

    return this->segments_[i];
  }

  Segment& append_segment() noexcept
  {
    this->segments_.emplace_back();
    return this->segments_.back();
  }

  void drop_segment(usize i) noexcept
  {
    this->segments_.erase(this->segments_.begin() + i);
  }
};

}  // namespace testing
}  // namespace turtle_kv
