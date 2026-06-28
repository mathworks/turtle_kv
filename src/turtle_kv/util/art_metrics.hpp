#pragma once
#define TURTLE_KV_UTIL_ART_METRICS_HPP

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>

namespace turtle_kv {

struct ARTMetrics {
  CountMetric<u64> construct_count;
  CountMetric<u64> destruct_count;
  FastCountMetric<u64> insert_count;
  FastCountMetric<u64> byte_alloc_count;
  FastCountMetric<u64> byte_free_count;

  /** \brief Resets all metrics to initial values.
   */
  void reset()
  {
    this->construct_count.reset();
    this->destruct_count.reset();
    this->insert_count.reset();
    this->byte_alloc_count.reset();
    this->byte_free_count.reset();
  }

  //----- --- -- -  -  -   -

  double bytes_per_instance() const
  {
    return (double)this->byte_alloc_count.get() / (double)this->construct_count.get();
  }

  double average_item_count() const
  {
    return (double)this->insert_count.get() / (double)this->construct_count.get();
  }

  double bytes_per_insert() const
  {
    return (double)this->byte_alloc_count.get() / (double)this->insert_count.get();
  }

  /** \brief Returns an estimate of the number of active instances (ART objects).
   */
  u64 instance_count() const
  {
    // Must be in this order!
    //
    const u64 observed_destruct_count = this->destruct_count.get();
    const u64 observed_construct_count = this->construct_count.get();

    return observed_construct_count - observed_destruct_count;
  }

  /** \brief Returns an estimate of the current number of bytes in use.
   */
  u64 bytes_in_use() const
  {
    // Must be in this order!
    //
    const u64 observed_free_count = this->byte_free_count.get();
    const u64 observed_alloc_count = this->byte_alloc_count.get();

    return observed_alloc_count - observed_free_count;
  }
};

}  // namespace turtle_kv
