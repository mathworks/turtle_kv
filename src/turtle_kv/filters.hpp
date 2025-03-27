#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/bloom_filter.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/async/queue.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/async/worker_pool.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace turtle_kv {

class FilterBuilder : public llfs::PageFilterBuilder
{
 public:
  explicit FilterBuilder(const TreeOptions& tree_options, batt::WorkerPool& worker_pool) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool accepts_page(const llfs::PinnedPage& pinned_page) const noexcept override;

  Status build_filter(                                                 //
      const llfs::PinnedPage& src_pinned_page,                         //
      const std::shared_ptr<llfs::PageBuffer>& dst_filter_page_buffer  //
      ) noexcept override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const TreeOptions tree_options_;

  batt::WorkerPool& worker_pool_;
};

}  // namespace turtle_kv
