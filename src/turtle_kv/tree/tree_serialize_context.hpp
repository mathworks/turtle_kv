#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache_job.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/worker_pool.hpp>
#include <batteries/small_fn.hpp>

#include <atomic>
#include <vector>

namespace turtle_kv {

class TreeSerializeContext
{
 public:
  using Self = TreeSerializeContext;
  using BuildPageJobId = usize;
  using BuildPageJobFn = batt::UniqueSmallFn<StatusOr<llfs::PinnedPage>(TreeSerializeContext&)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  TreeSerializeContext(const TreeSerializeContext&) = delete;
  TreeSerializeContext& operator=(const TreeSerializeContext&) = delete;

  explicit TreeSerializeContext(const TreeOptions& tree_options, llfs::PageCacheJob& page_job,
                                batt::WorkerPool& worker_pool) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options() const noexcept;

  llfs::PageCacheJob& page_job() noexcept;

  batt::WorkerPool& worker_pool() noexcept;

  const batt::CancelToken& cancel_token() const noexcept;

  BuildPageJobId async_build_page(BuildPageJobFn&& build_page_fn) noexcept;

  Status build_all_pages() noexcept;

  StatusOr<llfs::PinnedPage> get_build_page_result(BuildPageJobId id) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void build_pages_task_fn() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options_;

  llfs::PageCacheJob& page_job_;

  batt::WorkerPool& worker_pool_;

  batt::CancelToken cancel_token_;

  std::vector<BuildPageJobFn> input_queue_;

  std::vector<StatusOr<llfs::PinnedPage>> output_queue_;

  std::atomic<usize> next_input_{0};
};

}  // namespace turtle_kv
