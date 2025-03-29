#pragma once

#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_fn.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache_job.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/future.hpp>
#include <batteries/async/worker_pool.hpp>
#include <batteries/strong_typedef.hpp>

#include <atomic>
#include <vector>

namespace turtle_kv {

class TreeSerializeContext
{
 public:
  using Self = TreeSerializeContext;

  BATT_STRONG_TYPEDEF(usize, BuildPageJobId);

  using PinPageToJobFn =
      UniqueSmallFn<StatusOr<llfs::PinnedPage>(llfs::PageCacheJob& job,
                                               std::shared_ptr<llfs::PageBuffer>&& page_buffer)>;

  using BuildPageFn = UniqueSmallFn<StatusOr<PinPageToJobFn>(llfs::PageBuffer& page_buffer)>;

  struct BuildPageJob {
    llfs::PageSize page_size;
    llfs::PageLayoutId page_layout_id;
    BuildPageFn build_page_fn;
    batt::Promise<std::shared_ptr<llfs::PageBuffer>> new_page_promise;
    StatusOr<PinPageToJobFn> pin_page_fn;
    StatusOr<llfs::PinnedPage> pinned_page;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    BuildPageJob(const BuildPageJob&) = delete;
    BuildPageJob& operator=(const BuildPageJob&) = delete;

    BuildPageJob(BuildPageJob&&) = default;
    BuildPageJob& operator=(BuildPageJob&&) = default;

    explicit BuildPageJob(llfs::PageSize size,
                          const llfs::PageLayoutId layout,
                          BuildPageFn&& build_fn) noexcept
        : page_size{size}
        , page_layout_id{layout}
        , build_page_fn{std::move(build_fn)}
    {
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  TreeSerializeContext(const TreeSerializeContext&) = delete;
  TreeSerializeContext& operator=(const TreeSerializeContext&) = delete;

  explicit TreeSerializeContext(const TreeOptions& tree_options,
                                llfs::PageCacheJob& page_job,
                                batt::WorkerPool& worker_pool) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options() const
  {
    return this->tree_options_;
  }

  llfs::PageCacheJob& page_job()
  {
    return this->page_job_;
  }

  batt::WorkerPool& worker_pool()
  {
    return this->worker_pool_;
  }

  const batt::CancelToken& cancel_token() const
  {
    return this->cancel_token_;
  }

  StatusOr<BuildPageJobId> async_build_page(usize page_size,
                                            const llfs::PageLayoutId& page_layout_id,
                                            BuildPageFn&& build_page_fn);

  Status build_all_pages();

  StatusOr<llfs::PinnedPage> get_build_page_result(BuildPageJobId id);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void build_pages_task_fn();

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const TreeOptions& tree_options_;

  llfs::PageCacheJob& page_job_;

  batt::WorkerPool& worker_pool_;

  batt::CancelToken cancel_token_;

  std::vector<BuildPageJob> queue_;

  std::atomic<usize> next_input_{0};
};

}  // namespace turtle_kv
