#include <turtle_kv/tree/tree_serialize_context.hpp>
//

namespace turtle_kv {

using BuildPageJobId = TreeSerializeContext::BuildPageJobId;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ TreeSerializeContext::TreeSerializeContext(const TreeOptions& tree_options,
                                                        llfs::PageCacheJob& page_job,
                                                        batt::WorkerPool& worker_pool) noexcept
    : tree_options_{tree_options}
    , page_job_{page_job}
    , worker_pool_{worker_pool}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<BuildPageJobId> TreeSerializeContext::async_build_page(
    usize page_size,
    const llfs::PageLayoutId& page_layout_id,
    llfs::LruPriority cache_priority,
    usize task_count,
    BuildPageFn&& build_page_fn)
{
  BATT_CHECK_GT(task_count, 0);

  BuildPageJobId id{this->queue_.size()};

  if (task_count == 1) {
    this->queue_.emplace_back(llfs::PageSize{BATT_CHECKED_CAST(u32, page_size)},
                              page_layout_id,
                              cache_priority,
                              std::move(build_page_fn),
                              /*task_i=*/0);
  } else {
    this->queue_.emplace_back(llfs::PageSize{BATT_CHECKED_CAST(u32, page_size)},
                              page_layout_id,
                              cache_priority,
                              batt::make_copy(build_page_fn),
                              /*task_i=*/0);

    batt::Promise<std::shared_ptr<llfs::PageBuffer>> promise = this->queue_.back().new_page_promise;

    for (usize task_i = 1; task_i < task_count; ++task_i) {
      this->queue_.emplace_back(llfs::PageSize{BATT_CHECKED_CAST(u32, page_size)},
                                page_layout_id,
                                cache_priority,
                                batt::make_copy(build_page_fn),
                                task_i,
                                promise);
    }
  }

  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status TreeSerializeContext::build_all_pages()
{
  const usize queue_size = this->queue_.size();
  const usize n_threads = std::min(queue_size, this->worker_pool_.size());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    batt::ScopedWorkContext context{this->worker_pool_};

    for (usize thread_i = 0; thread_i < n_threads; ++thread_i) {
      BATT_REQUIRE_OK(context.async_run([this] {
        this->build_pages_task_fn();
      }));
    }

    for (usize queue_i = 0; queue_i < queue_size; ++queue_i) {
      BuildPageJob& build = this->queue_[queue_i];
      if (build.task_i != 0) {
        continue;
      }

      StatusOr<std::shared_ptr<llfs::PageBuffer>> page_buffer =
          this->page_job_.new_page(build.page_size,
                                   batt::WaitForResource::kTrue,
                                   build.page_layout_id,
                                   build.lru_priority,
                                   /*callers=*/0,
                                   this->cancel_token_);

      build.new_page_promise.set_value(std::move(page_buffer));
    }

    this->build_pages_task_fn();
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  for (usize queue_i = 0; queue_i < queue_size; ++queue_i) {
    BuildPageJob& build = this->queue_[queue_i];

    if (!build.pin_page_fn.ok()) {
      build.pinned_page = build.pin_page_fn.status();
      continue;
    }

    StatusOr<std::shared_ptr<llfs::PageBuffer>> new_page =
        build.new_page_promise.get_future().await();

    BATT_CHECK_OK(new_page);

    build.pinned_page = (*build.pin_page_fn)(this->page_job_, std::move(*new_page));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> TreeSerializeContext::get_build_page_result(BuildPageJobId id)
{
  BATT_CHECK_LT(id, this->queue_.size());
  return this->queue_[id].pinned_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeSerializeContext::build_pages_task_fn()
{
  const usize queue_size = this->queue_.size();

  for (;;) {
    const usize consumed_i = this->next_input_.fetch_add(1);
    if (consumed_i >= queue_size) {
      return;
    }

    BuildPageJob& build = this->queue_[consumed_i];

    StatusOr<std::shared_ptr<llfs::PageBuffer>> page_buffer =
        build.new_page_promise.get_future().await();

    if (!page_buffer.ok()) {
      build.pin_page_fn = page_buffer.status();
      continue;
    }

    build.pin_page_fn = build.build_page_fn(build.task_i, this->page_job_.cache(), **page_buffer);
  }
}

}  // namespace turtle_kv
