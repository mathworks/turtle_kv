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
const TreeOptions& TreeSerializeContext::tree_options() const noexcept
{
  return this->tree_options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
llfs::PageCacheJob& TreeSerializeContext::page_job() noexcept
{
  return this->page_job_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::WorkerPool& TreeSerializeContext::worker_pool() noexcept
{
  return this->worker_pool_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const batt::CancelToken& TreeSerializeContext::cancel_token() const noexcept
{
  return this->cancel_token_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BuildPageJobId TreeSerializeContext::async_build_page(BuildPageJobFn&& build_page_fn) noexcept
{
  BuildPageJobId id = this->input_queue_.size();

  this->input_queue_.emplace_back(std::move(build_page_fn));
  this->output_queue_.emplace_back();

  return id;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status TreeSerializeContext::build_all_pages() noexcept
{
  const usize n_threads = this->worker_pool_.size();
  batt::ScopedWorkContext context{this->worker_pool_};

  for (usize i = 0; i < n_threads; ++i) {
    BATT_REQUIRE_OK(context.async_run([this] {
      this->build_pages_task_fn();
    }));
  }

  this->build_pages_task_fn();

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> TreeSerializeContext::get_build_page_result(BuildPageJobId id) noexcept
{
  BATT_CHECK_LT(id, this->output_queue_.size());
  return this->output_queue_[id];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void TreeSerializeContext::build_pages_task_fn() noexcept
{
  for (;;) {
    const usize consumed = this->next_input_.fetch_add(1);
    if (consumed >= this->input_queue_.size()) {
      return;
    }

    this->output_queue_[consumed] = this->input_queue_[consumed](*this);
  }
}

}  // namespace turtle_kv
