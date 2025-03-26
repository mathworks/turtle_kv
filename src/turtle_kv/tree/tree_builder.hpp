#pragma once

#include <turtle_kv/tree/pinning_page_loader.hpp>
#include <turtle_kv/tree/subtree.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/core/merge_compactor.hpp>

#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_loader.hpp>

#include <batteries/async/worker_pool.hpp>

namespace turtle_kv {

class TreeCheckpointJob
{
 public:
};

class TreeBuilder
{
 public:
  explicit TreeBuilder(batt::WorkerPool& worker_pool,  //
                       llfs::PageCache& page_cache,    //
                       const TreeOptions& tree_options) noexcept;

  TreeBuilder(const TreeBuilder&) = delete;
  TreeBuilder& operator=(const TreeBuilder&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const Subtree& get_tree() const noexcept
  {
    return this->tree_;
  }

  Subtree& get_tree() noexcept
  {
    return this->tree_;
  }

  Status apply_batch_update(
      MergeCompactor::ResultSet</*decay_to_items=*/false>&& result_set) noexcept;

  StatusOr<std::unique_ptr<llfs::PageCacheJob>> serialize(llfs::PageCache& page_cache) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Status grow_tree() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  batt::WorkerPool& worker_pool_;
  llfs::PageCache& page_cache_;
  Optional<PinningPageLoader> page_loader_;
  llfs::FinalizedPageCacheJob base_job_;
  TreeOptions tree_options_;
  Subtree tree_;
  i32 height_;
  std::atomic<batt::CancelToken*> cancel_token_;
};

}  // namespace turtle_kv
