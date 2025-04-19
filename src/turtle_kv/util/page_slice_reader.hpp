#pragma once

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/page_size.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/sharded_page_view.hpp>
#include <llfs/stable_string_store.hpp>

#include <type_traits>
#include <variant>

namespace turtle_kv {

/** \brief A scoped object which pins data returned from a PageSliceReader.
 */
struct PageSliceStorage {
  llfs::BasicStableStringStore<512, 4096> stable_string_store;
  SmallVec<llfs::PinnedPage, 8> pinned_pages;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Helper to assist in loading small slices of a page (leaf) using LLFS sharded views.
 */
class PageSliceReader
{
 public:
  /** \brief Constructs a new PageSliceReader which can read page slices from the given page via
   * the given loader, using sharded view reads of the specified size (unless overridden in a
   * specific call to `this->read_slice`.
   */
  explicit PageSliceReader(llfs::PageLoader& page_loader,
                           llfs::PageId page_id,
                           llfs::PageSize default_shard_size) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Loads the requested page slice using sharded views of the specified size.
   *
   * The read data is returned by reference (zero-copy); therefore the caller must make sure to
   * keep `storage_out` in scope while it is reading the returned data.
   */
  StatusOr<ConstBuffer> read_slice(
      llfs::PageSize shard_size,
      const Interval<usize>& slice,
      PageSliceStorage& storage_out,
      llfs::PinPageToJob pin_page_to_job = llfs::PinPageToJob::kDefault) const;

  /** \brief Loads the requested page slice using the default shard size passed in at construction
   * time.
   */
  StatusOr<ConstBuffer> read_slice(
      const Interval<usize>& slice,
      PageSliceStorage& storage_out,
      llfs::PinPageToJob pin_page_to_job = llfs::PinPageToJob::kDefault) const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  llfs::PageLoader& page_loader_;
  llfs::PageCache& page_cache_;
  llfs::PageId page_id_;
  llfs::PageSize default_shard_size_;
};

}  // namespace turtle_kv
