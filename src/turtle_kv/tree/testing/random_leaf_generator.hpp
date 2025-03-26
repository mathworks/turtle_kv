#pragma once

#include <turtle_kv/tree/leaf_page_view.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/testing/fake_page_loader.hpp>
#include <turtle_kv/tree/testing/fake_pinned_page.hpp>

#include <turtle_kv/core/algo/compute_running_total.hpp>
#include <turtle_kv/core/algo/split_parts.hpp>
#include <turtle_kv/core/merge_compactor.hpp>
#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_id.hpp>

#include <atomic>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class RandomLeafGenerator
{
 public:
  using Self = RandomLeafGenerator;

  template <bool kDecayToItems>
  struct Result {
    MergeCompactor::ResultSet<kDecayToItems> result_set;
    std::vector<FakePinnedPage> leaf_pages;
    std::vector<llfs::PageId> leaf_page_ids;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static llfs::PageId make_fake_page_id() noexcept
  {
    static std::atomic<u64> next_page_id_int{1};
    return llfs::PageId{next_page_id_int.fetch_add(1)};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  RandomResultSetGenerator& items_generator() noexcept
  {
    return this->items_generator_;
  }

  template <bool kDecayToItems, typename Rng>
  Result<kDecayToItems> operator()(DecayToItem<kDecayToItems> decay_to_items, Rng& rng,
                                   FakePageLoader& fake_loader,
                                   llfs::StableStringStore& store) noexcept
  {
    Result<kDecayToItems> result;
    llfs::PageSize page_size = fake_loader.get_page_size();

    // Generate a sorted run of random key/value pairs.
    //
    result.result_set = this->items_generator_(decay_to_items, rng, store);

    batt::WorkerPool& worker_pool = batt::WorkerPool::null_pool();

    constexpr u32 kTrieIndexReserveSize = 16384;

    BATT_CHECK_GT(page_size, kTrieIndexReserveSize * 8);

    const llfs::PageSize effective_page_size{page_size - kTrieIndexReserveSize};

    // Compute a running total of packed sizes, so we can split the result set in to leaf pages.
    //
    batt::RunningTotal running_total =
        compute_running_total(worker_pool, result.result_set, DecayToItem<true>{});

    SplitParts page_parts = split_parts(       //
        running_total,                         //
        MinPartSize{effective_page_size / 4},  //
        MaxPartSize{effective_page_size},      //
        MaxItemSize{384});

    for (const Interval<usize>& part_extents : page_parts) {
      const auto items_slice = result.result_set.get();
      const auto page_items = batt::slice_range(items_slice, part_extents);

      // We need a fake id for our fake leaf.
      //
      llfs::PageId page_id = Self::make_fake_page_id();

      // Use the fake loader to allocate a page buffer that will be fake-loadable later on.
      //
      FakePinnedPage fake_pinned_page = BATT_OK_RESULT_OR_PANIC(fake_loader.get_page_with_layout(
          page_id, LeafPageView::page_layout_id(), llfs::OkIfNotFound{false}));

      // Grab the PageBuffer so we can build the page.
      //
      std::shared_ptr<llfs::PageBuffer> page_buffer = fake_pinned_page.get_page_buffer();

      auto page_plan = PackedLeafLayoutPlan::from_items(page_buffer->size(), page_items,
                                                        /*use_trie_index=*/true);

      // Build the page from the item slice assigned to it by the multi-page plan.
      //
      PackedLeafPage* packed_leaf_page = build_leaf_page(  //
          page_buffer->mutable_buffer(),                   //
          page_plan,                                       //
          page_items                                       //
      );

      BATT_CHECK_NOT_NULLPTR(packed_leaf_page);

      // Add the fake page and id to the result.
      //
      result.leaf_pages.emplace_back(std::move(fake_pinned_page));
      result.leaf_page_ids.emplace_back(page_id);
    }

    return result;
  }

 private:
  RandomResultSetGenerator items_generator_;
};

}  // namespace testing
}  // namespace turtle_kv
