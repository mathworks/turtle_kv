#include <turtle_kv/tree/sharded_leaf_page_scanner.hpp>
//
#include <turtle_kv/tree/sharded_leaf_page_scanner.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

#include <turtle_kv/tree/memory_storage.hpp>
#include <turtle_kv/tree/packed_leaf_page.hpp>

#include <llfs/page_cache.hpp>

#include <batteries/constants.hpp>
#include <batteries/int_types.hpp>

#include <algorithm>
#include <memory>
#include <vector>

namespace {

using namespace batt::int_types;
using namespace batt::constants;

using turtle_kv::testing::RandomStringGenerator;
using turtle_kv::testing::SequentialStringGenerator;

using turtle_kv::build_leaf_page;
using turtle_kv::EditView;
using turtle_kv::KeyEqual;
using turtle_kv::KeyOrder;
using turtle_kv::make_memory_page_cache;
using turtle_kv::PackedLeafLayoutPlan;
using turtle_kv::PackedLeafLayoutPlanBuilder;
using turtle_kv::PackedLeafPage;
using turtle_kv::ShardedLeafPageScanner;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::TreeOptions;
using turtle_kv::ValueView;

using llfs::LruPriority;
using llfs::PageSize;
using llfs::PinnedPage;
using llfs::StableStringStore;

using batt::MutableBuffer;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ShardedLeafPageScannerTest : public ::testing::Test
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::vector<EditView> random_edits()
  {
    std::vector<EditView> edits;

    usize space = this->tree_options.flush_size();
    const usize edit_size = this->tree_options.expected_item_size();

    RandomStringGenerator generate_key;
    generate_key.set_size(this->tree_options.key_size_hint());

    SequentialStringGenerator generate_value{this->tree_options.value_size_hint()};

    while (space >= edit_size) {
      edits.emplace_back(
          EditView{generate_key(this->rng, this->string_storage),
                   ValueView::from_str(this->string_storage.store(generate_value()))});
      space -= edit_size;
    }

    // Sort and deduplicate.
    //
    std::sort(edits.begin(), edits.end(), KeyOrder{});
    edits.erase(std::unique(edits.begin(), edits.end(), KeyEqual{}), edits.end());

    return edits;
  }

  bool pick_branch(int percent = 50)
  {
    std::uniform_int_distribution<int> pick_pct{0, 99};
    return pick_pct(this->rng) < percent;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::default_random_engine rng{1};

  StableStringStore string_storage;

  TreeOptions tree_options = TreeOptions::with_default_values()  //
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(17)
                                 .set_value_size_hint(61);

  std::shared_ptr<llfs::PageCache> page_cache =
      make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                             this->tree_options,
                             /*byte_capacity=*/5000 * kMiB);
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ShardedLeafPageScannerTest, Test)
{
  DLOG(INFO) << BATT_INSPECT(tree_options.trie_index_sharded_view_size());

  for (usize i = 0; i < 8000; ++i) {
    auto batch = this->random_edits();
    ASSERT_TRUE(std::is_sorted(batch.begin(), batch.end(), KeyOrder{}));
    for (usize j = 1; j < batch.size(); ++j) {
      ASSERT_NE(batch[j - 1], batch[j]);
    }

    StatusOr<PinnedPage> pinned_page =
        this->page_cache->allocate_page_of_size(this->tree_options.leaf_size(),
                                                batt::WaitForResource{true},
                                                LruPriority{1},
                                                /*callers=*/0,
                                                /*job_id=*/0);

    ASSERT_TRUE(pinned_page.ok());

    MutableBuffer buffer =
        BATT_OK_RESULT_OR_PANIC(pinned_page->get()->get_new_page_buffer())->mutable_buffer();

    ASSERT_EQ(buffer.size(), this->tree_options.leaf_size());

    // Build a plan.
    //
    PackedLeafLayoutPlanBuilder plan_builder;
    {
      plan_builder.page_size = this->tree_options.leaf_size();
      plan_builder.trie_index_reserved_size = this->tree_options.trie_index_reserve_size();
    }
    for (const EditView& edit : batch) {
      plan_builder.add(edit.key, edit.value);
    }
    PackedLeafLayoutPlan plan = plan_builder.build();

    // Build the leaf.
    //
    PackedLeafPage* packed_leaf = build_leaf_page(buffer, plan, batch);

    ASSERT_NE(packed_leaf, nullptr);
    EXPECT_EQ(packed_leaf->key_count, batch.size());
    EXPECT_EQ(packed_leaf->min_key(), get_key(batch.front()));
    EXPECT_EQ(packed_leaf->max_key(), get_key(batch.back()));

    for (usize i = 0; i < packed_leaf->key_count; ++i) {
      ASSERT_EQ(packed_leaf->key_at(i), get_key(batch[i]));
      ASSERT_EQ(packed_leaf->value_at(i), get_value(batch[i]));
    }

    bool written = false;
    this->page_cache->async_write_new_page(batt::make_copy(*pinned_page), [&written](auto result) {
      written = result.ok();
    });
    ASSERT_TRUE(written);

    RandomStringGenerator generate_key;
    generate_key.set_size(this->tree_options.key_size_hint());

    // Now do some short scans and verify.
    //
    for (usize j = 0; j < 5000; ++j) {
      ShardedLeafPageScanner scanner{*this->page_cache, pinned_page->page_id(), this->tree_options};
      DLOG(INFO) << "load_header";
      Status header_status = scanner.load_header();
      ASSERT_TRUE(header_status.ok()) << BATT_INSPECT(header_status);

      for (usize j2 = 0; j2 < 100; ++j2) {
        std::string key;

        if (this->pick_branch()) {
          // Scan to a random key that *is* in the batch.
          //
          std::uniform_int_distribution<usize> pick_item{0, batch.size() - 1};
          key.assign(get_key(batch[pick_item(rng)]));

        } else {
          // Scan to a new random key.
          //
          key = generate_key(rng);
        }

        // Find the search key's lower bound in the actual batch.
        //
        auto actual_iter = std::lower_bound(batch.begin(), batch.end(), key, KeyOrder{});

        // Seek to the first key.
        //
        DLOG(INFO) << "seek_to(" << batt::c_str_literal(key) << ")";
        Status seek_status = scanner.seek_to(key);
        if (seek_status == batt::StatusCode::kEndOfStream) {
          ASSERT_EQ(actual_iter, batch.end());
          continue;
        }
        ASSERT_TRUE(seek_status.ok()) << BATT_INSPECT(seek_status);
        DLOG(INFO) << "(seek OK; verifying keys)";

        bool first_in_range = true;

        usize max_scan_len = this->pick_branch() ? (batch.size() + 1) / 2 : 100;

        // Pick a random scan length.
        //
        std::uniform_int_distribution<usize> pick_scan_len{1, max_scan_len};
        const usize scan_len = pick_scan_len(rng);
        DLOG(INFO) << BATT_INSPECT(scan_len);

        for (usize k = 0; k < scan_len && actual_iter != batch.end(); ++k, ++actual_iter) {
          if (first_in_range) {
            ASSERT_FALSE(scanner.item_range_empty());
            first_in_range = false;
          }

          ASSERT_EQ(scanner.front_key(), get_key(*actual_iter));

          DLOG(INFO) << "verified key [" << k << "]: " << batt::c_str_literal(scanner.front_key());

          scanner.drop_front();
          if (scanner.item_range_empty()) {
            DLOG(INFO) << "next_item_range";
            Status next_status = scanner.load_next_item_range();
            if (next_status == batt::StatusCode::kEndOfStream) {
              ASSERT_EQ(std::next(actual_iter), batch.end());
              break;
            }
            ASSERT_TRUE(next_status.ok()) << BATT_INSPECT(next_status);
            first_in_range = true;
          }
        }
      }
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

}  // namespace
