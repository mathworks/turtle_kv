//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/mem_table/mem_table.hpp>
//
#include <turtle_kv/mem_table/mem_table.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/mem_table/mem_table.ipp>

#include <turtle_kv/mem_table/mock_mem_table_allocation_tracker.test.hpp>
#include <turtle_kv/mem_table/mock_mem_table_storage.test.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <turtle_kv/core/testing/generate.hpp>

#include <map>
#include <random>

namespace {

using namespace ::turtle_kv::int_types;
using namespace ::turtle_kv::constants;

using ::testing::AnyOf;
using ::testing::DoAll;
using ::testing::Eq;
using ::testing::Expectation;
using ::testing::Ge;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::StrictMock;

using ::turtle_kv::testing::MockMemTableAllocationTracker;
using ::turtle_kv::testing::MockMemTableStorage;
using ::turtle_kv::testing::RandomStringGenerator;
using MockBlockBuffer = ::turtle_kv::testing::MockMemTableStorage::BlockBuffer;

using ::turtle_kv::EditOffset;
using ::turtle_kv::EditOffsetDelta;
using ::turtle_kv::FirstVisitToBlock;
using ::turtle_kv::KeyView;
using ::turtle_kv::MemTableMetrics;
using ::turtle_kv::None;
using ::turtle_kv::OkStatus;
using ::turtle_kv::Optional;
using ::turtle_kv::OvercommitMetrics;
using ::turtle_kv::PackedSizeOfEdit;
using ::turtle_kv::Status;
using ::turtle_kv::StatusOr;
using ::turtle_kv::ValueView;

using MemTableWithMocks =
    ::turtle_kv::BasicMemTable<MockMemTableStorage, MockMemTableAllocationTracker>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MemTableTest : public ::testing::Test
{
 public:
  /** \brief The minimum length (bytes) for randomly generated keys.
   */
  static constexpr usize kMinKeyLen = 3;

  /** \brief The maximum length (bytes) for randomly generated keys.
   */
  static constexpr usize kMaxKeyLen = 25;

  /** \brief The minimum length (bytes) for randomly generated values.
   */
  static constexpr usize kMinValueLen = 32;

  /** \brief The maximum length (bytes) for randomly generated values.
   */
  static constexpr usize kMaxValueLen = 255;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs the MemTable object-under-test.
   */
  void SetUp() override
  {
    this->mem_table.emplace(this->allocation_tracker,
                            this->storage_writer,
                            this->mem_table_metrics,
                            EditOffset{0},
                            this->max_bytes_per_batch,
                            this->max_batch_count);

    EXPECT_CALL(this->block_buffer, ref_count())
        .WillRepeatedly(Return(this->block_buffer.fake_ref_count_));
  }

  /** \brief Destructs the MemTable object-under-test.
   */
  void TearDown() override
  {
    this->mem_table = None;
  }

  /** \brief Generates and returns a random key; the key bytes are stored in
   * this->stable_string_store.
   */
  KeyView make_random_key()
  {
    return this->random_key_generator(this->rng, this->stable_string_store);
  }

  /** \brief Generates and returns a random value; the value bytes are stored in
   * this->stable_string_store.
   */
  ValueView make_random_value()
  {
    return ValueView::from_str(this->random_value_generator(this->rng, this->stable_string_store));
  }

  /** \brief Adds a key/value pair to the expected_items collection.
   *
   * The underlying data pointed to by `key` and `value` *must* be stored in a location that will
   * outlive `this->expected_items` (e.g., `this->stable_string_store`).
   */
  void put_expected_item(const KeyView& key, const ValueView& value)
  {
    const auto [iter, inserted] = this->expected_items.emplace(key, value);
    if (!inserted) {
      iter->second = value;
      ++this->update_count;
    } else {
      ++this->insert_count;
    }
  }

  /** \brief Puts a key/value pair into the MemTable.
   *
   * \param dst_block_buffer The block buffer to pass to append_slot's callback fn.
   */
  Status put_into_mem_table(const KeyView& key,
                            const ValueView& value,
                            MockBlockBuffer* dst_block_buffer)
  {
    BATT_DEBUG_INFO(BATT_INSPECT(this->next_edit_offset));

    const usize item_size = this->update_max_item_size(key, value);
    const bool expect_overflow = (this->estimate_batch_count(item_size) > this->max_batch_count);

    if (!expect_overflow) {
      EXPECT_CALL(this->storage_writer_context,
                  append_slot(/*min_edit_offset_lower_bound=*/Eq(
                                  this->mem_table->edit_offset_lower_bound()),
                              /*byte_count=*/Ge(key.size() + value.size()),
                              /*wait_for_resource=*/::testing::_,
                              /*callback=*/::testing::_))
          .WillOnce(Invoke([this, dst_block_buffer](EditOffset /*min_edit_offset_lower_bound*/,
                                                    usize byte_count,
                                                    batt::WaitForResource,
                                                    auto&& callback_fn) -> Status {
            // Determine whether this is the first visit, and if so, set expectations that the
            // MemTable will add/remove a ref count.
            //
            auto first_visit = FirstVisitToBlock{this->next_edit_offset == 0};
            if (first_visit) {
              EXPECT_CALL(*dst_block_buffer, add_ref(1))  //
                  .WillOnce(Return());

              EXPECT_CALL(*dst_block_buffer, block_size())  //
                  .WillRepeatedly(Return(0));

              EXPECT_CALL(*dst_block_buffer, remove_ref(1))  //
                  .WillOnce(Return());
            }

            // Invoke MemTable's callback.
            //
            callback_fn(first_visit,
                        dst_block_buffer,
                        this->stable_string_store.allocate(byte_count),
                        EditOffset{this->next_edit_offset});

            // Update the fake EditOffset and slot count.
            //
            this->next_edit_offset += byte_count;
            dst_block_buffer->fake_slot_count_ += 1;

            return OkStatus();
          }));
    }

    Status status = this->mem_table->put(this->storage_writer_context, key, value);
    EXPECT_EQ((status == batt::StatusCode::kResourceExhausted), expect_overflow);

    if (status.ok()) {
      this->total_inserted_items_size += item_size;
    }

    Mock::VerifyAndClearExpectations(&this->storage_writer_context);

    return status;
  }

  /** \brief Returns a worst-case estimate of the number of batches which will be produced by the
   * current MemTable.
   */
  usize estimate_batch_count(usize next_item_size = 0) const
  {
    const usize max_wasted_bytes_per_batch = std::max(next_item_size, this->max_item_size) - 1;
    const usize worst_case_batch_size = this->max_bytes_per_batch - max_wasted_bytes_per_batch;
    const usize worst_case_batch_count = (this->total_inserted_items_size + next_item_size +  //
                                          worst_case_batch_size - 1)                          //
                                         / worst_case_batch_size;
    return worst_case_batch_count;
  }

  /** \brief Updates the maximum packed edit size; returns the item size.
   */
  usize update_max_item_size(const KeyView& key, const ValueView& value)
  {
    const usize item_size = PackedSizeOfEdit{}(key.size(), value.size());
    if (item_size > this->max_item_size) {
      this->max_item_size = item_size;
    }
    return item_size;
  }

  /** \brief Scans through `this->expected_items`, verifying that each key/value pair appears in
   * `this->mem_table` as it should.
   */
  void verify_items()
  {
    usize i = 0;
    for (const auto& [key, expected_value] : this->expected_items) {
      Optional<ValueView> actual_value = this->mem_table->get(key);

      ASSERT_TRUE(actual_value) << BATT_INSPECT_STR(key) << BATT_INSPECT(i);
      EXPECT_THAT(actual_value->as_str(), StrEq(expected_value.as_str()))
          << BATT_INSPECT(this->insert_count) << BATT_INSPECT(this->update_count);

      ++i;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief A pseudo-random number generator.
   */
  std::default_random_engine rng{/*seed=*/1};

  /** \brief Stores key/value strings, both for generated input and for fake log storage.
   */
  batt::StableStringStore stable_string_store;

  /** \brief Generates random keys.
   */
  RandomStringGenerator random_key_generator{kMinKeyLen, kMaxKeyLen};

  /** \brief Generates random values.
   */
  RandomStringGenerator random_value_generator{kMinValueLen, kMaxValueLen};

  /** \brief Tracks the number of times we see PageCache overcommits.
   */
  OvercommitMetrics overcommit_metrics;

  /** \brief MemTable requires a metrics struct.
   */
  MemTableMetrics mem_table_metrics{&overcommit_metrics};

  /** \brief The mock allocation tracker; this replaces the production impl which allocates space in
   * the PageCache for ChangeLogBlock buffers and the ART index.
   */
  StrictMock<MockMemTableAllocationTracker> allocation_tracker;

  /** \brief The mock storage writer; this replaces ChangeLogWriter.
   */
  StrictMock<MockMemTableStorage::Writer> storage_writer;

  /** \brief The mock storage writer context; this replaces ChangeLogWriter::Context.
   */
  StrictMock<MockMemTableStorage::WriterContext> storage_writer_context;

  /** \brief A mock block buffer; this replaces all ChangeLogBlock objects.
   */
  StrictMock<MockBlockBuffer> block_buffer;

  /** \brief The configured maximum batch size for the MemTable under test.
   */
  usize max_bytes_per_batch = 1 * kMiB;

  /** \brief The configured maximum batch count for the MemTable under test.
   */
  usize max_batch_count = 8;

  /** \brief This tracks the next EditOffset handed out to each slot as items are inserted/updated
   * in the MemTable.
   */
  i64 next_edit_offset = 0;

  /** \brief Tracks the maximum item size (packed key + value)
   */
  usize max_item_size = 0;

  /** \brief Tracks the total packed edits size in the current MemTable.
   */
  usize total_inserted_items_size = 0;

  /** \brief The object-under-test.
   */
  Optional<MemTableWithMocks> mem_table;

  /** \brief What we expect to find in the MemTable.
   */
  std::map<KeyView, ValueView> expected_items;

  /** \brief The number of insertions to expected_items.
   */
  usize insert_count = 0;

  /** \brief The number of updates to items already in expected_items.
   */
  usize update_count = 0;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(MemTableTest, CreateDestroy)
{
  // Verify the post-construction state of the MemTable.
  //
  EXPECT_FALSE(this->mem_table->is_finalized());
  EXPECT_EQ(this->mem_table->edit_offset_lower_bound(), EditOffset{0});
  EXPECT_EQ(this->mem_table->max_byte_size(),
            (this->max_bytes_per_batch - (MemTableWithMocks::kDefaultItemSize - 1)) *
                this->max_batch_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(MemTableTest, PutGet)
{
  // Perform a series of random puts, verifying every so often against expected_items as we go +
  // once more at the end.
  //
  constexpr usize kNumItems = 1000;
  constexpr usize kVerifyEvery = 25;

  for (usize i = 0; i < kNumItems; ++i) {
    KeyView key = this->make_random_key();
    ValueView value = this->make_random_value();

    this->put_expected_item(key, value);

    Status status = this->put_into_mem_table(key, value, &this->block_buffer);
    EXPECT_TRUE(status.ok()) << BATT_INSPECT(status);

    if ((i + 1) % kVerifyEvery == 0) {
      ASSERT_NO_FATAL_FAILURE(this->verify_items());
    }
  }

  EXPECT_GT(insert_count, 0);
  EXPECT_GT(update_count, 0);
  EXPECT_EQ(insert_count + update_count, kNumItems);

  ASSERT_NO_FATAL_FAILURE(this->verify_items());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(MemTableTest, PutUntilFull)
{
  usize total_key_bytes = 0;
  usize total_value_bytes = 0;
  usize put_count = 0;

  for (;;) {
    KeyView key = this->make_random_key();
    ValueView value = this->make_random_value();

    this->put_expected_item(key, value);

    Status status = this->put_into_mem_table(key, value, &this->block_buffer);

    if (!status.ok()) {
      EXPECT_EQ(status, batt::StatusCode::kResourceExhausted);
      break;
    }

    total_key_bytes += key.size();
    total_value_bytes += value.size();
    ++put_count;
  }
}

}  // namespace
