//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/change_log/change_log_file.hpp>
#include <turtle_kv/change_log/change_log_reader.hpp>
#include <turtle_kv/change_log/change_log_writer.hpp>
#include <turtle_kv/change_log/edit_offset.hpp>

#include <turtle_kv/data_root.test.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/env.hpp>
#include <batteries/require.hpp>

#include <filesystem>
#include <random>
#include <thread>
#include <unordered_set>

namespace turtle_kv {

class ChangeLogTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
    ASSERT_TRUE(root.ok());

    this->test_dir_ = *root / "turtle_kv_Test";
    this->test_file_ = this->test_dir_ / "test_change_log.log";

    std::filesystem::create_directories(this->test_dir_);
  }

  void TearDown() override
  {
    return;
  }

  /** \brief Creates and returns a `ChangeLogWriter` instance.
   */
  std::unique_ptr<ChangeLogWriter> create_writer(
      const ChangeLogFile::Config& config,
      const ChangeLogWriter::Options& options = ChangeLogWriter::Options::with_default_values(),
      RemoveExisting remove_existing = RemoveExisting{true})
  {
    StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
        ChangeLogWriter::open_or_create(this->test_file_, config, options, remove_existing);
    BATT_CHECK(writer.ok()) << BATT_INSPECT(writer.status());

    (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());
    return std::move(*writer);
  }

  /** \brief Halts the specified `ChangeLogWriter` instance. If specified, thi function first waits
   * for the writer to process appends before halting.
   */
  void shutdown_writer(std::unique_ptr<ChangeLogWriter>& writer, bool flush = true)
  {
    if (flush) {
      // Wait for writer to process appends before halting.
      //
      ASSERT_TRUE(writer->wait_for_flush());
    }

    writer->halt();
    writer->join();
  }

  using AppendCallback = std::function<void(FirstVisitToBlock first_visit,
                                            ChangeLogBlock* block,
                                            MutableBuffer buffer,
                                            EditOffset offset)>;

  /** \brief Appends the payload in `data` to a new slot within some `BlockBuffer` owned by the
   * specified `context`. Optionally takes in a callback function that would execute after the
   * data is copied into the slot when specified.
   */
  Status append_slot(ChangeLogWriter::Context& context,
                     const std::string& data,
                     EditOffset min_edit_offset_lower_bound = EditOffset{0},
                     batt::WaitForResource wait_for_resource = batt::WaitForResource::kTrue,
                     Optional<AppendCallback> callback_fn = None)
  {
    return context.append_slot(
        min_edit_offset_lower_bound,
        data.size(),
        wait_for_resource,
        [&data, &callback_fn](FirstVisitToBlock first_visit,
                              ChangeLogBlock* block,
                              MutableBuffer buffer,
                              EditOffset offset) {
          VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                  << ", on slot: " << offset;
          VLOG(1) << BATT_INSPECT(first_visit) << BATT_INSPECT(block->slot_count())
                  << BATT_INSPECT(block->edit_offset_range());

          std::memcpy(buffer.data(), data.data(), data.size());

          if (callback_fn) {
            (*callback_fn)(first_visit, block, buffer, offset);
          }
        });
  }

  /** \brief Opens a `ChangeLogReader` instance and visits slots with the `visitor_fn` function
   * specified.
   */
  template <typename VisitorFn>
    requires std::invocable<VisitorFn,
                            usize,
                            FirstVisitToBlock,
                            ChangeLogBlock*,
                            EditOffset,
                            ConstBuffer> &&
             std::same_as<std::invoke_result_t<VisitorFn,
                                               usize,
                                               FirstVisitToBlock,
                                               ChangeLogBlock*,
                                               EditOffset,
                                               ConstBuffer>,
                          Status>
  usize open_reader_and_visit(VisitorFn&& visitor_fn)
  {
    StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
    BATT_CHECK(reader.ok()) << BATT_INSPECT(reader.status());

    usize slots_read = 0;
    auto counting_visitor = [&](FirstVisitToBlock first_visit,
                                ChangeLogBlock* block,
                                EditOffset edit_offset,
                                ConstBuffer payload) -> Status {
      ++slots_read;
      return visitor_fn(slots_read, first_visit, block, edit_offset, payload);
    };

    batt::Status visit_status = (*reader)->visit_slots(counting_visitor).status();
    BATT_CHECK(visit_status.ok()) << BATT_INSPECT(visit_status);

    return slots_read;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::filesystem::path test_dir_;
  std::filesystem::path test_file_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, CreateAndOpenFile)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  Status status = ChangeLogFile::create(this->test_file_, config, RemoveExisting{true});
  ASSERT_TRUE(status.ok()) << BATT_INSPECT(status) << BATT_INSPECT(this->test_file_);

  StatusOr<std::unique_ptr<ChangeLogFile>> log_file = ChangeLogFile::open(this->test_file_);
  ASSERT_TRUE(log_file.ok());
  ASSERT_NE(log_file->get(), nullptr);

  EXPECT_EQ((*log_file)->config().block_size, config.block_size);
  EXPECT_EQ((*log_file)->config().block_count, config.block_count);
  EXPECT_EQ((*log_file)->config().block0_offset, config.block0_offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, WriterBasicOperations)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  ChangeLogWriter::Context context(*writer);

  // Write some test data
  //
  std::string test_data = "Hello, ChangeLog!";
  Status write_status = this->append_slot(context, test_data);
  ASSERT_TRUE(write_status.ok());

  this->shutdown_writer(writer);

  EXPECT_GT(writer->metrics().received_user_byte_count.load(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, WriteAndReadMultipleSlots)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_size = BlockSize{4096};
  config.block_count = BlockCount{10};
  std::vector<std::string> test_data = {"First slot data",
                                        "Second slot data with more content",
                                        "Third slot",
                                        "Fourth slot with even more data to test",
                                        "Fifth and final slot"};

  // Write phase
  //
  {
    std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

    ChangeLogWriter::Context context(*writer);

    // Write multiple slots
    //
    for (size_t i = 0; i < test_data.size(); ++i) {
      Status write_status = this->append_slot(context, test_data[i]);
      ASSERT_TRUE(write_status.ok()) << "Failed to write slot " << i;
    }

    this->shutdown_writer(writer);
  }

  // Read phase
  //
  {
    std::vector<std::string> read_data;
    std::vector<EditOffset> edit_offsets;

    usize slots_read = this->open_reader_and_visit([&](usize,
                                                       FirstVisitToBlock first_visit,
                                                       ChangeLogBlock* block,
                                                       EditOffset edit_offset,
                                                       ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset;
      VLOG(1) << BATT_INSPECT(first_visit) << BATT_INSPECT(block->slot_count())
              << BATT_INSPECT(block->edit_offset_range());
      read_data.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
      edit_offsets.push_back(edit_offset);
      return OkStatus();
    });

    EXPECT_EQ(slots_read, 5);

    // Verify we read all slots.
    //
    EXPECT_EQ(read_data.size(), 5);

    // Verify data matches.
    //
    std::set<std::string> expected_set(test_data.begin(), test_data.end());
    std::set<std::string> actual_set(read_data.begin(), read_data.end());
    EXPECT_EQ(expected_set, actual_set);

    // Verify edit offsets are in ascending order. Only true in single threaded environment.
    //
    for (size_t i = 1; i < edit_offsets.size(); ++i) {
      EXPECT_LT(edit_offsets[i - 1], edit_offsets[i]);
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, ConcurrentWritesMultipleContexts)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_count = BlockCount{20};
  const int num_threads = 4;
  const int slots_per_thread = 10;
  batt::Mutex<std::unordered_set<i64>> offsets;

  // Write Phase
  //
  {
    std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

    std::vector<std::thread> threads;
    std::atomic<int> total_writes{0};

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&, thread_id = t]() {
        ChangeLogWriter::Context context(*writer);

        for (int i = 0; i < slots_per_thread; ++i) {
          std::string data = "Thread " + std::to_string(thread_id) + " Slot " + std::to_string(i);

          Status write_status = this->append_slot(
              context,
              data,
              EditOffset{0},
              batt::WaitForResource::kTrue,
              [&offsets](FirstVisitToBlock, ChangeLogBlock*, MutableBuffer, EditOffset offset) {
                batt::ScopedLock<std::unordered_set<i64>> locked_offsets{offsets};
                locked_offsets->insert(offset.value());
              });

          if (write_status.ok()) {
            total_writes.fetch_add(1);
          }
        }
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    this->shutdown_writer(writer);

    EXPECT_EQ(total_writes.load(), num_threads * slots_per_thread);
  }

  // Read Phase
  //
  {
    usize slots_read = this->open_reader_and_visit([&](usize,
                                                       FirstVisitToBlock,
                                                       ChangeLogBlock* block,
                                                       EditOffset edit_offset,
                                                       ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset << ", payload size: " << payload.size();

      batt::ScopedLock<std::unordered_set<i64>> locked_offsets{offsets};

      // Check that edit_offset was in the set of offsets we wrote
      //
      BATT_REQUIRE_NE(locked_offsets->find(edit_offset.value()), locked_offsets->end());
      locked_offsets->erase(edit_offset.value());
      return OkStatus();
    });

    EXPECT_EQ(slots_read, num_threads * slots_per_thread);

    // Verify that we read all the offsets we wrote.
    //
    batt::ScopedLock<std::unordered_set<i64>> locked_offsets{offsets};
    EXPECT_EQ(locked_offsets->size(), 0);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, BlockBoundaryConditions)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_size = BlockSize{1024};  // Small blocks to test boundaries
  config.block_count = BlockCount{5};
  int num_appends = 6;

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  ChangeLogWriter::Context context(*writer);

  // Write data that will span multiple blocks
  //
  std::string large_data(900, 'X');  // Almost fills a block

  for (int i = 0; i < num_appends; ++i) {
    Status write_status = this->append_slot(
        context,
        large_data,
        EditOffset{0},
        batt::WaitForResource::kTrue,
        [&, i](FirstVisitToBlock, ChangeLogBlock*, MutableBuffer, EditOffset offset) {
          writer->trim(offset + EditOffsetDelta{(i64)large_data.size()}).IgnoreError();
        });

    ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);
  }

  this->shutdown_writer(writer);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, ReadEmptyLog)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  // Don't write anything, just close
  //
  writer.reset();

  // Try to read
  //

  usize slots_read = this->open_reader_and_visit([&](usize,
                                                     FirstVisitToBlock,
                                                     ChangeLogBlock* block,
                                                     EditOffset edit_offset,
                                                     ConstBuffer payload) -> Status {
    VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
            << ", on slot: " << edit_offset << ", payload size: " << payload.size();
    return OkStatus();
  });

  EXPECT_EQ(slots_read, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, ExceedCapacityWrapAround)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_size = BlockSize{4096};
  config.block_count = BlockCount{8};  // Small capacity to test wrap-around

  const i64 total_capacity = config.block_size * config.block_count;
  const i64 target_data_size = total_capacity * 2.5;  // Write 2.5x the capacity

  // Generate varying sizes of data
  //
  std::mt19937 rng(42);  // Fixed seed for reproducibility
  std::uniform_int_distribution<usize> size_dist(100, 2000);

  std::map<i64, i64> offsets;
  i64 total_written = 0;
  i64 successful_writes = 0;
  i64 slots_trimmed = 0;

  // Write Phase
  //
  {
    std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

    ChangeLogWriter::Context context(*writer);

    i64 expected_next_offset = 0;

    // Keep writing until we've written target_data_size
    //
    while (total_written < target_data_size) {
      usize slot_size = size_dist(rng);
      std::string slot_data(slot_size, 'A');

      Status write_status = batt::StatusCode::kUnknown;

      for (;;) {
        write_status = this->append_slot(
            context,
            slot_data,
            EditOffset{0},
            offsets.empty() ? batt::WaitForResource::kTrue : batt::WaitForResource::kFalse,
            [&](FirstVisitToBlock, ChangeLogBlock*, MutableBuffer, EditOffset offset) {
              BATT_CHECK_EQ(offset.value(), expected_next_offset);
              expected_next_offset += slot_data.size();
              offsets.emplace(offset.value(), offset.value() + (i64)slot_data.size());
            });

        if (write_status.ok() || write_status != batt::StatusCode::kGrantUnavailable ||
            offsets.empty()) {
          break;
        }

        // If the append failed because we ran out of blocks, then try trimming the next offset and
        // retrying.
        //
        VLOG(1) << "trimming to " << offsets.begin()->second;
        Status trim_status = writer->trim(EditOffset{offsets.begin()->second});
        ASSERT_TRUE(trim_status.ok()) << BATT_INSPECT(trim_status);
        offsets.erase(offsets.begin());
        ++slots_trimmed;
      }

      if (write_status.ok()) {
        successful_writes++;
        total_written += slot_size;
      } else {
        ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status) << BATT_INSPECT_RANGE(offsets);
      }
    }

    // Give writer time to flush remaining data
    //
    this->shutdown_writer(writer);

    LOG(INFO) << "Wrap-around test stats:"
              << " total_written=" << total_written << " capacity=" << total_capacity;

    // Verify we wrote significantly more than capacity
    //
    EXPECT_GT(total_written, total_capacity * 2);
    EXPECT_GT(successful_writes, 0);

    auto& metrics = writer->metrics();
    EXPECT_GT(metrics.written_user_byte_count.load(), 0);
    EXPECT_GT(metrics.write_count.load(), 0);
    EXPECT_GT(ChangeLogBlock::metrics().block_alloc_count.get(), config.block_count.value());
  }

  // Read Phase
  //
  {
    std::unordered_set<i64> unique_blocks;

    usize slots_read = this->open_reader_and_visit([&](usize,
                                                       FirstVisitToBlock first_visit,
                                                       ChangeLogBlock* block,
                                                       EditOffset edit_offset,
                                                       ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset << ", payload size: " << payload.size()
              << BATT_INSPECT(first_visit) << BATT_INSPECT(block->get_block_index());

      BATT_REQUIRE_NE(offsets.find(edit_offset.value()), offsets.end());
      offsets.erase(edit_offset.value());

      unique_blocks.insert(block->edit_offset_lower_bound().value());
      return OkStatus();
    });

    EXPECT_GT(slots_read, 0);
    EXPECT_LE(unique_blocks.size(), config.block_count.value());
    EXPECT_EQ(slots_read, successful_writes - slots_trimmed);
    EXPECT_TRUE(offsets.empty());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, CorruptBlockInMiddle)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_size = BlockSize{4096};
  config.block_count = BlockCount{10};

  const usize data_size_per_slot = 4000;
  const int num_blocks_to_write = 5;

  std::vector<std::string> test_data;
  for (int i = 0; i < num_blocks_to_write; ++i) {
    test_data.push_back(std::string(data_size_per_slot, 'A' + i));
  }

  // Write phase
  //
  {
    std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

    ChangeLogWriter::Context context(*writer);

    // Write slots s.t. each block has one slot
    //
    for (int i = 0; i < num_blocks_to_write; ++i) {
      Status write_status = this->append_slot(context, test_data[i]);

      ASSERT_TRUE(write_status.ok()) << "Failed to write slot " << i;
    }

    this->shutdown_writer(writer);
  }

  // Corrupt a block by overwriting its magic number
  //
  const i64 corrupt_block_index = num_blocks_to_write / 2;
  {
    StatusOr<int> fd = llfs::open_file_read_write(this->test_file_.string(),
                                                  llfs::OpenForAppend{false},
                                                  llfs::OpenRawIO{false});
    ASSERT_TRUE(fd.ok());

    auto on_scope_exit = batt::finally([fd] {
      llfs::close_fd(*fd).IgnoreError();
    });

    const i64 corrupt_block_offset =
        config.block0_offset + (corrupt_block_index * config.block_size);

    LOG(INFO) << "Corrupting block at index " << corrupt_block_index
              << ", file offset: " << corrupt_block_offset;

    big_u64 invalid_magic = 0xDEADBEEFBADC0DEEull;

    // Write the invalid magic number at the start of the block
    //
    Status write_status = llfs::write_fd(*fd,
                                         ConstBuffer{&invalid_magic, sizeof(invalid_magic)},
                                         corrupt_block_offset);
    ASSERT_TRUE(write_status.ok()) << "Failed to corrupt block: " << write_status;

    // TODO: [Gabe Bornstein 4/3/26] Consider updating the corrupt block
    // s.t. it appears to have been written after all other blocks (higher edit offset lower bound).
    // We still need to recover all other blocks in this case.
    //
  }

  // Read phase - ChangeLogReader should handle the corrupt block gracefully
  //
  {
    std::vector<EditOffset> recovered_offsets;

    usize slots_read = open_reader_and_visit([&](usize slot_index,
                                                 FirstVisitToBlock,
                                                 ChangeLogBlock* block,
                                                 EditOffset edit_offset,
                                                 ConstBuffer payload) -> Status {
      recovered_offsets.push_back(edit_offset);

      LOG(INFO) << "Post-corruption read: slot " << slot_index << " at edit_offset: " << edit_offset
                << ", block lower_bound: " << block->edit_offset_lower_bound()
                << ", payload size: " << payload.size();

      return OkStatus();
    });

    EXPECT_EQ(slots_read, corrupt_block_index)
        << "Expected to read " << corrupt_block_index
        << " slots (from blocks before corruption), but read " << slots_read;

    EXPECT_EQ(recovered_offsets.size(), corrupt_block_index);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, Sync)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  ChangeLogWriter::Context context(*writer);

  // Append a slot and capture the edit offset after it.
  //
  std::string test_data = "Slot data for sync test";
  Status write_status = this->append_slot(context, test_data);
  ASSERT_TRUE(write_status.ok());

  const EditOffset target = writer->next_edit_offset();

  Status sync_status = writer->sync(target);
  EXPECT_TRUE(sync_status.ok()) << BATT_INSPECT(sync_status);

  // After sync returns, durable_upper_bound must return target.
  //
  EXPECT_EQ(writer->durable_upper_bound().value(), target.value());

  this->shutdown_writer(writer, /*flush=*/false);

  LOG(INFO) << BATT_INSPECT(writer->metrics().advance_sync_upper_bound_latency);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, MultipleSync)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_count = BlockCount{20};

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  ChangeLogWriter::Context context(*writer);

  // Append a slot so we have a target offset to sync to.
  //
  std::string test_data = "Multiple sync test slot";
  Status write_status = this->append_slot(context, test_data);
  ASSERT_TRUE(write_status.ok());

  const EditOffset target = writer->next_edit_offset();
  const usize num_waiters = std::thread::hardware_concurrency();

  std::atomic<bool> start{false};
  std::atomic<usize> completed{0};

  std::vector<std::thread> threads;
  threads.reserve(num_waiters);

  for (usize i = 0; i < num_waiters; ++i) {
    threads.emplace_back([&]() {
      while (!start.load()) {
        continue;
      }

      Status s = writer->sync(target);
      EXPECT_TRUE(s.ok()) << BATT_INSPECT(s);

      completed.fetch_add(1);
    });
  }

  start.store(true);

  for (auto& t : threads) {
    t.join();
  }

  // All threads must have completed successfully.
  //
  EXPECT_EQ(completed.load(), num_waiters);

  this->shutdown_writer(writer, /*flush=*/false);

  LOG(INFO) << BATT_INSPECT(writer->metrics().advance_sync_upper_bound_latency);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, SyncStaggeredOffsets)
{
  // Use small blocks with large payloads to force slots into separate blocks and
  // separate write batches.
  //
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  config.block_size = BlockSize{512};
  config.block_count = BlockCount{20};

  std::unique_ptr<ChangeLogWriter> writer = this->create_writer(config);

  const usize num_slots = 4;
  const usize slot_size = 400;

  // Pre-compute the target offsets.
  //
  std::vector<EditOffset> targets;
  targets.reserve(num_slots);
  for (usize i = 0; i < num_slots; ++i) {
    targets.push_back(EditOffset{(i64)(i + 1) * (i64)slot_size});
  }

  // Launch sync threads before appending data, so they block on await_true.
  //
  std::vector<std::atomic<i64>> completion_order(num_slots);
  std::atomic<i64> completion_counter{0};

  std::vector<std::thread> sync_threads;
  sync_threads.reserve(num_slots);

  for (usize i = 0; i < num_slots; ++i) {
    sync_threads.emplace_back([&, i]() {
      Status s = writer->sync(targets[i]);
      EXPECT_TRUE(s.ok()) << BATT_INSPECT(s);

      completion_order[i].store(completion_counter.fetch_add(1));
    });
  }

  // Append slots one at a time, waiting for each to flush before appending the next.
  //
  std::thread appender([&]() {
    ChangeLogWriter::Context context(*writer);

    for (usize i = 0; i < num_slots; ++i) {
      std::string data(slot_size, 'A' + i);
      Status write_status = this->append_slot(context, data);
      ASSERT_TRUE(write_status.ok());
      ASSERT_TRUE(writer->wait_for_flush());
    }
  });

  appender.join();

  for (auto& t : sync_threads) {
    t.join();
  }

  // Verify that a thread waiting on a smaller offset must complete no later than
  // a thread waiting on a larger offset.
  //
  for (usize i = 0; i < num_slots; ++i) {
    for (usize j = i + 1; j < num_slots; ++j) {
      EXPECT_LE(completion_order[i].load(), completion_order[j].load())
          << "Thread waiting on offset " << targets[i] << " completed after thread waiting on "
          << targets[j];
    }
  }

  this->shutdown_writer(writer, /*flush=*/false);

  LOG(INFO) << BATT_INSPECT(writer->metrics().advance_sync_upper_bound_latency);
}

}  // namespace turtle_kv
