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
  }

  void TearDown() override
  {
    return;
  }

  void on_visit_block(FirstVisitToBlock first_visit, ChangeLogBlock* block)
  {
    BATT_CHECK_NOT_NULLPTR(block);

    EXPECT_EQ(first_visit,
              this->visited_blocks_.count(
                  std::make_pair(block, block->edit_offset_lower_bound().value())) == 0);

    this->insert_visited_block(block);
  }

  void insert_visited_block(ChangeLogBlock* block)
  {
    BATT_CHECK_NOT_NULLPTR(block);
    this->visited_blocks_.insert(std::make_pair(block, block->edit_offset_lower_bound().value()));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::filesystem::path test_dir_;
  std::filesystem::path test_file_;
  std::set<std::pair<ChangeLogBlock*, i64>> visited_blocks_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, CreateAndOpenFile)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();
  ASSERT_TRUE(ChangeLogFile::create(this->test_file_, config, RemoveExisting{true}).ok());

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
  ChangeLogWriter::Options options = ChangeLogWriter::Options::with_default_values();

  StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
      ChangeLogWriter::open_or_create(this->test_file_, config, options, RemoveExisting{true});
  ASSERT_TRUE(writer.ok());

  (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

  ChangeLogWriter::Context context(**writer);

  // Write some test data
  //
  std::string test_data = "Hello, ChangeLog!";
  Status write_status = context.append_slot(
      test_data.size(),
      [&test_data, this](FirstVisitToBlock first_visit,
                         ChangeLogBlock* block,
                         MutableBuffer buffer,
                         EditOffset offset) {
        VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                << ", on slot: " << offset;
        this->on_visit_block(first_visit, block);
        std::memcpy(buffer.data(), test_data.data(), test_data.size());
      });
  ASSERT_TRUE(write_status.ok());

  // Wait for writer to process appends before halting.
  //
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  (*writer)->halt();
  (*writer)->join();

  EXPECT_GT((*writer)->metrics().received_user_byte_count.load(), 0);
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
    StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
        ChangeLogWriter::open_or_create(this->test_file_,
                                        config,
                                        ChangeLogWriter::Options::with_default_values(),
                                        RemoveExisting{true});
    ASSERT_TRUE(writer.ok());

    (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

    ChangeLogWriter::Context context(**writer);

    // Write multiple slots
    //
    for (size_t i = 0; i < test_data.size(); ++i) {
      Status write_status = context.append_slot(
          test_data[i].size(),
          [&data = test_data[i], this](FirstVisitToBlock first_visit,
                                       ChangeLogBlock* block,
                                       MutableBuffer buffer,
                                       EditOffset offset) {
            VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                    << ", on slot: " << offset;
            this->on_visit_block(first_visit, block);
            std::memcpy(buffer.data(), data.data(), data.size());
          });
      ASSERT_TRUE(write_status.ok()) << "Failed to write slot " << i;
    }

    // Wait for writer to process appends before halting.
    //
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    (*writer)->halt();
    (*writer)->join();
  }

  // Read phase
  //
  {
    this->visited_blocks_.clear();

    StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
    ASSERT_TRUE(reader.ok());

    std::vector<std::string> read_data;
    std::vector<EditOffset> edit_offsets;

    auto visitor_fn = [&](FirstVisitToBlock first_visit,
                          ChangeLogBlock* block,
                          EditOffset edit_offset,
                          ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset;
      this->on_visit_block(first_visit, block);
      std::string data(reinterpret_cast<const char*>(payload.data()), payload.size());
      read_data.push_back(data);
      edit_offsets.push_back(edit_offset);
      return OkStatus();
    };

    batt::Status visit_status = (*reader)->visit_slots(visitor_fn);
    ASSERT_TRUE(visit_status.ok()) << BATT_INSPECT(visit_status);

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
    StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
        ChangeLogWriter::open_or_create(this->test_file_,
                                        config,
                                        ChangeLogWriter::Options::with_default_values(),
                                        RemoveExisting{true});
    ASSERT_TRUE(writer.ok());

    (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

    std::vector<std::thread> threads;
    std::atomic<int> total_writes{0};

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&, thread_id = t]() {
        ChangeLogWriter::Context context(**writer);

        for (int i = 0; i < slots_per_thread; ++i) {
          std::string data = "Thread " + std::to_string(thread_id) + " Slot " + std::to_string(i);

          Status write_status = context.append_slot(
              data.size(),
              [&data, &offsets](FirstVisitToBlock,
                                ChangeLogBlock* block,
                                MutableBuffer buffer,
                                EditOffset offset) {
                VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                        << ", on slot: " << offset << BATT_INSPECT(data.size());
                batt::ScopedLock<std::unordered_set<i64>> locked_offsets{offsets};
                locked_offsets->insert(offset.value());
                std::memcpy(buffer.data(), data.data(), data.size());
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

    // Wait for writer to process appends before halting.
    //
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    (*writer)->halt();
    (*writer)->join();

    EXPECT_EQ(total_writes.load(), num_threads * slots_per_thread);
  }

  // Read Phase
  //
  {
    StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
    ASSERT_TRUE(reader.ok());

    int slots_read = 0;
    auto visitor_fn = [&](FirstVisitToBlock,
                          ChangeLogBlock* block,
                          EditOffset edit_offset,
                          ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset << ", payload size: " << payload.size();

      slots_read++;

      batt::ScopedLock<std::unordered_set<i64>> locked_offsets{offsets};

      // Check that edit_offset was in the set of offsets we wrote
      //
      BATT_REQUIRE_NE(locked_offsets->find(edit_offset.value()), locked_offsets->end());
      locked_offsets->erase(edit_offset.value());
      return OkStatus();
    };

    batt::Status visit_status = (*reader)->visit_slots(visitor_fn);
    ASSERT_TRUE(visit_status.ok()) << BATT_INSPECT(visit_status);

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

  StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
      ChangeLogWriter::open_or_create(this->test_file_,
                                      config,
                                      ChangeLogWriter::Options::with_default_values(),
                                      RemoveExisting{true});
  ASSERT_TRUE(writer.ok());

  (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

  ChangeLogWriter::Context context(**writer);

  // Write data that will span multiple blocks
  //
  std::string large_data(900, 'X');  // Almost fills a block

  EXPECT_TRUE(this->visited_blocks_.empty());

  for (int i = 0; i < num_appends; ++i) {
    Status write_status = context.append_slot(
        large_data.size(),
        [&large_data, &writer, i, this](FirstVisitToBlock first_visit,
                                        ChangeLogBlock* block,
                                        MutableBuffer buffer,
                                        EditOffset offset) {
          VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                  << ", on slot: " << offset;
          if (first_visit) {
            (*writer)->trim(block->edit_offset_lower_bound()).IgnoreError();
          }
          this->on_visit_block(first_visit, block);
          std::memcpy(buffer.data(), large_data.data(), large_data.size());
        });

    if (!write_status.ok()) {
      // TODO: [Gabe Bornstein 4/1/26] We never hit this condition, do we expect to?
      //
      LOG(INFO) << "Resource exhausted";
      EXPECT_EQ(write_status, batt::StatusCode::kResourceExhausted);
    }
  }

  // Wait for writer to process appends before halting.
  //
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  (*writer)->halt();
  (*writer)->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(ChangeLogTest, ReadEmptyLog)
{
  ChangeLogFile::Config config = ChangeLogFile::Config::with_default_values();

  StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
      ChangeLogWriter::open_or_create(this->test_file_,
                                      config,
                                      ChangeLogWriter::Options::with_default_values(),
                                      RemoveExisting{true});
  ASSERT_TRUE(writer.ok());

  // Don't write anything, just close
  //
  writer->reset();

  // Try to read
  //
  StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
  ASSERT_TRUE(reader.ok());

  int slots_read = 0;
  auto visitor_fn = [&](FirstVisitToBlock,
                        ChangeLogBlock* block,
                        EditOffset edit_offset,
                        ConstBuffer payload) -> Status {
    VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
            << ", on slot: " << edit_offset << ", payload size: " << payload.size();
    slots_read++;
    return OkStatus();
  };

  batt::Status visit_status = (*reader)->visit_slots(visitor_fn);
  ASSERT_TRUE(visit_status.ok()) << BATT_INSPECT(visit_status);

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

  std::unordered_set<i64> offsets;
  i64 total_written = 0;
  i64 successful_writes = 0;

  // Write Phase
  //
  {
    StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
        ChangeLogWriter::open_or_create(this->test_file_,
                                        config,
                                        ChangeLogWriter::Options::with_default_values(),
                                        RemoveExisting{true});
    ASSERT_TRUE(writer.ok());

    (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

    ChangeLogWriter::Context context(**writer);

    // Keep writing until we've written target_data_size
    //
    while (total_written < target_data_size) {
      usize slot_size = size_dist(rng);
      std::string slot_data(slot_size, 'A');

      Status write_status = context.append_slot(
          slot_data.size(),
          [&slot_data, &offsets](FirstVisitToBlock,
                                 ChangeLogBlock* block,
                                 MutableBuffer buffer,
                                 EditOffset offset) {
            VLOG(1) << "Appending block with lower_bound: " << block->edit_offset_lower_bound()
                    << ", on slot: " << offset;
            offsets.insert(offset.value());
            std::memcpy(buffer.data(), slot_data.data(), slot_data.size());
          });

      if (write_status.ok()) {
        successful_writes++;
        total_written += slot_size;
      } else {
        ASSERT_TRUE(write_status.ok()) << BATT_INSPECT(write_status);
      }
    }

    // Give writer time to flush remaining data
    //
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    (*writer)->halt();
    (*writer)->join();

    LOG(INFO) << "Wrap-around test stats:"
              << " total_written=" << total_written << " capacity=" << total_capacity;

    // Verify we wrote significantly more than capacity
    //
    EXPECT_GT(total_written, total_capacity * 2);
    EXPECT_GT(successful_writes, 0);

    auto& metrics = (*writer)->metrics();
    EXPECT_GT(metrics.written_user_byte_count.load(), 0);
    EXPECT_GT(metrics.write_count.load(), 0);
    EXPECT_GT(ChangeLogBlock::metrics().block_alloc_count.get(), config.block_count.value());
  }

  // Read Phase
  //
  {
    StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
    ASSERT_TRUE(reader.ok());

    int slots_read = 0;
    std::unordered_set<i64> unique_blocks;
    auto visitor_fn = [&](FirstVisitToBlock,
                          ChangeLogBlock* block,
                          EditOffset edit_offset,
                          ConstBuffer payload) -> Status {
      VLOG(1) << "Reading block with lower_bound: " << block->edit_offset_lower_bound()
              << ", on slot: " << edit_offset << ", payload size: " << payload.size();

      slots_read++;

      BATT_REQUIRE_NE(offsets.find(edit_offset.value()), offsets.end());
      offsets.erase(edit_offset.value());

      unique_blocks.insert(block->edit_offset_lower_bound().value());
      return OkStatus();
    };

    batt::Status visit_status = (*reader)->visit_slots(visitor_fn);
    ASSERT_TRUE(visit_status.ok()) << BATT_INSPECT(visit_status);

    EXPECT_GT(slots_read, 0);
    EXPECT_EQ(unique_blocks.size(), config.block_count.value());
    EXPECT_EQ(slots_read, successful_writes - offsets.size());
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
    StatusOr<std::unique_ptr<ChangeLogWriter>> writer =
        ChangeLogWriter::open_or_create(this->test_file_,
                                        config,
                                        ChangeLogWriter::Options::with_default_values(),
                                        RemoveExisting{true});
    ASSERT_TRUE(writer.ok());

    (*writer)->start(batt::Runtime::instance().default_scheduler().schedule_task());

    ChangeLogWriter::Context context(**writer);

    // Write slots s.t. each block has one slot
    //
    for (int i = 0; i < num_blocks_to_write; ++i) {
      Status write_status = context.append_slot(
          test_data[i].size(),
          [&data = test_data[i],
           i](FirstVisitToBlock, ChangeLogBlock* block, MutableBuffer buffer, EditOffset offset) {
            VLOG(1) << "Writing slot " << i
                    << " to block with lower_bound: " << block->edit_offset_lower_bound()
                    << ", at edit_offset: " << offset;

            std::memcpy(buffer.data(), data.data(), data.size());
          });

      ASSERT_TRUE(write_status.ok()) << "Failed to write slot " << i;
    }

    // Wait for writer to flush all data
    //
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    (*writer)->halt();
    (*writer)->join();
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
    StatusOr<std::unique_ptr<ChangeLogReader>> reader = ChangeLogReader::open(this->test_file_);
    ASSERT_TRUE(reader.ok());

    int slots_read = 0;
    std::vector<EditOffset> recovered_offsets;

    auto visitor_fn = [&](FirstVisitToBlock,
                          ChangeLogBlock* block,
                          EditOffset edit_offset,
                          ConstBuffer payload) -> Status {
      slots_read++;
      recovered_offsets.push_back(edit_offset);

      LOG(INFO) << "Post-corruption read: slot " << slots_read << " at edit_offset: " << edit_offset
                << ", block lower_bound: " << block->edit_offset_lower_bound()
                << ", payload size: " << payload.size();

      return OkStatus();
    };

    Status visit_status = (*reader)->visit_slots(visitor_fn);

    // The visit should succeed but only read blocks before the corruption
    ASSERT_TRUE(visit_status.ok()) << "Visit failed with: " << visit_status;

    // TODO: [Gabe Bornstein 4/14/26] Change this check when we update how we read past corrupt
    // blocks. Currently, this check for # of blocks before we reach a corrupt block. We'll update
    // it to be # of valid blocks with a lower EditOffset than the corrupt block.
    //
    EXPECT_EQ(slots_read, corrupt_block_index)
        << "Expected to read " << corrupt_block_index
        << " slots (from blocks before corruption), but read " << slots_read;

    EXPECT_EQ(recovered_offsets.size(), corrupt_block_index);
  }
}

}  // namespace turtle_kv
