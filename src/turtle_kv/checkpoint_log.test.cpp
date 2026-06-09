#include <turtle_kv/checkpoint_log.hpp>
//
#include <turtle_kv/checkpoint_log.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_root.test.hpp"

#include <turtle_kv/page_file.hpp>

#include <batteries/async/runtime.hpp>

namespace {

using turtle_kv::PageFileSpec;
using turtle_kv::RemoveExisting;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using namespace batt::constants;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void mkdir_p(std::filesystem::path p) noexcept
{
  std::filesystem::create_directories(p);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void rm_rf(std::filesystem::path p) noexcept
{
  if (std::filesystem::exists(p)) {
    std::filesystem::remove_all(p);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(CheckpointLogTest, CreateOpen)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_dir = *root / "checkpoint_log_test/create_open";

  std::filesystem::path filename = test_dir / "checkpoint_log.llfs";

  mkdir_p(test_dir);
  rm_rf(filename);

  llfs::ScopedIoRing scoped_io_ring = BATT_OK_RESULT_OR_PANIC(
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{1}));

  const llfs::IoRing& io_ring = scoped_io_ring.get_io_ring();

  {
    auto storage_context =
        llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(), io_ring);

    Status status = turtle_kv::create_checkpoint_log(*storage_context,
                                                     turtle_kv::TreeOptions::with_default_values(),
                                                     filename);

    ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
  }

  EXPECT_GT(std::filesystem::file_size(filename), 2 * kMiB);

  {
    auto storage_context =
        llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(), io_ring);

    StatusOr<std::unique_ptr<llfs::Volume>> checkpoint_log =
        turtle_kv::open_checkpoint_log(*storage_context, filename);

    ASSERT_TRUE(checkpoint_log.ok()) << BATT_INSPECT(checkpoint_log.status());
    EXPECT_NE(*checkpoint_log, nullptr);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(CheckpointLogTest, CreatePageFiles)
{
  batt::StatusOr<std::filesystem::path> root = turtle_kv::data_root();
  ASSERT_TRUE(root.ok());

  std::filesystem::path test_dir = *root / "checkpoint_log_test/create_page_files";

  llfs::ScopedIoRing scoped_io_ring = BATT_OK_RESULT_OR_PANIC(
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{64}, llfs::ThreadPoolSize{1}));

  const llfs::IoRing& io_ring = scoped_io_ring.get_io_ring();

  for (auto spec : {
           PageFileSpec{
               .filename = test_dir / "pages_4k.llfs",
               .initial_page_count = llfs::PageCount{100},
               .page_size = llfs::PageSize{4096},
           },
           PageFileSpec{
               .filename = test_dir / "pages_32k.llfs",
               .initial_page_count = llfs::PageCount{50},
               .page_size = llfs::PageSize{32768},
           },
           PageFileSpec{
               .filename = test_dir / "pages_2mb.llfs",
               .initial_page_count = llfs::PageCount{40},
               .page_size = llfs::PageSize{2 * 1024 * 1024},
           },
       }) {
    //----- --- -- -  -  -   -
    mkdir_p(spec.filename.parent_path());
    rm_rf(spec.filename);
    //----- --- -- -  -  -   -
    {
      auto storage_context =
          llfs::StorageContext::make_shared(batt::Runtime::instance().default_scheduler(), io_ring);

      {
        Status status = turtle_kv::create_page_file(*storage_context, spec);
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
        {
          std::error_code ec;
          EXPECT_TRUE(std::filesystem::exists(spec.filename, ec) && !ec) << BATT_INSPECT(ec);
        }
      }
      {
        Status status = turtle_kv::create_page_file(*storage_context, spec);
        EXPECT_EQ(status, batt::status_from_errno(EEXIST));
      }
      {
        Status status = turtle_kv::create_page_file(*storage_context, spec, RemoveExisting{true});
        ASSERT_TRUE(status.ok()) << BATT_INSPECT(status);
      }
    }

    EXPECT_GT(std::filesystem::file_size(spec.filename), spec.initial_page_count * spec.page_size);
  }
}

}  // namespace
