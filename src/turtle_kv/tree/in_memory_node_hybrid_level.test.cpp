#include <turtle_kv/tree/in_memory_node_hybrid_level.hpp>
//
#include <turtle_kv/tree/in_memory_node_hybrid_level.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/tree/in_memory_node_merged_level.hpp>
#include <turtle_kv/tree/in_memory_node_segmented_level.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <sstream>

namespace {

using namespace turtle_kv::int_types;
using namespace turtle_kv::constants;

using turtle_kv::InMemoryNodeHybridLevel;
using turtle_kv::InMemoryNodeMergedLevel;
using turtle_kv::InMemoryNodeSegment;
using turtle_kv::InMemoryNodeSegmentedLevel;
using turtle_kv::TreeOptions;

using HybridLevel = InMemoryNodeHybridLevel;
using MergedLevel = InMemoryNodeMergedLevel;
using SegmentedLevel = InMemoryNodeSegmentedLevel;
using Segment = InMemoryNodeSegment;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class InMemoryNodeHybridLevelTest : public ::testing::Test
{
 public:
  Segment make_segment(u64 page_id, const std::vector<i32>& active_pivot_indices)
  {
    Segment segment;
    segment.page_id_slot = llfs::PageIdSlot::from_page_id(llfs::PageId{page_id});
    for (i32 pivot_i : active_pivot_indices) {
      segment.active_pivots.set(pivot_i, true);
    }
    return segment;
  }

  SegmentedLevel make_segmented_level(std::vector<Segment> segments)
  {
    SegmentedLevel level;
    for (auto& seg : segments) {
      level.segments.emplace_back(std::move(seg));
    }
    return level;
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// empty / front / back / get_levels
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, EmptyByDefault)
{
  HybridLevel level;
  EXPECT_TRUE(level.empty());
  EXPECT_EQ(level.front(), nullptr);
  EXPECT_EQ(level.back(), nullptr);
  EXPECT_TRUE(level.get_levels().empty());
}

TEST_F(InMemoryNodeHybridLevelTest, NonEmptyAfterAddSubLevel)
{
  HybridLevel level;
  SegmentedLevel seg = this->make_segmented_level({this->make_segment(1, {0})});
  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg)});

  EXPECT_FALSE(level.empty());
  EXPECT_NE(level.front(), nullptr);
  EXPECT_NE(level.back(), nullptr);
  EXPECT_EQ(level.get_levels().size(), 1u);
}

TEST_F(InMemoryNodeHybridLevelTest, FrontAndBackDistinct)
{
  HybridLevel level;
  SegmentedLevel seg1 = this->make_segmented_level({this->make_segment(1, {0})});
  MergedLevel merged;

  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg1)});
  level.add_new_sub_level(HybridLevel::SubLevel{std::move(merged)});

  EXPECT_NE(level.front(), level.back());
  EXPECT_TRUE(batt::is_case<SegmentedLevel>(*level.front()));
  EXPECT_TRUE(batt::is_case<MergedLevel>(*level.back()));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// add_new_sub_level (SubLevel&&)
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, AddSubLevelWithPushPivots)
{
  HybridLevel level;
  SegmentedLevel seg = this->make_segmented_level({this->make_segment(10, {0, 1})});

  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg)}, /*push_pivot_count=*/3);

  ASSERT_EQ(level.get_levels().size(), 1u);
  auto& segmented = std::get<SegmentedLevel>(level.sub_levels[0]);
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(3));
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(4));
  EXPECT_FALSE(segmented.get_segment(0).is_pivot_active(0));
  EXPECT_FALSE(segmented.get_segment(0).is_pivot_active(1));
}

TEST_F(InMemoryNodeHybridLevelTest, AddSubLevelNoPush)
{
  HybridLevel level;
  SegmentedLevel seg = this->make_segmented_level({this->make_segment(20, {2, 5})});

  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg)}, /*push_pivot_count=*/0);

  auto& segmented = std::get<SegmentedLevel>(level.sub_levels[0]);
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(2));
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(5));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// add_new_sub_level (HybridLevel&&)
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, AddHybridLevelMerges)
{
  HybridLevel level;
  level.add_new_sub_level(HybridLevel::SubLevel{MergedLevel{}});

  HybridLevel other;
  other.add_new_sub_level(
      HybridLevel::SubLevel{this->make_segmented_level({this->make_segment(30, {1})})});
  other.add_new_sub_level(
      HybridLevel::SubLevel{this->make_segmented_level({this->make_segment(31, {2})})});

  level.add_new_sub_level(std::move(other), /*push_pivot_count=*/0);

  EXPECT_EQ(level.get_levels().size(), 3u);
}

TEST_F(InMemoryNodeHybridLevelTest, AddHybridLevelWithPushPivots)
{
  HybridLevel level;

  HybridLevel other;
  other.add_new_sub_level(
      HybridLevel::SubLevel{this->make_segmented_level({this->make_segment(40, {0})})});

  level.add_new_sub_level(std::move(other), /*push_pivot_count=*/5);

  ASSERT_EQ(level.get_levels().size(), 1u);
  auto& segmented = std::get<SegmentedLevel>(level.sub_levels[0]);
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(5));
  EXPECT_FALSE(segmented.get_segment(0).is_pivot_active(0));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// push_front_pivots
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, PushFrontPivotsShiftsSegmented)
{
  HybridLevel level;
  level.add_new_sub_level(
      HybridLevel::SubLevel{this->make_segmented_level({this->make_segment(50, {0, 3})})});
  level.add_new_sub_level(HybridLevel::SubLevel{MergedLevel{}});

  level.push_front_pivots(/*node_pivot_count=*/2);

  auto& segmented = std::get<SegmentedLevel>(level.sub_levels[0]);
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(2));
  EXPECT_TRUE(segmented.get_segment(0).is_pivot_active(5));
  EXPECT_FALSE(segmented.get_segment(0).is_pivot_active(0));
  EXPECT_FALSE(segmented.get_segment(0).is_pivot_active(3));
}

TEST_F(InMemoryNodeHybridLevelTest, PushFrontPivotsSkipsMerged)
{
  HybridLevel level;
  MergedLevel merged;
  level.add_new_sub_level(HybridLevel::SubLevel{std::move(merged)});

  level.push_front_pivots(/*node_pivot_count=*/10);

  EXPECT_TRUE(batt::is_case<MergedLevel>(level.sub_levels[0]));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// segment_count
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, SegmentCountEmpty)
{
  HybridLevel level;
  TreeOptions tree_options = TreeOptions::with_default_values()
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(24)
                                 .set_value_size_hint(100);

  EXPECT_EQ(level.segment_count(tree_options), 0u);
}

TEST_F(InMemoryNodeHybridLevelTest, SegmentCountWithSegmentedSubLevel)
{
  HybridLevel level;
  TreeOptions tree_options = TreeOptions::with_default_values()
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(24)
                                 .set_value_size_hint(100);

  SegmentedLevel seg = this->make_segmented_level({
      this->make_segment(100, {0}),
      this->make_segment(101, {1}),
      this->make_segment(102, {2}),
  });

  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg)});

  EXPECT_EQ(level.segment_count(tree_options), 3u);
}

TEST_F(InMemoryNodeHybridLevelTest, SegmentCountMultipleSubLevels)
{
  HybridLevel level;
  TreeOptions tree_options = TreeOptions::with_default_values()
                                 .set_leaf_size(512 * kKiB)
                                 .set_node_size(4 * kKiB)
                                 .set_key_size_hint(24)
                                 .set_value_size_hint(100);

  SegmentedLevel seg1 = this->make_segmented_level({
      this->make_segment(200, {0}),
      this->make_segment(201, {1}),
  });
  SegmentedLevel seg2 = this->make_segmented_level({
      this->make_segment(300, {0}),
  });

  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg1)});
  level.add_new_sub_level(HybridLevel::SubLevel{MergedLevel{}});
  level.add_new_sub_level(HybridLevel::SubLevel{std::move(seg2)});

  EXPECT_GE(level.segment_count(tree_options), 3u);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// dump
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST_F(InMemoryNodeHybridLevelTest, DumpProducesOutput)
{
  HybridLevel level;
  level.add_new_sub_level(
      HybridLevel::SubLevel{this->make_segmented_level({this->make_segment(1, {0})})});

  std::ostringstream oss;
  auto dump_fn = level.dump();
  dump_fn(oss);

  std::string output = oss.str();
  EXPECT_FALSE(output.empty());
  EXPECT_NE(output.find("HybridLevel"), std::string::npos);
}

TEST_F(InMemoryNodeHybridLevelTest, DumpEmptyLevel)
{
  HybridLevel level;

  std::ostringstream oss;
  auto dump_fn = level.dump();
  dump_fn(oss);

  std::string output = oss.str();
  EXPECT_NE(output.find("HybridLevel"), std::string::npos);
}

}  // namespace
