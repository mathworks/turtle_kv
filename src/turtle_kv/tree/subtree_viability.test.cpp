#include <turtle_kv/tree/subtree_viability.hpp>
//
#include <turtle_kv/tree/subtree_viability.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/stream_util.hpp>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::compacting_levels_might_fix;
using turtle_kv::is_root_viable;
using turtle_kv::NeedsMerge;
using turtle_kv::NeedsSplit;
using turtle_kv::normal_flush_might_fix;
using turtle_kv::normal_flush_might_fix_root;
using turtle_kv::SubtreeViability;
using turtle_kv::Viable;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// NeedsMerge::operator bool
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(NeedsMergeTest, DefaultIsFalse)
{
  NeedsMerge nm;
  EXPECT_FALSE(nm.single_pivot);
  EXPECT_FALSE(nm.too_few_pivots);
  EXPECT_FALSE(nm.too_few_items);
  EXPECT_FALSE(nm.zero_items);
  EXPECT_FALSE(bool(nm));
}

TEST(NeedsMergeTest, TooFewPivotsIsTrue)
{
  NeedsMerge nm;
  nm.too_few_pivots = true;
  EXPECT_TRUE(bool(nm));
}

TEST(NeedsMergeTest, TooFewItemsIsTrue)
{
  NeedsMerge nm;
  nm.too_few_items = true;
  EXPECT_TRUE(bool(nm));
}

TEST(NeedsMergeTest, SinglePivotAloneIsFalse)
{
  NeedsMerge nm;
  nm.single_pivot = true;
  EXPECT_FALSE(bool(nm));
}

TEST(NeedsMergeTest, ZeroItemsAloneIsFalse)
{
  NeedsMerge nm;
  nm.zero_items = true;
  EXPECT_FALSE(bool(nm));
}

TEST(NeedsMergeTest, StreamOutput)
{
  NeedsMerge nm;
  nm.too_few_pivots = true;
  nm.zero_items = true;

  EXPECT_EQ(batt::to_string(nm),
            "NeedsMerge{.single_pivot=0, .too_few_pivots=1, .too_few_items=0, .zero_items=1,}");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// NeedsSplit::operator bool
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(NeedsSplitTest, DefaultIsFalse)
{
  NeedsSplit ns{};
  EXPECT_EQ(ns.pivot_count, 0u);
  EXPECT_EQ(ns.segment_count, 0u);
  EXPECT_EQ(ns.height, 0);
  EXPECT_FALSE(ns.items_too_large);
  EXPECT_FALSE(ns.keys_too_large);
  EXPECT_FALSE(ns.too_many_pivots);
  EXPECT_FALSE(ns.too_many_segments);
  EXPECT_FALSE(ns.segment_filters_too_large);
  EXPECT_FALSE(bool(ns));
}

TEST(NeedsSplitTest, ItemsTooLargeIsTrue)
{
  NeedsSplit ns{};
  ns.items_too_large = true;
  EXPECT_TRUE(bool(ns));
}

TEST(NeedsSplitTest, KeysTooLargeIsTrue)
{
  NeedsSplit ns{};
  ns.keys_too_large = true;
  EXPECT_TRUE(bool(ns));
}

TEST(NeedsSplitTest, TooManyPivotsIsTrue)
{
  NeedsSplit ns{};
  ns.too_many_pivots = true;
  EXPECT_TRUE(bool(ns));
}

TEST(NeedsSplitTest, TooManySegmentsIsTrue)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  EXPECT_TRUE(bool(ns));
}

TEST(NeedsSplitTest, SegmentFiltersTooLargeIsTrue)
{
  NeedsSplit ns{};
  ns.segment_filters_too_large = true;
  EXPECT_TRUE(bool(ns));
}

TEST(NeedsSplitTest, StreamOutput)
{
  NeedsSplit ns{};
  ns.too_many_pivots = true;
  ns.pivot_count = 42;

  EXPECT_EQ(
      batt::to_string(ns),
      "NeedsSplit{.items_too_large=0, .keys_too_large=0, .too_many_pivots=1, .pivot_count=42, "
      ".segment_count=0, .height=0, .too_many_segments=0, .segment_filters_too_large=0,}");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Viable
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(ViableTest, StreamOutput)
{
  Viable v;
  EXPECT_EQ(batt::to_string(v), "Viable");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// SubtreeViability stream output
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(SubtreeViabilityTest, StreamOutputViable)
{
  SubtreeViability sv = Viable{};
  EXPECT_EQ(batt::to_string(sv), "Viable");
}

TEST(SubtreeViabilityTest, StreamOutputNeedsMerge)
{
  NeedsMerge nm;
  nm.too_few_items = true;
  SubtreeViability sv = nm;

  EXPECT_EQ(batt::to_string(sv),
            "NeedsMerge{.single_pivot=0, .too_few_pivots=0, .too_few_items=1, .zero_items=0,}");
}

TEST(SubtreeViabilityTest, StreamOutputNeedsSplit)
{
  NeedsSplit ns{};
  ns.items_too_large = true;
  SubtreeViability sv = ns;

  EXPECT_EQ(batt::to_string(sv),
            "NeedsSplit{.items_too_large=1, .keys_too_large=0, .too_many_pivots=0, .pivot_count=0, "
            ".segment_count=0, .height=0, .too_many_segments=0, .segment_filters_too_large=0,}");
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// compacting_levels_might_fix
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(CompactingLevelsMightFixTest, ViableReturnsFalse)
{
  SubtreeViability sv = Viable{};
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, NeedsMergeReturnsFalse)
{
  NeedsMerge nm;
  nm.too_few_items = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, SegmentFiltersTooLargeOnly)
{
  NeedsSplit ns{};
  ns.segment_filters_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_TRUE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, TooManySegmentsOnly)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  SubtreeViability sv = ns;
  EXPECT_TRUE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, SegmentFiltersPlusItemsTooLarge)
{
  NeedsSplit ns{};
  ns.segment_filters_too_large = true;
  ns.items_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, TooManySegmentsPlusKeysTooLarge)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  ns.keys_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, TooManySegmentsPlusTooManyPivots)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  ns.too_many_pivots = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

TEST(CompactingLevelsMightFixTest, NeitherSegmentIssueReturnsFalse)
{
  NeedsSplit ns{};
  ns.items_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(compacting_levels_might_fix(sv));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// normal_flush_might_fix
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(NormalFlushMightFixTest, ViableReturnsFalse)
{
  SubtreeViability sv = Viable{};
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, NeedsMergeReturnsFalse)
{
  NeedsMerge nm;
  nm.too_few_pivots = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightTwoSegmentFiltersTooLarge)
{
  NeedsSplit ns{};
  ns.height = 2;
  ns.segment_filters_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_TRUE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightTwoTooManySegments)
{
  NeedsSplit ns{};
  ns.height = 2;
  ns.too_many_segments = true;
  SubtreeViability sv = ns;
  EXPECT_TRUE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightThreeReturnsFalse)
{
  NeedsSplit ns{};
  ns.height = 3;
  ns.segment_filters_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightTwoButItemsTooLarge)
{
  NeedsSplit ns{};
  ns.height = 2;
  ns.too_many_segments = true;
  ns.items_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightTwoButKeysTooLarge)
{
  NeedsSplit ns{};
  ns.height = 2;
  ns.segment_filters_too_large = true;
  ns.keys_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

TEST(NormalFlushMightFixTest, HeightTwoButTooManyPivots)
{
  NeedsSplit ns{};
  ns.height = 2;
  ns.too_many_segments = true;
  ns.too_many_pivots = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix(sv));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// normal_flush_might_fix_root
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(NormalFlushMightFixRootTest, ViableReturnsFalse)
{
  SubtreeViability sv = Viable{};
  EXPECT_FALSE(normal_flush_might_fix_root(sv));
}

TEST(NormalFlushMightFixRootTest, NeedsMergeReturnsFalse)
{
  NeedsMerge nm;
  nm.too_few_items = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(normal_flush_might_fix_root(sv));
}

TEST(NormalFlushMightFixRootTest, TooManySegmentsOnly)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  SubtreeViability sv = ns;
  EXPECT_TRUE(normal_flush_might_fix_root(sv));
}

TEST(NormalFlushMightFixRootTest, TooManySegmentsPlusTooManyPivots)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  ns.too_many_pivots = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix_root(sv));
}

TEST(NormalFlushMightFixRootTest, TooManySegmentsPlusKeysTooLarge)
{
  NeedsSplit ns{};
  ns.too_many_segments = true;
  ns.keys_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix_root(sv));
}

TEST(NormalFlushMightFixRootTest, SegmentFiltersAloneReturnsFalse)
{
  NeedsSplit ns{};
  ns.segment_filters_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(normal_flush_might_fix_root(sv));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// is_root_viable
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

TEST(IsRootViableTest, ViableReturnsTrue)
{
  SubtreeViability sv = Viable{};
  EXPECT_TRUE(is_root_viable(sv));
}

TEST(IsRootViableTest, NeedsSplitReturnsFalse)
{
  NeedsSplit ns{};
  ns.items_too_large = true;
  SubtreeViability sv = ns;
  EXPECT_FALSE(is_root_viable(sv));
}

TEST(IsRootViableTest, NeedsMergeNoSpecialFlagsReturnsTrue)
{
  NeedsMerge nm;
  nm.too_few_items = true;
  SubtreeViability sv = nm;
  EXPECT_TRUE(is_root_viable(sv));
}

TEST(IsRootViableTest, NeedsMergeSinglePivotReturnsFalse)
{
  NeedsMerge nm;
  nm.single_pivot = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(is_root_viable(sv));
}

TEST(IsRootViableTest, NeedsMergeZeroItemsReturnsFalse)
{
  NeedsMerge nm;
  nm.zero_items = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(is_root_viable(sv));
}

TEST(IsRootViableTest, NeedsMergeBothSpecialFlagsReturnsFalse)
{
  NeedsMerge nm;
  nm.single_pivot = true;
  nm.zero_items = true;
  SubtreeViability sv = nm;
  EXPECT_FALSE(is_root_viable(sv));
}

}  // namespace
