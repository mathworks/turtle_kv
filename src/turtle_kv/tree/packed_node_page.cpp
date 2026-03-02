#include <turtle_kv/tree/packed_node_page.hpp>
//

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/subtree.hpp>

#include <turtle_kv/util/buffer_bounds_checker.hpp>

#include <llfs/packed_page_header.hpp>

#include <bitset>
#include <cstddef>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedNodePage* build_node_page(const MutableBuffer& buffer, const InMemoryNode& src_node)
{
  BATT_CHECK(src_node.is_packable());
  BATT_CHECK_GT(buffer.size(), sizeof(llfs::PackedPageHeader));

  BufferBoundsChecker bounds_checker{buffer};

  llfs::PackedPageHeader* page_header = static_cast<llfs::PackedPageHeader*>(buffer.data());
  BATT_CHECK_EQ(page_header->layout_id, NodePageView::page_layout_id());

  MutableBuffer payload_buffer = buffer + sizeof(llfs::PackedPageHeader);
  BATT_CHECK_GE(payload_buffer.size(), sizeof(PackedNodePage));

  std::memset(payload_buffer.data(), 0, payload_buffer.size());

  PackedNodePage* packed_node = static_cast<PackedNodePage*>(payload_buffer.data());

  MutableBuffer variable_buffer =
      payload_buffer + offsetof(PackedNodePage, key_and_flushed_item_data_);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  const auto pack_key =                                                                       //
      [&variable_buffer, packed_node]                                                         //
      (PackedNodePage::Key & dst_key, const std::string_view& src_key) -> bool [[nodiscard]]  //
  {
    usize n = src_key.size();
    if (is_global_max_key(src_key)) {
      BATT_CHECK_NE((const void*)std::addressof(dst_key),
                    (const void*)std::addressof(packed_node->pivot_keys_[0]));
      n = 0;
    }
    if (n > variable_buffer.size()) {
      return false;
    }
    void* copy_dst = variable_buffer.data();
    if (n != 0) {
      std::memcpy(copy_dst, src_key.data(), n);
      variable_buffer += n;
    }
    dst_key.pointer.offset =
        BATT_CHECKED_CAST(u16, byte_distance(std::addressof(dst_key.pointer), copy_dst));

    return (void*)dst_key.pointer.get() == copy_dst;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack everything up to the update buffer

  const usize pivot_count = src_node.pivot_count();

  BATT_CHECK_LE(pivot_count, PackedNodePage::kMaxPivots);
  BATT_CHECK_EQ(src_node.pivot_keys_.size(), pivot_count + 1);
  BATT_CHECK_EQ(src_node.children.size(), pivot_count);
  BATT_CHECK_EQ(src_node.pending_bytes.size(), pivot_count);

  packed_node->height = BATT_CHECKED_CAST(u8, src_node.height);
  packed_node->pivot_count_and_flags =
      BATT_CHECKED_CAST(u8, pivot_count & PackedNodePage::kPivotCountMask);

  if (src_node.is_size_tiered()) {
    packed_node->pivot_count_and_flags |= PackedNodePage::kFlagSizeTiered;
  }

  for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
    packed_node->pending_bytes[pivot_i] = BATT_CHECKED_CAST(u32, src_node.pending_bytes[pivot_i]);
    packed_node->children[pivot_i] = src_node.children[pivot_i].packed_page_id_or_panic();
  }

  for (usize pivot_i = 0; pivot_i < pivot_count + 1; ++pivot_i) {
    BATT_CHECK(pack_key(packed_node->pivot_keys_[pivot_i],  //
                        src_node.pivot_keys_[pivot_i]));
  }

  BATT_CHECK(pack_key(packed_node->pivot_keys_[packed_node->index_of_max_key()],  //
                      src_node.max_key_));

  BATT_CHECK(pack_key(packed_node->pivot_keys_[packed_node->index_of_common_key_prefix()],  //
                      src_node.common_key_prefix));

  BATT_CHECK(pack_key(packed_node->pivot_keys_[packed_node->index_of_final_key_end()], ""));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack the update buffer

  using EmptyLevel = InMemoryNode::UpdateBuffer::EmptyLevel;
  using SegmentedLevel = InMemoryNode::UpdateBuffer::SegmentedLevel;
  using Segment = InMemoryNode::UpdateBuffer::Segment;

  // Initialize the array containing cut points for segment filters.
  //
  const usize segment_filters_items = src_node.total_segment_filter_cut_points();
  const usize segment_filters_array_size = src_node.segment_filters_byte_size();
  BATT_CHECK_GE(variable_buffer.size(), segment_filters_array_size);

  llfs::PackedArray<little_u32>* segment_filters_array =
      static_cast<llfs::PackedArray<little_u32>*>(variable_buffer.data());
  segment_filters_array->initialize(segment_filters_items);

  variable_buffer += segment_filters_array_size;

  packed_node->update_buffer.segment_filters.reset(segment_filters_array, &bounds_checker);

  {
    usize dst_segment_i = 0;
    usize level_i = 0;
    usize segment_filters_offset = 0;
    for (; level_i < src_node.update_buffer.levels.size(); ++level_i) {
      if (!src_node.is_size_tiered()) {
        packed_node->update_buffer.level_start[level_i] = BATT_CHECKED_CAST(u8, dst_segment_i);
      }

      const InMemoryNode::UpdateBuffer::Level& src_level = src_node.update_buffer.levels[level_i];

      if (batt::is_case<EmptyLevel>(src_level)) {
        continue;
      }
      BATT_CHECK((batt::is_case<SegmentedLevel>(src_level)));

      const SegmentedLevel& segmented_level = std::get<SegmentedLevel>(src_level);
      for (const Segment& src_segment : segmented_level.segments) {
        BATT_CHECK_LT(dst_segment_i, packed_node->update_buffer.segments.size());

        PackedNodePage::UpdateBuffer::Segment& dst_segment =
            packed_node->update_buffer.segments[dst_segment_i];

        dst_segment.leaf_page_id = llfs::PackedPageId::from(src_segment.page_id_slot.page_id);
        dst_segment.active_pivots = src_segment.get_active_pivots();

        BATT_CHECK_EQ(bit_count(src_segment.get_active_pivots()),
                      bit_count(dst_segment.active_pivots));

        dst_segment.filter_start = BATT_CHECKED_CAST(u16, segment_filters_offset);

        const PiecewiseFilter<u32>& segment_filter = src_segment.filter;
        if (!src_segment.is_unfiltered()) {
          Slice<const Interval<u32>> dropped_ranges = segment_filter.dropped();
          BATT_CHECK(!dropped_ranges.empty());

          // If the first item is filtered, set the most significant bit of `filter_start` to 1.
          //
          bool start_filtered = dropped_ranges[0].lower_bound == 0;
          if (start_filtered) {
            dst_segment.filter_start |= PackedNodePage::kSegmentStartsFiltered;
          }

          for (const Interval<u32>& range : dropped_ranges) {
            // If the first item is filtered, don't store index 0. Otherwise, store both bounds.
            //
            if (range.lower_bound == 0) {
              segment_filters_array->items[segment_filters_offset] = range.upper_bound;
              segment_filters_offset++;
            } else {
              segment_filters_array->items[segment_filters_offset] = range.lower_bound;
              segment_filters_offset++;

              segment_filters_array->items[segment_filters_offset] = range.upper_bound;
              segment_filters_offset++;
            }
          }
        }

        ++dst_segment_i;
      }
    }

    // The remainder of the `level_start` array should point to the end of the valid segments range.
    //
    if (src_node.is_size_tiered()) {
      level_i = 0;
    }
    for (; level_i < packed_node->update_buffer.level_start.size(); ++level_i) {
      packed_node->update_buffer.level_start[level_i] = BATT_CHECKED_CAST(u8, dst_segment_i);
    }
  }

  page_header->unused_begin = byte_distance(buffer.data(), variable_buffer.data());
  page_header->unused_end = buffer.size();

  BATT_CHECK_LE(page_header->unused_begin, page_header->unused_end);

  return packed_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree PackedNodePage::get_child(i32 pivot_i) const
{
  return Subtree::from_packed_page_id(this->children[pivot_i]);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> PackedNodePage::find_key(KeyQuery& query) const
{
  return in_node(*this).find_key(query);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedNodePage::UpdateBuffer::SegmentFilterData PackedNodePage::get_segment_filter_values(
    usize level_i,
    usize segment_i) const
{
  const usize i = [&]() -> usize {
    if (this->is_size_tiered()) {
      BATT_CHECK_LT(level_i, this->update_buffer.segment_count());
      BATT_CHECK_EQ(segment_i, 0);
      return level_i;
    }
    BATT_CHECK_LT(level_i, kMaxLevels);
    const usize i = this->update_buffer.level_start[level_i] + segment_i;
    BATT_CHECK_LT(i, this->update_buffer.level_start[level_i + 1]);
    return i;
  }();

  const UpdateBuffer::Segment& segment = this->update_buffer.segments[i];
  const llfs::PackedArray<little_u32>& packed_filters = *this->update_buffer.segment_filters;

  // To retrieve the starting offset into the packed_filters array for this segment, clear the
  // most significant bit, since that bit stores whether or not the start of the segment is
  // filtered.
  //
  u32 filter_start_i = segment.filter_start.value() & ~PackedNodePage::kSegmentStartsFiltered;
  u32 filter_end_i;
  if (i + 1 < this->update_buffer.segment_count()) {
    filter_end_i = this->update_buffer.segments[i + 1].filter_start.value() &
                   ~PackedNodePage::kSegmentStartsFiltered;
  } else {
    filter_end_i = packed_filters.size();
  }

  bool start_filtered =
      (segment.filter_start.value() & PackedNodePage::kSegmentStartsFiltered) != 0;

  return PackedNodePage::UpdateBuffer::SegmentFilterData{
      as_const_slice(packed_filters.data() + filter_start_i, packed_filters.data() + filter_end_i),
      start_filtered};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PiecewiseFilter<u32>> PackedNodePage::create_piecewise_filter(usize level_i,
                                                                       usize segment_i) const
{
  PackedNodePage::UpdateBuffer::SegmentFilterData filter_data =
      this->get_segment_filter_values(level_i, segment_i);

  // If the start is marked as unfiltered and there aren't any values stored for this segment,
  // the entire segment is unfiltered.
  //
  if (filter_data.values.empty()) {
    BATT_CHECK(!filter_data.start_is_filtered);
    return PiecewiseFilter<u32>{};
  }

  SmallVec<Interval<u32>, 64> dropped_ranges;
  u32 i = 0;

  // If the first item at index 0 is filtered, add the corresponding interval first since the
  // serialized version of the filter doesn't store index 0.
  //
  if (filter_data.start_is_filtered) {
    BATT_CHECK_NE(filter_data.values.size() % 2, 0);
    dropped_ranges.emplace_back(Interval<u32>{0, filter_data.values[i].value()});
    i++;
  }

  for (; i + 1 < filter_data.values.size(); i += 2) {
    dropped_ranges.emplace_back(
        Interval<u32>{filter_data.values[i].value(), filter_data.values[i + 1].value()});
  }

  return PiecewiseFilter<u32>::from_dropped(as_slice(dropped_ranges));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> PackedNodePage::find_key_in_level(usize level_i,
                                                      KeyQuery& query,
                                                      i32 key_pivot_i) const
{
  UpdateBuffer::SegmentedLevel level =
      this->is_size_tiered() ? this->get_tier(level_i) : this->get_level(level_i);

  return in_segmented_level(*this, level, *query.page_loader, query.overcommit())
      .find_key(key_pivot_i, query);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> PackedNodePage::UpdateBuffer::Segment::load_leaf_page(
    llfs::PageLoader& page_loader,
    llfs::PinPageToJob pin_page_to_job,
    llfs::PageCacheOvercommit& overcommit) const
{
  return page_loader.load_page(this->leaf_page_id.unpack(),
                               llfs::PageLoadOptions{
                                   LeafPageView::page_layout_id(),
                                   pin_page_to_job,
                                   llfs::OkIfNotFound{false},
                                   llfs::LruPriority{kLeafLruPriority},
                                   overcommit,
                               });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PackedNodePage::UpdateBuffer::Segment::is_index_filtered(const SegmentedLevel& level,
                                                              u32 index) const
{
  const usize segment_i = std::distance(level.segments_slice.begin(), this);
  PackedNodePage::UpdateBuffer::SegmentFilterData filter_data =
      level.packed_node_->get_segment_filter_values(level.level_i_, segment_i);

  // If there are no values stored for this filter, the entire segment is unfiltered.
  //
  if (filter_data.values.empty()) {
    return false;
  }

  // Compute the number of cut points that occur up to and potentially including this index.
  //
  auto iter = std::upper_bound(filter_data.values.begin(), filter_data.values.end(), index);

  usize previous_cut_points = std::distance(filter_data.values.begin(), iter);

  // If the start is filtered, an even number of previous cut points would mean that we are in a
  // filtered region. If the start is unfiltered, an even number of previous cut points means that
  // we are in an unfiltered region.
  //
  bool is_filtered = (previous_cut_points % 2 == 0) == filter_data.start_is_filtered;

  return is_filtered;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 PackedNodePage::UpdateBuffer::Segment::live_lower_bound(const SegmentedLevel& level,
                                                            u32 item_i) const
{
  const usize segment_i = std::distance(level.segments_slice.begin(), this);
  PackedNodePage::UpdateBuffer::SegmentFilterData filter_data =
      level.packed_node_->get_segment_filter_values(level.level_i_, segment_i);

  const Slice<const little_u32> filter_values = filter_data.values;

  // Every item is live when the entire segment is unfiltered.
  //
  if (filter_values.empty()) {
    return item_i;
  }

  auto iter = std::upper_bound(filter_values.begin(), filter_values.end(), item_i);

  usize previous_cut_points = std::distance(filter_data.values.begin(), iter);

  bool is_filtered = (previous_cut_points % 2 == 0) == filter_data.start_is_filtered;

  // If we're already in an unfiltered region, just return the index. Otherwise, our upper bound
  // is the next unfiltered index, as it is an unfiltered cut point.
  //
  if (!is_filtered) {
    return item_i;
  }

  if (iter != filter_values.end()) {
    return iter->value();
  }

  // If iter points to the end iterator here, the filter is in an invalid state; the last region
  // of the filter should always be unfiltered.
  //
  BATT_PANIC() << "Filter is in an invalid state!" << BATT_INSPECT(item_i)
               << BATT_INSPECT(filter_values);
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<u32> PackedNodePage::UpdateBuffer::Segment::get_live_item_range(
    const SegmentedLevel& level,
    Interval<u32> i) const
{
  u32 start_i = i.lower_bound;
  u32 end_i = i.upper_bound;

  BATT_CHECK_LT(start_i, end_i);

  const usize segment_i = std::distance(level.segments_slice.begin(), this);
  PackedNodePage::UpdateBuffer::SegmentFilterData filter_data =
      level.packed_node_->get_segment_filter_values(level.level_i_, segment_i);

  const Slice<const little_u32> filter_values = filter_data.values;

  if (filter_values.empty()) {
    return i;
  }

  auto iter = std::upper_bound(filter_values.begin(), filter_values.end(), start_i);

  usize previous_cut_points = std::distance(filter_data.values.begin(), iter);

  bool is_filtered = (previous_cut_points % 2 == 0) == filter_data.start_is_filtered;

  if (is_filtered) {
    BATT_CHECK_NE(iter, filter_values.end()) << BATT_INSPECT(i) << BATT_INSPECT(filter_values);

    start_i = iter->value();
    if (start_i >= end_i) {
      return Interval<u32>{end_i, end_i};
    }

    ++iter;
  }

  if (iter != filter_values.end()) {
    end_i = std::min(end_i, iter->value());
  }

  BATT_CHECK_LT(start_i, end_i) << BATT_INSPECT(i);

  return Interval<u32>{start_i, end_i};
}
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> PackedNodePage::dump() const
{
  return [this](std::ostream& out) {
    out << "PackedNodePage:" << std::endl                              //
        << "  height: " << (i32)this->height.value() << std::endl      //
        << "  pivot_count: " << (i32)this->pivot_count() << std::endl  //
        << "  size_tiered: " << this->is_size_tiered() << std::endl    //
        << "  pivot_keys:" << std::endl;

    usize i = 0;
    for (const Key& key : this->pivot_keys_) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i
          << "] offset=" << (i32)key.pointer.offset.value();
      if (key.pointer) {
        if (i < this->index_of_final_key_end()) {
          out << " data=" << batt::c_str_literal(get_key(key)) << std::endl;
        } else if (i == this->index_of_final_key_end()) {
          out << " (end)" << std::endl;
        } else {
          out << std::endl;
        }
      } else {
        out << std::endl;
      }
      ++i;
    }

    i = 0;
    out << "  pending_bytes:" << std::endl;
    for (const little_u32& count : this->pending_bytes) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "] " << count.value()
          << std::endl;
      ++i;
    }

    i = 0;
    out << "  children:" << std::endl;
    for (const llfs::PackedPageId& child_id : this->children) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "] " << child_id.unpack()
          << std::endl;
      ++i;
    }

    out << "  segments:" << std::endl;
    i = 0;
    for (const UpdateBuffer::Segment& segment : this->update_buffer.segments) {
      u32 filter_start_i = segment.filter_start.value() & ~PackedNodePage::kSegmentStartsFiltered;
      bool start_filtered =
          (segment.filter_start.value() & PackedNodePage::kSegmentStartsFiltered) != 0;
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "]:" << std::endl
          << "     leaf_page_id: " << segment.leaf_page_id.unpack() << std::endl
          << "     active_pivots:  " << std::bitset<64>{segment.active_pivots.value()} << std::endl
          << "     filter_start:  " << filter_start_i << std::endl
          << "     starts_filtered: " << start_filtered << std::endl
          << std::endl;
      ++i;
    }

    out << "  segment_filters:" << std::endl;
    const llfs::PackedArray<little_u32>& packed_filters = *this->update_buffer.segment_filters;
    i = 0;
    for (; i < packed_filters.size(); ++i) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "]:" << std::endl
          << packed_filters[i] << std::endl
          << std::endl;
    }

    out << "  level_start:" << std::endl;
    i = 0;
    for (const little_u8& start : this->update_buffer.level_start) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "]: " << (i32)start.value()
          << std::endl;
      ++i;
    }
  };
}

}  // namespace turtle_kv
