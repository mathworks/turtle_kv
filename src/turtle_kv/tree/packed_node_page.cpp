#include <turtle_kv/tree/packed_node_page.hpp>
//

#include <turtle_kv/tree/algo/nodes.hpp>
#include <turtle_kv/tree/algo/segmented_levels.hpp>
#include <turtle_kv/tree/in_memory_node.hpp>
#include <turtle_kv/tree/node_page_view.hpp>
#include <turtle_kv/tree/subtree.hpp>

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
    dst_key.pointer.offset = byte_distance(std::addressof(dst_key.pointer), copy_dst);
    return (void*)dst_key.pointer.get() == copy_dst;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  const auto pack_u32 = [&variable_buffer](u32 value) -> bool [[nodiscard]] {
    if (variable_buffer.size() < 4) {
      return false;
    }
    little_u32* dst = static_cast<little_u32*>(variable_buffer.data());
    *dst = value;
    variable_buffer += sizeof(little_u32);
    return true;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack everything up to the update buffer

  const usize pivot_count = src_node.pivot_count();

  BATT_CHECK_EQ(src_node.pivot_keys_.size(), pivot_count + 1);
  BATT_CHECK_EQ(src_node.children.size(), pivot_count);
  BATT_CHECK_EQ(src_node.pending_bytes.size(), pivot_count);

  packed_node->height = src_node.height;
  packed_node->pivot_count = BATT_CHECKED_CAST(u8, pivot_count);

  for (usize pivot_i = 0; pivot_i < pivot_count; ++pivot_i) {
    packed_node->pending_bytes[pivot_i] = src_node.pending_bytes[pivot_i];
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

  {
    usize dst_segment_i = 0;
    usize level_i = 0;
    for (; level_i < src_node.update_buffer.levels.size(); ++level_i) {
      packed_node->update_buffer.level_start[level_i] = BATT_CHECKED_CAST(u8, dst_segment_i);

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
        dst_segment.flushed_pivots = src_segment.get_flushed_pivots();

        BATT_CHECK_EQ(bit_count(src_segment.get_flushed_pivots()),
                      src_segment.flushed_item_upper_bound_.size());

        BATT_CHECK_EQ(bit_count(src_segment.get_active_pivots()),
                      bit_count(dst_segment.active_pivots));

        BATT_CHECK_EQ(bit_count(src_segment.get_flushed_pivots()),
                      bit_count(dst_segment.flushed_pivots));

        BATT_CHECK_EQ(((~src_segment.get_active_pivots()) & src_segment.get_flushed_pivots()),
                      u64{0});

        {
          auto* p_item_count =
              std::addressof(packed_node->update_buffer.flushed_item_upper_bound[dst_segment_i]);

          p_item_count->offset = byte_distance(p_item_count, variable_buffer.data());
        }

        for (u32 src_value : src_segment.flushed_item_upper_bound_) {
          BATT_CHECK(pack_u32(src_value));
        }

        ++dst_segment_i;
      }
    }

    // The remainder of the `level_start` array should point to the end of the valid segments range.
    //
    for (; level_i < packed_node->update_buffer.level_start.size(); ++level_i) {
      packed_node->update_buffer.level_start[level_i] = BATT_CHECKED_CAST(u8, dst_segment_i);
    }
  }

  page_header->unused_begin = byte_distance(buffer.data(), variable_buffer.data());
  page_header->unused_end = buffer.size();

  BATT_CHECK_LE(page_header->unused_begin, page_header->unused_end);

  if (/*paraoid_checks==*/false) {
    for (usize level_i = 0; level_i < src_node.get_level_count(); ++level_i) {
      batt::case_of(
          src_node.update_buffer.levels[level_i],
          [&](const EmptyLevel&) {
            const auto dst_level = packed_node->update_buffer.get_level(level_i);
            BATT_CHECK(dst_level.empty());
          },
          [&](const SegmentedLevel& src_level) {
            const auto dst_level = packed_node->update_buffer.get_level(level_i);
            BATT_CHECK_EQ(src_level.segment_count(), dst_level.segment_count());

            for (usize segment_i = 0; segment_i < src_level.segment_count(); ++segment_i) {
              const auto& src_segment = src_level.get_segment(segment_i);
              const auto& dst_segment = dst_level.get_segment(segment_i);

              BATT_CHECK_EQ(src_segment.get_active_pivots(), dst_segment.get_active_pivots());
              BATT_CHECK_EQ(src_segment.get_flushed_pivots(), dst_segment.get_flushed_pivots());

              for (i32 pivot_i = 0; pivot_i < (i32)pivot_count; ++pivot_i) {
                BATT_CHECK_EQ(src_segment.get_flushed_item_upper_bound(src_level, pivot_i),
                              dst_segment.get_flushed_item_upper_bound(dst_level, pivot_i));
              }
            }
          },
          [&](const auto& merged) {
            BATT_PANIC() << "Bad level case! " << batt::name_of<std::decay_t<decltype(merged)>>();
          });
    }
  }

  return packed_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Subtree PackedNodePage::get_child(i32 pivot_i) const
{
  return Subtree{
      .impl = llfs::PageIdSlot::from_page_id(this->children[pivot_i].unpack()),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> PackedNodePage::find_key(llfs::PageLoader& page_loader,
                                             llfs::PinnedPage& pinned_page_out,
                                             const KeyView& key) const
{
  return in_node(*this).find_key(page_loader, pinned_page_out, key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<ValueView> PackedNodePage::find_key_in_level(usize level_i,                      //
                                                      llfs::PageLoader& page_loader,      //
                                                      llfs::PinnedPage& pinned_page_out,  //
                                                      i32 key_pivot_i,                    //
                                                      const KeyView& key) const
{
  UpdateBuffer::SegmentedLevel level = this->update_buffer.get_level(level_i);

  return in_segmented_level(*this, level, page_loader)  //
      .find_key(pinned_page_out, key_pivot_i, key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<llfs::PinnedPage> PackedNodePage::UpdateBuffer::Segment::load_leaf_page(
    llfs::PageLoader& page_loader,
    llfs::PinPageToJob pin_page_to_job) const
{
  return page_loader.get_page_with_layout_in_job(this->leaf_page_id.unpack(),
                                                 LeafPageView::page_layout_id(),
                                                 pin_page_to_job,
                                                 llfs::OkIfNotFound{false});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PackedNodePage::UpdateBuffer::Segment::get_flushed_item_upper_bound(
    const SegmentedLevel& level,
    i32 pivot_i) const
{
  if (!get_bit(this->flushed_pivots, pivot_i)) {
    return 0;
  }

  const UpdateBuffer& update_buffer = *level.update_buffer_;
  const usize segment_i = std::distance(level.segments_slice.begin(), this);
  const Slice<const little_u32> flushed_item_upper_bounds =
      update_buffer.get_flushed_item_upper_bounds(level.level_i_, segment_i);

  const usize index = bit_rank(this->flushed_pivots, pivot_i);

  BATT_CHECK_LT(index, flushed_item_upper_bounds.size());

  return flushed_item_upper_bounds[index];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> PackedNodePage::dump() const
{
  return [this](std::ostream& out) {
    out << "PackedNodePage:" << std::endl                                    //
        << "  height: " << (i32)this->height.value() << std::endl            //
        << "  pivot_count: " << (i32)this->pivot_count.value() << std::endl  //
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
      out << "   - [" << std::setw(2) << std::setfill(' ') << i << "]:" << std::endl
          << "     leaf_page_id: " << segment.leaf_page_id.unpack() << std::endl
          << "     active_pivots:  " << std::bitset<64>{segment.active_pivots.value()} << std::endl
          << "     flushed_pivots: " << std::bitset<64>{segment.flushed_pivots.value()}
          << std::endl;
      ++i;
    }

    out << "  flushed_item_upper_bounds_start:" << std::endl;
    i = 0;
    for (const FlushedItemUpperBoundPointer& pointer :
         this->update_buffer.flushed_item_upper_bound) {
      out << "   - [" << std::setw(2) << std::setfill(' ') << i
          << "]: offset=" << pointer.offset.value() << std::endl;
      ++i;
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
