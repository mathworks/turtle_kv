#pragma once

#include <turtle_kv/tree/packed_node_page_key.hpp>

#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/value_view.hpp>

#include <turtle_kv/util/bit_ops.hpp>
#include <turtle_kv/util/page_buffers.hpp>
#include <turtle_kv/util/placement.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/seq.hpp>
#include <turtle_kv/import/slice.hpp>

#include <llfs/packed_page_id.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>

#include <batteries/assert.hpp>

#include <boost/iterator/iterator_facade.hpp>
#include <boost/range/iterator_range.hpp>

#include <array>
#include <cstdlib>
#include <iterator>
#include <memory>

namespace turtle_kv {

struct Subtree;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedNodePage {
  static constexpr usize kMaxPivots = 64;
  static constexpr usize kMaxSegments = kMaxPivots - 1;
  static constexpr usize kMaxLevels = batt::log2_ceil(kMaxPivots);
  static constexpr usize kPivotKeysSize =
      kMaxPivots + 1 /*max_key*/ + 1 /*common_prefix*/ + 1 /*final_offset*/;

  using Key = PackedNodePageKey;
  using FlushedItemUpperBoundPointer = llfs::PackedPointer<little_u32, little_u16>;

  class KeyIterator
      : public boost::iterator_facade<        //
            KeyIterator,                      // <- Derived
            KeyView,                          // <- Value
            std::random_access_iterator_tag,  // <- CategoryOrTraversal
            KeyView                           // <- Reference
            >
  {
   public:
    using Self = KeyIterator;
    using value_type = KeyView;
    using reference = KeyView;

    //----- --- -- -  -  -   -

    KeyIterator() noexcept : key_{nullptr}, index_{std::numeric_limits<isize>::max()}
    {
    }

    KeyIterator(const Key* key, isize index) noexcept : key_{key}, index_{index}
    {
    }

    //----- --- -- -  -  -   -

    reference dereference() const noexcept;

    bool equal(const Self& other) const noexcept
    {
      return this->key_ == other.key_;
    }

    void increment() noexcept
    {
      ++this->key_;
      ++this->index_;
    }

    void decrement() noexcept
    {
      --this->key_;
      --this->index_;
    }

    void advance(isize delta) noexcept
    {
      this->key_ += delta;
      this->index_ += delta;
    }

    isize distance_to(const Self& other) const noexcept
    {
      return other.key_ - this->key_;
    }

    //----- --- -- -  -  -   -
   private:
    const Key* key_;
    isize index_;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct UpdateBuffer {
    struct SegmentedLevel;

    struct Segment {
      llfs::PackedPageId leaf_page_id;  // +8 -> 8
      little_u64 active_pivots;         // +8 -> 16
      little_u64 flushed_pivots;        // +8 -> 24

      //+++++++++++-+-+--+----- --- -- -  -  -   -

      bool is_pivot_active(i32 pivot_i) const noexcept
      {
        return get_bit(this->active_pivots, pivot_i);
      }

      u64 get_active_pivots() const noexcept
      {
        return this->active_pivots;
      }

      u64 get_flushed_pivots() const noexcept
      {
        return this->flushed_pivots;
      }

      StatusOr<llfs::PinnedPage> load_leaf_page(llfs::PageLoader& page_loader,
                                                llfs::PinPageToJob pin_page_to_job) const noexcept;

      usize get_flushed_item_upper_bound(const SegmentedLevel& level, i32 pivot_i) const noexcept;
    };

    struct SegmentedLevel {
      using Segment = UpdateBuffer::Segment;

      const UpdateBuffer* update_buffer_;
      usize level_i_;
      Slice<const Segment> segments_slice;

      const Slice<const Segment>& get_segments_slice() const noexcept
      {
        return this->segments_slice;
      }

      usize segment_count() const noexcept
      {
        return this->segments_slice.size();
      }

      const Segment& get_segment(usize i) const noexcept
      {
        return this->segments_slice[i];
      }

      bool empty() const noexcept
      {
        return this->segments_slice.empty();
      }
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<Segment, kMaxSegments> segments;             //    +(24*63) -> 1512
    std::array<FlushedItemUpperBoundPointer, kMaxSegments>  //
        flushed_item_upper_bound;                           // +(2*63=126) -> 1638
    std::array<little_u8, kMaxLevels + 1> level_start;      //      +(1*7) -> 1645
    std::array<u8, 3> pad_;                                 // +3 -> 1648 (=8*206)

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    SegmentedLevel get_level(usize level_i) const noexcept
    {
      BATT_CHECK_LT(level_i, kMaxLevels);

      const usize level_begin_i = this->level_start[level_i];
      const usize level_end_i = this->level_start[level_i + 1];

      return SegmentedLevel{
          .update_buffer_ = this,
          .level_i_ = level_i,
          .segments_slice = as_const_slice(std::addressof(this->segments[level_begin_i]),
                                           std::addressof(this->segments[level_end_i])),
      };
    }

    usize segment_count() const noexcept
    {
      return this->level_start.back();
    }

    const Segment* segments_begin() const noexcept
    {
      return this->segments.data();
    }

    const Segment* segments_end() const noexcept
    {
      return this->segments_begin() + this->segment_count();
    }

    Slice<const Segment> segments_slice() const noexcept
    {
      return as_const_slice(this->segments_begin(), this->segments_end());
    }

    Slice<const little_u32> get_flushed_item_upper_bounds(usize level_i,
                                                          usize segment_i) const noexcept
    {
      BATT_CHECK_LT(level_i, kMaxLevels);

      const usize i = this->level_start[level_i] + segment_i;
      BATT_CHECK_LT(i, this->level_start[level_i + 1]);

      const Segment& segment = this->segments[i];
      const usize flushed_pivots_count = bit_count(segment.flushed_pivots);

      const little_u32* const flushed_items_begin = this->flushed_item_upper_bound[i].get();
      const little_u32* const flushed_items_end = flushed_items_begin + flushed_pivots_count;

      return as_const_slice(flushed_items_begin, flushed_items_end);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // TODO [tastolfi 2025-03-16] shrink pad0_ / optimize&compress layout

  little_u8 height;                                     //          +1 -> 1
  little_u8 pivot_count;                                //          +1 -> 2
  std::array<Key, kPivotKeysSize> pivot_keys_;          // +(2*67=134) -> 136 (=4*34)
  std::array<little_u32, kMaxPivots> pending_bytes;     // +(4*64=256) -> 392 (=8*49)
  std::array<llfs::PackedPageId, kMaxPivots> children;  // +(8*64=512) -> 904 (=8/113)
  UpdateBuffer update_buffer;                           //       +1648 -> 2552
  std::array<u8, (4096 - 64 - 2552)> key_and_flushed_item_data_;

  //----- --- -- -  -  -   -
  // (char data: pivot_keys)
  //----- --- -- -  -  -   -

  //----- --- -- -  -  -   -
  // (little_u32 data: flushed_item_count)
  //----- --- -- -  -  -   -

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  static const PackedNodePage& view_of(T&& t) noexcept
  {
    const ConstBuffer buffer = get_page_const_payload(BATT_FORWARD(t));

    BATT_CHECK_GE(buffer.size(), sizeof(PackedNodePage));

    const PackedNodePage& packed_node_page = *static_cast<const PackedNodePage*>(buffer.data());

    BATT_CHECK_LE(packed_node_page.pivot_count, 64);

    return packed_node_page;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const llfs::PackedPageId* children_begin() const noexcept
  {
    return this->children.data();
  }

  const llfs::PackedPageId* children_end() const noexcept
  {
    return this->children_begin() + this->pivot_count;
  }

  Slice<const llfs::PackedPageId> children_slice() const noexcept
  {
    return as_const_slice(this->children_begin(), this->children_end());
  }

  Subtree get_child(i32 pivot_i) const noexcept;

  //----- --- -- -  -  -   -

  usize index_of_key_lower_bound() const noexcept
  {
    return 0;
  }

  usize index_of_key_upper_bound() const noexcept
  {
    return this->pivot_count;
  }

  usize index_of_min_key() const noexcept
  {
    return 0;
  }

  usize index_of_max_key() const noexcept
  {
    return this->pivot_count + 1;
  }

  usize index_of_common_key_prefix() const noexcept
  {
    return this->pivot_count + 2;
  }

  usize index_of_final_key_end() const noexcept
  {
    return this->pivot_count + 3;
  }

  //----- --- -- -  -  -   -

  usize pivot_keys_size() const noexcept
  {
    return this->pivot_count + 1;
  }

  KeyIterator pivot_keys_begin() const noexcept
  {
    return KeyIterator{this->pivot_keys_.data(), 0};
  }

  KeyIterator pivot_keys_end() const noexcept
  {
    return this->pivot_keys_begin() + this->pivot_keys_size();
  }

  boost::iterator_range<KeyIterator> get_pivot_keys() const noexcept
  {
    return boost::iterator_range<KeyIterator>{
        this->pivot_keys_begin(),
        this->pivot_keys_end(),
    };
  }

  KeyView get_pivot_key(usize pivot_i) const noexcept
  {
    KeyIterator iter{std::addressof(this->pivot_keys_[pivot_i]), static_cast<isize>(pivot_i)};
    return iter.dereference();
  }

  //----- --- -- -  -  -   -

  KeyView min_key() const noexcept
  {
    return this->get_pivot_key(this->index_of_min_key());
  }

  KeyView max_key() const noexcept
  {
    return this->get_pivot_key(this->index_of_max_key());
  }

  KeyView common_key_prefix() const noexcept
  {
    return this->get_pivot_key(this->index_of_common_key_prefix());
  }

  //----- --- -- -  -  -   -

  usize get_level_count() const noexcept
  {
    // TODO [tastolfi 2025-03-22] optimize using log2_ceil(pivot_count) here?
    //
    return kMaxLevels;
  }

  StatusOr<ValueView> find_key(llfs::PageLoader& page_loader, llfs::PinnedPage& pinned_page_out,
                               const KeyView& key) const noexcept;

  StatusOr<ValueView> find_key_in_level(usize level_i,                      //
                                        llfs::PageLoader& page_loader,      //
                                        llfs::PinnedPage& pinned_page_out,  //
                                        i32 key_pivot_i,                    //
                                        const KeyView& key) const noexcept;

  //----- --- -- -  -  -   -

  std::function<void(std::ostream&)> dump() const noexcept;
};

// Verify the packed structure of PackedNodePage::UpdateBuffer::Segment.
//
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer::Segment, leaf_page_id, 0, 8, 8);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer::Segment, active_pivots, 8, 16, 8);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer::Segment, flushed_pivots, 16, 24, 8);
BATT_STATIC_ASSERT_EQ(sizeof(PackedNodePage::UpdateBuffer::Segment), 24);

// Verify the packed structure of PackedNodePage::FlushedItemUpperBoundPointer.
//
BATT_STATIC_ASSERT_EQ(sizeof(PackedNodePage::FlushedItemUpperBoundPointer), 2);

// Verify the packed structure of PackedNodePage::UpdateBuffer.
//
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer, segments, 0, 1512, 8);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer, flushed_item_upper_bound, 1512, 1638, 4);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer, level_start, 1638, 1645, 1);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage::UpdateBuffer, pad_, 1645, 1648, 1);
BATT_STATIC_ASSERT_EQ(sizeof(PackedNodePage::UpdateBuffer), 1648);

// Verify the packed structure of PackedNodePage.
//
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, height, 0, 1, 1);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, pivot_count, 1, 2, 1);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, pivot_keys_, 2, 136, 2);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, pending_bytes, 136, 392, 4);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, children, 392, 904, 8);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, update_buffer, 904, 2552, 8);
TURTLE_KV_ASSERT_PLACEMENT(PackedNodePage, key_and_flushed_item_data_, 2552, 4032, 1);

// PackedNodePage plus the page header should exactly fit a 4kb page.
//
BATT_STATIC_ASSERT_EQ(sizeof(llfs::PackedPageHeader), 64);
BATT_STATIC_ASSERT_EQ(sizeof(PackedNodePage) + sizeof(llfs::PackedPageHeader), 4096);

struct InMemoryNode;

/** \brief Packs the InMemoryNode into `buffer`.
 *
 * `buffer` is the *entire* page buffer, including the llfs::PackedPageHeader.
 *
 * Panics if src_node is *not* in a packable state (i.e., it must contain no MergedLevels and all
 * subtrees must be serialized).
 */
PackedNodePage* build_node_page(const MutableBuffer& buffer, const InMemoryNode& src_node) noexcept;

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto PackedNodePage::KeyIterator::dereference() const noexcept -> reference
{
  KeyView key_view = get_key(*this->key_);
  if (key_view.empty() && this->index_ != 0) {
    return global_max_key();
  }
  return key_view;
}

}  // namespace turtle_kv
