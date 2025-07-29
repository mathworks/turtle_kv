#pragma once

#include <turtle_kv/tree/segmented_level_scanner.hpp>

#include <turtle_kv/core/sharded_key_value_slice.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>

namespace turtle_kv {

/** \brief Contains the necessary data needed to initialize a ShardedKeyValueSlice.
 */
struct SliceData {
  isize key_offset_delta;
  usize value_lower_bound;
  usize end_i;
  const PackedKeyValue* end_i_pkv;
  const PackedKeyValue* start_i_pkv;
};

/** \brief When we load a slice of key offsets/pointers, we save some information about that slice
 * so that we can try reusing the slice (and avoid another sharded page load) in later calls to
 * ShardedLevelScanner::next.
 */
struct CachedItems {
  usize upper_bound;
  ConstBuffer items_buffer;

  void reset()
  {
    this->upper_bound = 0;
    this->items_buffer = ConstBuffer{};
  }
};

template <typename PinnedLeafT>
struct FullLeafData {
  bool needs_load = true;
  PinnedLeafT leaf_page;
};

template <typename NodeT, typename LevelT, typename PageLoaderT>
class ShardedLevelScanner : private SegmentedLevelScannerBase
{
 public:
  using Self = ShardedLevelScanner;
  using Super = SegmentedLevelScannerBase;
  using Node = NodeT;
  using Level = LevelT;
  using PageLoader = PageLoaderT;
  using PinnedPageT = typename PageLoader::PinnedPageT;
  using Segment = typename Level::Segment;

  using Item = ShardedKeyValueSlice;

  static constexpr u64 kDefaultLeafShardedViewSize = 4096;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ShardedLevelScanner(Node& node,
                               Level& level,
                               PageLoader& loader,
                               PageSliceStorage& slice_storage,
                               llfs::PinPageToJob pin_pages_to_job,
                               Status& status,
                               llfs::PageSize trie_index_size,
                               i32 min_pivot_i = 0,
                               Optional<KeyView> min_key = None) noexcept;

  explicit ShardedLevelScanner(Node& node,
                               Level& level,
                               PageLoader& loader,
                               PageSliceStorage& slice_storage,
                               llfs::PinPageToJob pin_pages_to_job,
                               llfs::PageSize trie_index_size,
                               i32 min_pivot_i = 0,
                               Optional<KeyView> min_key = None) noexcept
      : ShardedLevelScanner{node,
                            level,
                            loader,
                            slice_storage,
                            pin_pages_to_job,
                            this->Super::self_contained_status_,
                            trie_index_size,
                            min_pivot_i,
                            min_key}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Seq methods

  Optional<Item> peek();

  Optional<Item> next();

  Status status() const
  {
    return this->status_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Optional<Item> peek_next_impl(bool advance);

  Optional<Item> peek_next_impl_full_leaf(bool advance);

  void advance_segment();

  Status advance_to_pivot(usize target_pivot_i, const Segment& segment) noexcept;

  void advance_to_pivot_full_leaf(usize target_pivot_i,
                                  const Segment& segment,
                                  const PackedLeafPage& leaf_page);

  StatusOr<SliceData> init_slice_data(const KeyView& gap_pivot_key) noexcept;

  Interval<usize> get_trie_search_range(const KeyView& key);

  Status set_start_item(usize flushed_upper_bound,
                        Optional<KeyView> lower_bound_key = None,
                        Optional<Interval<usize>> search_range = None) noexcept;

  Status try_full_leaf_load(const Segment& segment) noexcept;

  void update_cached_items(usize prev_item_i);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  Node* node_;
  Level* level_;
  PageLoader* loader_;
  PageSliceStorage* slice_storage_;
  ConstBuffer head_shard_slice_;
  CachedItems cached_items_;
  PageSliceReader slice_reader_;
  llfs::PageSize trie_index_sharded_view_size_;
  llfs::PinPageToJob pin_pages_to_job_;
  Status& status_;
  Optional<FullLeafData<PinnedPageT>> full_leaf_data_;
  Optional<KeyView> min_key_;
  usize segment_i_;
  usize item_i_;
  i32 min_pivot_i_;
  i32 pivot_i_;
  batt::BoolStatus load_full_leaf_;
  bool passed_min_key_;
  bool hit_gap_pivot_key_;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline /*explicit*/ ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::ShardedLevelScanner(
    Node& node,
    Level& level,
    PageLoader& loader,
    PageSliceStorage& slice_storage,
    llfs::PinPageToJob pin_pages_to_job,
    Status& status,
    llfs::PageSize trie_index_size,
    i32 min_pivot_i,
    Optional<KeyView> min_key) noexcept
    : node_{std::addressof(node)}
    , level_{std::addressof(level)}
    , loader_{std::addressof(loader)}
    , slice_storage_{std::addressof(slice_storage)}
    , slice_reader_{BATT_FORWARD(loader),
                    llfs::PageId{},
                    llfs::PageSize{kDefaultLeafShardedViewSize}}
    , trie_index_sharded_view_size_{trie_index_size}
    , pin_pages_to_job_{pin_pages_to_job}
    , status_{status}
    , min_key_{min_key}
    , segment_i_{0}
    , item_i_{0}
    , min_pivot_i_{min_pivot_i}
    , pivot_i_{0}
    , load_full_leaf_{batt::BoolStatus::kUnknown}
    , passed_min_key_{false}
    , hit_gap_pivot_key_{false}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::peek() -> Optional<Item>
{
  if (this->load_full_leaf_ == batt::BoolStatus::kTrue) {
    return this->peek_next_impl_full_leaf(false);
  }
  return this->peek_next_impl(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::next() -> Optional<Item>
{
  if (this->load_full_leaf_ == batt::BoolStatus::kTrue) {
    return this->peek_next_impl_full_leaf(true);
  }
  return this->peek_next_impl(true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::peek_next_impl(bool advance)
    -> Optional<Item>
{
  // Errors are final; check the current status.
  //
  if (!this->status_.ok()) {
    return None;
  }

  // If the current segment is past the end, return None.
  //
  if (this->segment_i_ == this->level_->segment_count()) {
    return None;
  }

  const Segment* segment = std::addressof(this->level_->get_segment(this->segment_i_));

  u64 active_pivots = segment->get_active_pivots();
  BATT_CHECK_NE(active_pivots, 0) << "This segment should have been dropped!";

  // If this->head_shard_slice_ is empty, we need haven't looked at the leaf corresponding to the
  // segment we are looking at. Thus, we need to load the "head shard", or the part of the leaf page
  // containing the trie index.
  //
  if (this->head_shard_slice_.size() == 0) {
    // Skip ahead to the next segment that is active at or past the minimum pivot.
    //
    while (last_bit(active_pivots) < this->min_pivot_i_) {
      ++this->segment_i_;
      if (this->segment_i_ == this->level_->segment_count()) {
        return None;
      }
      segment = std::addressof(this->level_->get_segment(this->segment_i_));
      active_pivots = segment->get_active_pivots();
    }

    i32 target_pivot_i = std::max(first_bit(active_pivots), this->min_pivot_i_);
    while (target_pivot_i < (i32)this->node_->pivot_count() &&
           !get_bit(active_pivots, target_pivot_i)) {
      ++target_pivot_i;
    }

    if (this->load_full_leaf_ == batt::BoolStatus::kUnknown) {
      Status load_leaf = this->try_full_leaf_load(*segment);
      if (load_leaf.ok()) {
        this->load_full_leaf_ = batt::BoolStatus::kTrue;
        this->advance_to_pivot_full_leaf(
            target_pivot_i,
            *segment,
            PackedLeafPage::view_of(this->full_leaf_data_->leaf_page.get_page_buffer()));
        return this->peek_next_impl_full_leaf(advance);
      } else {
        this->load_full_leaf_ = batt::BoolStatus::kFalse;
      }
    }

    // Read the head shard!
    //
    this->slice_reader_.set_page_id(segment->get_leaf_page_id());

    StatusOr<ConstBuffer> head_buffer =
        this->slice_reader_.read_slice(this->trie_index_sharded_view_size_,
                                       Interval<usize>{0, this->trie_index_sharded_view_size_},
                                       *(this->slice_storage_),
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kTrieIndexLruPriority});
    if (!head_buffer.ok()) {
      this->status_ = head_buffer.status();
      return None;
    }
    this->head_shard_slice_ = *head_buffer;

    // Advance to `target_pivot_i` in this segment.
    //
    Status advance_pivot_status = this->advance_to_pivot(target_pivot_i, *segment);
    if (!advance_pivot_status.ok()) {
      this->status_ = advance_pivot_status;
      return None;
    }

    const void* page_start = this->head_shard_slice_.data();
    const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
    const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

    // If advancing to the next pivot set a starting item past this leaf's key range, move on to the
    // next segment/leaf. It is likely that this->min_key_ is larger than all keys in this current
    // leaf.
    //
    if (this->item_i_ >= packed_leaf_page.key_count) {
      this->advance_segment();
      return ShardedKeyValueSlice{};
    }
  }

  const void* page_start = this->head_shard_slice_.data();
  const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
  const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

  const i32 next_inactive_pivot_i = next_bit(~active_pivots, this->pivot_i_);
  const i32 next_flushed_pivot_i = next_bit(segment->get_flushed_pivots(), this->pivot_i_);
  const i32 next_gap_pivot_i = std::min(next_inactive_pivot_i, next_flushed_pivot_i);

  // Compute the next gap pivot key, and use that to compute the end of the slice to return, as well
  // as other necessary data needed for a ShardedKeyValueSlice.
  //
  const KeyView gap_pivot_key = this->node_->get_pivot_key(next_gap_pivot_i);
  StatusOr<SliceData> slice_data = this->init_slice_data(gap_pivot_key);
  if (!slice_data.ok()) {
    this->status_ = slice_data.status();
    return None;
  }

  ShardedKeyValueSlice scanned_items;
  if (this->item_i_ < slice_data->end_i) {
    scanned_items = ShardedKeyValueSlice{slice_data->start_i_pkv,
                                         slice_data->end_i_pkv,
                                         slice_data->key_offset_delta,
                                         slice_data->value_lower_bound,
                                         segment->get_leaf_page_id(),
                                         this->loader_,
                                         this->slice_storage_};
  }

  if (advance) {
    // If we have set `gap_pivot_key` to be the ending item in the `scanned_items` slice, advance
    // accordingly.
    //
    usize prev_item_i = this->item_i_;
    if (this->hit_gap_pivot_key_) {
      if (next_gap_pivot_i == next_inactive_pivot_i) {
        const usize next_pivot_i =
            (next_inactive_pivot_i < 64) ? next_bit(active_pivots, next_inactive_pivot_i) : 64;

        if (next_pivot_i < this->node_->pivot_count()) {
          Status advance_pivot_status = this->advance_to_pivot(next_pivot_i, *segment);
          this->status_ = advance_pivot_status;
          this->update_cached_items(prev_item_i);
        } else {
          this->advance_segment();
        }
      } else {
        this->pivot_i_ = next_flushed_pivot_i;

        BATT_CHECK_LT(this->pivot_i_, this->node_->pivot_count())
            << BATT_INSPECT(next_inactive_pivot_i) << BATT_INSPECT(next_gap_pivot_i);

        usize flushed_upper_bound =
            segment->get_flushed_item_upper_bound(*this->level_, next_flushed_pivot_i);
        BATT_CHECK_LT(flushed_upper_bound, packed_leaf_page.key_count);

        this->status_ = this->set_start_item(flushed_upper_bound);
        this->update_cached_items(prev_item_i);
      }

      this->hit_gap_pivot_key_ = false;
    } else {
      // If we have not hit the next gap pivot key yet, set the starting item for the next call to
      // `peek_next_impl` to be the end of the current slice that we returning.
      //
      this->item_i_ = slice_data->end_i;
      if (this->item_i_ >= packed_leaf_page.key_count) {
        this->advance_segment();
        this->hit_gap_pivot_key_ = false;
      } else {
        this->update_cached_items(prev_item_i);
      }
    }
  }

  return scanned_items;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline auto ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::peek_next_impl_full_leaf(bool advance)
    -> Optional<Item>
{
  // Errors are final; check the current status.
  //
  if (!this->status_.ok()) {
    return None;
  }

  // If the current segment is past the end, return None.
  //
  if (this->segment_i_ == this->level_->segment_count()) {
    return None;
  }

  const Segment* segment = std::addressof(this->level_->get_segment(this->segment_i_));

  u64 active_pivots = segment->get_active_pivots();
  BATT_CHECK_NE(active_pivots, 0) << "This segment should have been dropped!";

  BATT_CHECK(this->full_leaf_data_);
  if (this->full_leaf_data_->needs_load) {
    while (last_bit(active_pivots) < this->min_pivot_i_) {
      ++this->segment_i_;
      if (this->segment_i_ == this->level_->segment_count()) {
        return None;
      }
      segment = std::addressof(this->level_->get_segment(this->segment_i_));
      active_pivots = segment->get_active_pivots();
    }

    // Try to load the page for this segment.
    //
    StatusOr<PinnedPageT> loaded_page =
        segment->load_leaf_page(*this->loader_, this->pin_pages_to_job_);

    if (!loaded_page.ok()) {
      this->status_ = loaded_page.status();
      VLOG(1) << "Failed to load page: " << BATT_INSPECT(loaded_page.status())
              << BATT_INSPECT((int)this->pin_pages_to_job_);
      return None;
    }

    this->full_leaf_data_.emplace(false, std::move(*loaded_page));

    i32 target_pivot_i = std::max(first_bit(active_pivots), this->min_pivot_i_);
    while (target_pivot_i < (i32)this->node_->pivot_count() &&
           !get_bit(active_pivots, target_pivot_i)) {
      ++target_pivot_i;
    }

    this->advance_to_pivot_full_leaf(
        target_pivot_i,
        *segment,
        PackedLeafPage::view_of(this->full_leaf_data_->leaf_page.get_page_buffer()));
  }

  const PackedLeafPage& leaf_page =
      PackedLeafPage::view_of(this->full_leaf_data_->leaf_page.get_page_buffer());

  const i32 next_inactive_pivot_i = next_bit(~active_pivots, this->pivot_i_);
  const i32 next_flushed_pivot_i = next_bit(segment->get_flushed_pivots(), this->pivot_i_);
  const i32 next_gap_pivot_i = std::min(next_inactive_pivot_i, next_flushed_pivot_i);

  const usize begin_i = this->item_i_;

  const usize end_i = [&]() -> usize {
    const KeyView gap_pivot_key = this->node_->get_pivot_key(next_gap_pivot_i);

    // The end of the next slice is the position of the gap pivot key's lower bound.
    //
    return std::distance(leaf_page.items_begin(),  //
                         leaf_page.lower_bound(gap_pivot_key));
  }();

  if (advance) {
    if (next_gap_pivot_i == next_inactive_pivot_i) {
      const usize next_pivot_i =
          (next_inactive_pivot_i < 64) ? next_bit(active_pivots, next_inactive_pivot_i) : 64;

      if (next_pivot_i < this->node_->pivot_count()) {
        this->advance_to_pivot_full_leaf(next_pivot_i, *segment, leaf_page);
      } else {
        this->advance_segment();
      }
    } else {
      this->pivot_i_ = next_flushed_pivot_i;

      BATT_CHECK_LT(this->pivot_i_, this->node_->pivot_count())
          << BATT_INSPECT(next_inactive_pivot_i) << BATT_INSPECT(next_gap_pivot_i);

      this->item_i_ = segment->get_flushed_item_upper_bound(*this->level_, next_flushed_pivot_i);

      BATT_CHECK_LT(this->item_i_, leaf_page.key_count);
    }
  }

  return ShardedKeyValueSlice{leaf_page.items_begin() + begin_i, leaf_page.items_begin() + end_i};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline void ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::advance_segment()
{
  ++this->segment_i_;

  if (this->load_full_leaf_ == batt::BoolStatus::kTrue) {
    this->full_leaf_data_.emplace();
  } else {
    this->head_shard_slice_ = ConstBuffer{};
    this->cached_items_.reset();
    this->passed_min_key_ = false;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline Status ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::advance_to_pivot(
    usize target_pivot_i,
    const Segment& segment) noexcept
{
  BATT_CHECK_LT(target_pivot_i, this->node_->pivot_count());

  this->pivot_i_ = target_pivot_i;

  const usize flushed_upper_bound =
      segment.get_flushed_item_upper_bound(*this->level_, this->pivot_i_);

  Optional<KeyView> search_key;
  Optional<Interval<usize>> search_range;

  // If the flushed upper bound of this pivot is not 0, look at the flushed upper bound key index
  // and this->min_key_ (if provided) to help determine the starting key index, this->item_i_. Else,
  // look at the pivot's lower bound key.
  //
  //
  if (flushed_upper_bound != 0) {
    // If a min key is provided, only look at it if we haven't "passed" it yet. Once we use it to
    // set this->item_i_, we won't need it again as we advance to another pivot.
    //
    if (this->min_key_ && !this->passed_min_key_) {
      const Interval<usize> min_key_range = this->get_trie_search_range(*(this->min_key_));
      // We need to consider the min_key if the flushed upper bound index falls within the search
      // range.
      //
      if (flushed_upper_bound < min_key_range.upper_bound) {
        search_key = this->min_key_;
        search_range = min_key_range;
      } else {
        search_key = None;
        search_range = None;
      }
    } else {
      search_key = None;
      search_range = None;
    }
  } else {
    const KeyView pivot_lower_bound_key = this->node_->get_pivot_key(this->pivot_i_);

    const KeyView lower_bound_key =
        this->min_key_ ? std::max(*this->min_key_, pivot_lower_bound_key, KeyOrder{})
                       : pivot_lower_bound_key;

    search_key = lower_bound_key;
    search_range = this->get_trie_search_range(lower_bound_key);
  }

  return this->set_start_item(flushed_upper_bound, search_key, search_range);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline void ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::advance_to_pivot_full_leaf(
    usize target_pivot_i,
    const Segment& segment,
    const PackedLeafPage& leaf_page)
{
  BATT_CHECK_LT(target_pivot_i, this->node_->pivot_count());

  this->pivot_i_ = target_pivot_i;

  const KeyView pivot_lower_bound_key = this->node_->get_pivot_key(this->pivot_i_);

  const KeyView lower_bound_key = this->min_key_
                                      ? std::max(*this->min_key_, pivot_lower_bound_key, KeyOrder{})
                                      : pivot_lower_bound_key;
  const usize flushed_upper_bound =
      segment.get_flushed_item_upper_bound(*this->level_, this->pivot_i_);

  if (flushed_upper_bound != 0) {
    if (this->min_key_) {
      this->item_i_ = std::max(flushed_upper_bound,
                               (usize)std::distance(leaf_page.items_begin(),  //
                                                    leaf_page.lower_bound(lower_bound_key)));
    } else {
      this->item_i_ = flushed_upper_bound;
    }
  } else {
    this->item_i_ = std::distance(leaf_page.items_begin(),  //
                                  leaf_page.lower_bound(lower_bound_key));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline StatusOr<SliceData> ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::init_slice_data(
    const KeyView& gap_pivot_key) noexcept
{
  const void* page_start = this->head_shard_slice_.data();
  const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
  const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

  BATT_CHECK_LT(this->item_i_, packed_leaf_page.key_count);

  const PackedKeyValue* head_items = packed_leaf_page.items->data();

  // Compute the nearest shard size aligned boundary for loading the key offset data.
  //
  usize item_i_offset = (usize)byte_distance(page_start, head_items + this->item_i_);
  usize item_nearest_aligned_boundary = (item_i_offset + (kDefaultLeafShardedViewSize - 1)) &
                                        ~((1 << batt::log2_ceil(kDefaultLeafShardedViewSize)) - 1);
  //                                       ^^^^
  // kDefaultLeafShardedViewSize is always a power of 2, so doing log, 1 << isn't needed.

  if (item_i_offset == item_nearest_aligned_boundary) {
    item_nearest_aligned_boundary += kDefaultLeafShardedViewSize;
  }
  usize aligned_boundary_i =
      this->item_i_ + ((item_nearest_aligned_boundary - item_i_offset) / sizeof(PackedKeyValue));

  // For any key loaded in shards, we need at least 2 keys loaded after it to compute key and value
  // size. If this->item_i_ is 3 keys (or less) away from the boundary we computed above, we should
  // cross the boundary and take as many of these keys as we can.
  //
  if (aligned_boundary_i < this->item_i_ + 4) {
    if (this->item_i_ == packed_leaf_page.key_count - 1) {
      aligned_boundary_i = packed_leaf_page.key_count + 2;
    } else {
      usize boundary_distance = aligned_boundary_i - this->item_i_;
      aligned_boundary_i = this->item_i_ + boundary_distance + 2;
    }
  } else {
    // Clamp the boundary to the bounds of the key set.
    //
    if (aligned_boundary_i > packed_leaf_page.key_count + 2) {
      aligned_boundary_i = packed_leaf_page.key_count + 2;
    }
  }

  StatusOr<ConstBuffer> items_buffer;
  if (this->cached_items_.items_buffer.size() != 0 &&
      aligned_boundary_i <= this->cached_items_.upper_bound) {
    items_buffer = this->cached_items_.items_buffer;
  } else {
    usize items_slice_upper_bound = aligned_boundary_i;
    Interval<usize> items_slice{item_i_offset,
                                (usize)byte_distance(page_start, head_items + aligned_boundary_i)};
    items_buffer = this->slice_reader_.read_slice(items_slice,
                                                  *(this->slice_storage_),
                                                  this->pin_pages_to_job_,
                                                  llfs::LruPriority{kLeafItemsShardLruPriority});
    if (!items_buffer.ok()) {
      return items_buffer.status();
    }

    this->cached_items_.upper_bound = items_slice_upper_bound;
    this->cached_items_.items_buffer = *items_buffer;
  }

  const auto items_begin = (const PackedKeyValue*)items_buffer->data();
  const auto items_end = items_begin + (aligned_boundary_i - this->item_i_);

  // Similar to what we did above, compute the nearest shard size aligned boundary for loading the
  // key data.
  //
  usize item_i_key_offset = item_i_offset + items_begin->key_offset;
  usize key_nearest_aligned_boundary = (item_i_key_offset + (kDefaultLeafShardedViewSize - 1)) &
                                       ~((1 << batt::log2_ceil(kDefaultLeafShardedViewSize)) - 1);
  if (item_i_key_offset == key_nearest_aligned_boundary) {
    key_nearest_aligned_boundary += kDefaultLeafShardedViewSize;
  }

  // Search the key offset data that we just loaded to find the last key that aligns with the
  // computed boundary.
  //
  const PackedKeyValue* end_tmp = std::prev(
      std::upper_bound(items_begin,
                       std::prev(items_end, 2),  // Since we need two keys past to load a given key
                       key_nearest_aligned_boundary,
                       PackedKeyOffsetCompare{item_i_offset, items_begin}));

  const PackedKeyValue* end_i_pkv;
  if (end_tmp < std::next(items_begin, 3)) {
    // If the computed end forces us to cross the computed key data boundary, take as many keys as
    // possible before crossing over.
    //
    if (items_begin == end_tmp) {
      end_tmp = std::next(items_begin);
    }
    const PackedKeyValue* items_iter = items_begin;
    usize i = this->item_i_;
    usize item_iter_absolute_offset = item_i_key_offset;
    while (i < packed_leaf_page.key_count && items_iter < end_tmp &&
           item_iter_absolute_offset < key_nearest_aligned_boundary) {
      items_iter = std::next(items_iter);
      ++i;
      item_iter_absolute_offset =
          (item_i_offset + (std::distance(items_begin, items_iter) * sizeof(PackedKeyValue))) +
          items_iter->key_offset;
    }

    end_i_pkv = items_iter;
  } else {
    end_i_pkv = std::prev(end_tmp, 2);
  }

  usize current_end_offset =
      item_i_offset + (std::distance(items_begin, end_i_pkv) * sizeof(PackedKeyValue));
  usize end_item_offset = current_end_offset + (2 * sizeof(PackedKeyValue));

  Interval<usize> key_data_slice{item_i_key_offset, end_item_offset + (end_i_pkv + 1)->key_offset};
  StatusOr<ConstBuffer> key_data_buffer =
      this->slice_reader_.read_slice(key_data_slice,
                                     *(this->slice_storage_),
                                     this->pin_pages_to_job_,
                                     llfs::LruPriority{kLeafKeyDataShardLruPriority});
  if (!key_data_buffer.ok()) {
    return key_data_buffer.status();
  }

  const isize offset_base = items_begin->key_offset;
  const isize offset_target = byte_distance(items_begin, key_data_buffer->data());
  const isize offset_delta = offset_target - offset_base;

  // Now, search for the gap pivot key in our key data to ensure that we don't cross this key.
  //
  const PackedKeyValue* gap_end_pkv = std::lower_bound(items_begin,
                                                       end_i_pkv,
                                                       gap_pivot_key,
                                                       ShiftedPackedKeyDataCompare{offset_delta});

  if (std::distance(items_begin, gap_end_pkv) < std::distance(items_begin, end_i_pkv)) {
    end_i_pkv = gap_end_pkv;
    this->hit_gap_pivot_key_ = true;
  }

  usize current_end_i = this->item_i_ + std::distance(items_begin, end_i_pkv);
  if (this->item_i_ < current_end_i) {
    // If searching for the gap pivot key maintained the constraint this->item_i_ < end_i, set the
    // page offset of the start of the value data, which will be loaded on demand.
    //
    usize front_value_lower_bound =
        key_data_slice.lower_bound +
        (usize)byte_distance(key_data_buffer->data(),
                             items_begin->shifted_value_data(offset_delta));

    return SliceData{offset_delta, front_value_lower_bound, current_end_i, end_i_pkv, items_begin};
  } else {
    return SliceData{};
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline Interval<usize> ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::get_trie_search_range(
    const KeyView& key)
{
  BATT_CHECK_NE(this->head_shard_slice_.size(), 0);

  const void* page_start = this->head_shard_slice_.data();
  const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
  const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

  // Sanity check; make sure this is a leaf!
  //
  packed_leaf_page.check_magic();

  // There should be a Trie index, and it should have fit inside the head_buffer; sanity
  // check these assertions.
  //
  const void* trie_begin = packed_leaf_page.trie_index.get();
  const void* trie_end = advance_pointer(trie_begin, packed_leaf_page.trie_index_size);
  BATT_CHECK_LE(byte_distance(page_start, trie_end), this->trie_index_sharded_view_size_);

  return packed_leaf_page.calculate_search_range(key);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline Status ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::set_start_item(
    usize flushed_upper_bound,
    Optional<KeyView> lower_bound_key,
    Optional<Interval<usize>> search_range) noexcept
{
  if (!search_range) {
    // In this case, we know for sure that the flushed upper bound will be our starting item.
    //
    this->item_i_ = flushed_upper_bound;
  } else {
    // If we are deciding between the flushed upper bound key and another key that we have a search
    // range for, we need to use the search range to narrow down thats key's index.
    //
    const void* page_start = this->head_shard_slice_.data();
    const void* payload_start = advance_pointer(page_start, sizeof(llfs::PackedPageHeader));
    const auto& packed_leaf_page = *static_cast<const PackedLeafPage*>(payload_start);

    const PackedKeyValue* head_items = packed_leaf_page.items->data();
    const Interval<usize> items_slice{
        (usize)byte_distance(page_start, head_items + search_range->lower_bound),
        (usize)byte_distance(page_start, head_items + (search_range->upper_bound + 2)),
    };

    StatusOr<ConstBuffer> items_buffer =
        this->slice_reader_.read_slice(items_slice,
                                       *(this->slice_storage_),
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kLeafItemsShardLruPriority});
    if (!items_buffer.ok()) {
      return items_buffer.status();
    }

    const auto items_begin = (const PackedKeyValue*)items_buffer->data();
    const auto items_end = items_begin + search_range->size();
    Interval<usize> key_data_slice{
        (usize)(items_slice.lower_bound + items_begin->key_offset),
        (usize)(items_slice.upper_bound + (items_end + 1)->key_offset),
    };

    StatusOr<ConstBuffer> key_data_buffer =
        this->slice_reader_.read_slice(key_data_slice,
                                       *(this->slice_storage_),
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kLeafKeyDataShardLruPriority});
    if (!key_data_buffer.ok()) {
      return key_data_buffer.status();
    }

    const isize offset_base = items_begin->key_offset;
    const isize offset_target = byte_distance(items_begin, key_data_buffer->data());
    const isize offset_delta = offset_target - offset_base;

    // Search for the given key within the key range we loaded.
    //
    const PackedKeyValue* found_item = std::lower_bound(items_begin,  //
                                                        items_end,
                                                        *lower_bound_key,
                                                        ShiftedPackedKeyOrder{offset_delta});

    if (found_item == items_end) {
      this->item_i_ = search_range->upper_bound;
    } else {
      usize lower_bound_key_i = search_range->lower_bound + std::distance(items_begin, found_item);
      if (flushed_upper_bound != 0) {
        // If the flushed upper bound is non zero, take the max between the min key index and the
        // flushed upper bound index.
        //
        this->item_i_ = std::max(flushed_upper_bound, lower_bound_key_i);
        this->passed_min_key_ = true;
      } else {
        this->item_i_ = lower_bound_key_i;
      }
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline Status ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::try_full_leaf_load(
    const Segment& segment) noexcept
{
  llfs::PageId leaf_page_id = segment.get_leaf_page_id();
  StatusOr<PinnedPageT> loaded_page = this->loader_->try_pin_cached_page(  //
      leaf_page_id,
      llfs::PageLoadOptions{
          LeafPageView::page_layout_id(),
          this->pin_pages_to_job_,
          llfs::LruPriority{kLeafLruPriority},
      });

  if (loaded_page.ok()) {
    this->full_leaf_data_.emplace(false, std::move(*loaded_page));
  }

  return loaded_page.status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename LevelT, typename PageLoaderT>
inline void ShardedLevelScanner<NodeT, LevelT, PageLoaderT>::update_cached_items(usize prev_item_i)
{
  if (this->item_i_ >= this->cached_items_.upper_bound) {
    this->cached_items_.reset();
  } else {
    usize index_diff = this->item_i_ - prev_item_i;
    this->cached_items_.items_buffer += index_diff * sizeof(PackedKeyValue);
  }
}
}  // namespace turtle_kv
