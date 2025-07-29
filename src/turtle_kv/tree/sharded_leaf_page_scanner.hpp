#pragma once

#include <turtle_kv/config.hpp>
//

#include <turtle_kv/tree/packed_leaf_page.hpp>
#include <turtle_kv/tree/tree_options.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>

#include <turtle_kv/import/status.hpp>

#include <batteries/checked_cast.hpp>

#include <algorithm>

namespace turtle_kv {

class ShardedLeafPageScanner
{
 public:
  using Self = ShardedLeafPageScanner;

  static constexpr usize round_down_to_shard_offset(usize n)
  {
    return n & ~(kDefaultLeafShardedViewSize - 1);
  }

  static constexpr usize round_up_to_shard_offset(usize n)
  {
    return Self::round_down_to_shard_offset(n + (kDefaultLeafShardedViewSize - 1));
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ShardedLeafPageScanner(llfs::PageLoader& page_loader,
                                  llfs::PageId page_id,
                                  llfs::PageSize trie_index_sharded_view_size) noexcept
      : slice_reader_{page_loader, page_id, llfs::PageSize{kDefaultLeafShardedViewSize}}
      , head_shard_size_{trie_index_sharded_view_size}
  {
  }

  explicit ShardedLeafPageScanner(llfs::PageLoader& page_loader,
                                  llfs::PageId page_id,
                                  const TreeOptions& tree_options) noexcept
      : ShardedLeafPageScanner{page_loader, page_id, tree_options.trie_index_sharded_view_size()}
  {
  }

  //----- --- -- -  -  -   -

  /** \brief Loads the head-of-page shard, which includes page header, leaf header, and trie index.
   * This function *must* be called successfully before any other functions on this object.
   */
  Status load_header() noexcept
  {
    if (this->header_ == nullptr) {
      BATT_ASSIGN_OK_RESULT(
          ConstBuffer head_buffer,
          this->slice_reader_.read_slice(this->head_shard_size_,
                                         Interval<usize>{0, this->head_shard_size_},
                                         this->slice_storage_,
                                         llfs::PinPageToJob::kDefault,
                                         llfs::LruPriority{kTrieIndexLruPriority}));

      this->page_start_ = head_buffer.data();
      const void* payload_start = advance_pointer(this->page_start_,  //
                                                  sizeof(llfs::PackedPageHeader));
      this->header_ = static_cast<const PackedLeafPage*>(payload_start);

      // Sanity check; make sure this is a leaf!
      //
      this->header_->check_magic();
      this->sanity_check_trie_index();
    }

    return OkStatus();
  }

  /** \brief Returns the loaded PackedLeafPage header.  Requires prior call to `this->load_header()`
   * with OK status.
   */
  const PackedLeafPage& header() noexcept
  {
    return *this->header_;
  }

  //----- --- -- -  -  -   -

  /** \brief Returns true iff this scanner has a loaded item range.
   */
  bool has_loaded_item_range() const noexcept
  {
    return bool{this->item_range_};
  }

  /** \brief Sets the current item range to the passed index values.  These are indexes into the
   * PackedLeafPage::items array.
   *
   * If `last_item_i` is None, then an ending index is chosen so as to provide the longest possible
   * range that minimizes I/O and data copying.
   *
   * Successful call to this function allows calling the following:
   *  - load_next_item_range
   *  - item_range_empty
   *  -
   */
  Status load_item_range(usize first_item_i,
                         Optional<usize> last_item_i,
                         Optional<usize> min_item_count = None) noexcept
  {
    // Set the item range.
    //
    this->set_item_range(first_item_i, last_item_i, min_item_count);
    if (this->item_range_->lower_bound == this->header_->key_count) {
      return batt::StatusCode::kEndOfStream;
    }

    // Calculate the byte range slice of the page containing the items to search.
    //
    const PackedKeyValue* head_items = this->header_->items->data();
    const Interval<usize> items_page_slice{
        (usize)byte_distance(this->page_start_, head_items + this->item_range_->lower_bound),
        (usize)byte_distance(this->page_start_, head_items + (this->item_range_->upper_bound + 2)),
        //                                                                                     △
        //                1 for the next key so we know key length, 1 more for the next value ─┘
    };

    // Load the shard containing the desired slice of items.
    //
    StatusOr<ConstBuffer> items_buffer =
        this->slice_reader_.read_slice(items_page_slice,
                                       this->slice_storage_,
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kLeafItemsShardLruPriority});
    if (!items_buffer.ok()) {
      return items_buffer.status();
    }

    // Calculate the position of the items slice inside the loaded buffer.
    //
    {
      const auto items_begin = static_cast<const PackedKeyValue*>(items_buffer->data());
      const auto items_end = items_begin + this->item_range_->size();
      this->loaded_items_ = as_slice(items_begin, items_end);
    }

    // Find the corresponding key data slice in the page.
    //
    {
      // Start with the key data range for all loaded items.
      //
      const Interval<usize> unaligned_key_data_page_slice{
          (usize)(items_page_slice.lower_bound + this->loaded_items_.begin()->key_offset),
          (usize)(items_page_slice.upper_bound + (this->loaded_items_.end() + 1)->key_offset),
          //                                                                   △
          //                1 more for the next value, so we know values size ─┘
      };

      // Find the end of key data for the minimum possible number of keys to keep.  The actual
      // loaded slice must be at least this large (key_data_min_upper_bound).
      //
      usize min_to_load = min_item_count.value_or(1);
      if (last_item_i) {
        min_to_load = std::max<usize>(min_to_load, *last_item_i - first_item_i);
      }
      ++min_to_load;
      const usize key_data_min_upper_bound =
          items_page_slice.lower_bound + (this->loaded_items_[min_to_load].key_offset  //
                                          + sizeof(PackedKeyValue) * min_to_load);

      // Find the end of the shard containing the start of key data.  If possible, we would like to
      // limit the data read to this boundary; however, the key_data_min_upper_bound must take
      // precedence.
      //
      const usize key_data_shard_boundary =
          Self::round_up_to_shard_offset(unaligned_key_data_page_slice.lower_bound + 1);

      const usize key_data_max_upper_bound =
          std::max<usize>(key_data_min_upper_bound, key_data_shard_boundary);

      // Now we are ready to set the final key data slice, in bytes offset from the start of the
      // page.
      //
      this->key_data_page_slice_.lower_bound =  //
          unaligned_key_data_page_slice.lower_bound;

      this->key_data_page_slice_.upper_bound =  //
          std::clamp<usize>(unaligned_key_data_page_slice.upper_bound,
                            /*min=*/key_data_min_upper_bound,
                            /*max=*/key_data_max_upper_bound);
    }

    // Load key data.
    //
    StatusOr<ConstBuffer> key_data_buffer =
        this->slice_reader_.read_slice(this->key_data_page_slice_,
                                       this->slice_storage_,
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kLeafKeyDataShardLruPriority});
    if (!key_data_buffer.ok()) {
      return key_data_buffer.status();
    }

    this->key_data_start_ = key_data_buffer->data();

    // Calculate and store the difference between where the first item *says* its data is
    // (nominal_offset) and the *actual* address where that data is known to be loaded
    // (loaded_offset).
    //
    const isize nominal_offset = this->loaded_items_.front().key_offset;
    const isize loaded_offset = byte_distance(std::addressof(this->loaded_items_.front()),  //
                                              key_data_buffer->data());

    this->item_to_key_data_offset_ = loaded_offset - nominal_offset;

    return OkStatus();
  }

  /** \brief Loads the minimal item range immediately following the current loaded range, skipping
   * any unread items if present.
   */
  Status load_next_item_range() noexcept
  {
    BATT_CHECK(this->item_range_);
    return this->load_item_range(this->item_range_->upper_bound, /*last_item_i=*/None);
  }

  /** \brief Loads the item range starting at `key` (or the next key present).
   */
  Status seek_to(const KeyView& key) noexcept
  {
    const Interval<usize> search_range = this->header_->calculate_search_range(key);

    BATT_REQUIRE_OK(this->load_item_range(search_range.lower_bound,
                                          /*last_item_i=*/None,
                                          /*min_item_count=*/search_range.size()));

    const usize loaded_size = this->loaded_items_.size();

    // Search for the given key within the key range we loaded.
    //
    const PackedKeyValue* items_begin = this->loaded_items_.begin();
    const PackedKeyValue* items_end =
        items_begin + std::min<usize>(loaded_size, search_range.size());

    const PackedKeyValue* found_item =
        std::lower_bound(items_begin,
                         items_end,
                         key,
                         ShiftedPackedKeyOrder{this->item_to_key_data_offset_});

    BATT_CHECK_GE(found_item, items_begin);

    const usize n_to_skip = found_item - items_begin;
    this->drop_front(n_to_skip);

    if (this->item_range_->empty()) {
      return this->load_next_item_range();
    }

    return OkStatus();
  }

  /** \brief Returns true iff there are no more items in the currently loaded range.
   *
   * If this function returns true, then this->load_next_item_range() may be called to load another
   * batch of key data.  If the current range is empty *and* there is no next range (i.e. the end of
   * the last range exhausted the key range of the leaf), then this->load_next_item_range() will
   * return `batt::StatusCode::kEndOfStream`.
   */
  bool item_range_empty() const noexcept
  {
    return this->loaded_items_.empty();
  }

  /** \brief Returns the current key, if this->item_range_empty() is false; otherwise behavior is
   * undefined.
   */
  KeyView front_key() const noexcept
  {
    return KeyView{static_cast<const char*>(this->key_data_start_),
                   this->loaded_items_.front().key_size()};
  }

  /** \brief Loads and returns the value associated with the current key.
   *
   * If this->item_range_empty() is true, behavior is undefined.
   */
  StatusOr<ValueView> front_value() noexcept
  {
    // Calculate the location within the page containing the value data.
    //
    const PackedKeyValue& key0 = this->loaded_items_[0];
    const PackedKeyValue& key1 = this->loaded_items_[1];

    const usize key0_size = key0.key_size();
    const usize key1_size = key1.key_size();

    const usize value0_offset_offset = this->key_data_page_slice_.lower_bound + key0_size;
    const usize value1_offset_offset = value0_offset_offset + sizeof(PackedValueOffset) + key1_size;

    const PackedValueOffset& value0_offset =
        key0.shifted_value_offset(this->item_to_key_data_offset_);

    const PackedValueOffset& value1_offset =
        key1.shifted_value_offset(this->item_to_key_data_offset_);

    Interval<usize> value_data_page_slice{
        .lower_bound = value0_offset_offset + value0_offset.int_value,
        .upper_bound = value1_offset_offset + value1_offset.int_value,
    };

    // Load the value and return.
    //
    BATT_ASSIGN_OK_RESULT(
        ConstBuffer value_data_buffer,
        this->slice_reader_.read_slice(value_data_page_slice,
                                       this->slice_storage_,
                                       this->pin_pages_to_job_,
                                       llfs::LruPriority{kLeafValueDataShardLruPriority}));

    return unpack_value_view(value_data_buffer);
  }

  /** \brief Drops one (or more) items from the front of the current range.
   *
   * External callers should *not* override the default count of 1, since they have no way in
   * general of knowing how many items are in the current range.  (count > 1 is used internally, by
   * this->seek_to)
   */
  void drop_front(usize count = 1) noexcept
  {
    const PackedKeyValue& key0 = this->loaded_items_[0];
    const PackedKeyValue& key1 = this->loaded_items_[count];

    const isize key_data_delta =
        (key1.key_offset + sizeof(PackedKeyValue) * count) - key0.key_offset;

    this->key_data_page_slice_.lower_bound += key_data_delta;
    this->key_data_start_ = advance_pointer(this->key_data_start_, key_data_delta);

    this->item_range_->lower_bound += count;
    this->loaded_items_.drop_front(count);

    // Check that we still have at least two keys' worth of data (we need both to find the correct
    // size of the value of the front key).
    //
    const usize new_key_data_min_size =
        this->loaded_items_.empty()
            ? usize{0}
            : (this->loaded_items_[0].key_size() + sizeof(PackedValueOffset) +  //
               this->loaded_items_[1].key_size() + sizeof(PackedValueOffset));

    if (this->loaded_items_.empty() ||
        (this->key_data_page_slice_.lower_bound + new_key_data_min_size) >
            this->key_data_page_slice_.upper_bound) {
      this->key_data_page_slice_.lower_bound = this->key_data_page_slice_.upper_bound;
      this->loaded_items_ = as_slice(this->loaded_items_.begin(), 0);
      this->item_range_->upper_bound = this->item_range_->lower_bound;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void sanity_check_trie_index() const noexcept
  {
    // There should be a Trie index, and it should have fit inside the head_buffer; sanity
    // check these assertions.
    //
    const void* trie_begin = this->header_->trie_index.get();
    const void* trie_end = advance_pointer(trie_begin, this->header_->trie_index_size);
    const isize trie_end_offset = byte_distance(this->page_start_, trie_end);
    BATT_CHECK_GT(trie_end_offset, 0);
    BATT_CHECK_LE(trie_end_offset, static_cast<isize>(this->head_shard_size_));
  }

  void set_item_range(usize first_item_i,
                      Optional<usize> last_item_i,
                      Optional<usize> min_item_count) noexcept
  {
    const usize items_page_offset =
        BATT_CHECKED_CAST(usize, byte_distance(this->page_start_, this->header_->items->data()));

    this->item_range_.emplace();
    this->item_range_->lower_bound = first_item_i;
    if (last_item_i) {
      this->item_range_->upper_bound = *last_item_i;
    } else {
      this->item_range_->upper_bound =
          std::max<usize>(this->infer_last_item_index(first_item_i, items_page_offset),
                          first_item_i + min_item_count.value_or(0));
    }
    if (this->item_range_->lower_bound < this->header_->key_count) {
      BATT_CHECK_GT(this->item_range_->upper_bound, this->item_range_->lower_bound);
      BATT_CHECK_GE(this->item_range_->upper_bound,
                    this->item_range_->lower_bound + min_item_count.value_or(0));
    }
  }

  /** \brief Chooses and returns a last item index (from the start of the this->header_->items
   * array) so as to minimize the amount of I/O and copying for key data, while maximizing the size
   * of the resulting item range.
   */
  usize infer_last_item_index(usize first_item_i, usize items_page_offset) const noexcept
  {
    // It costs the same to load up to the end of the shard containing the first item, so figure
    // out where the next shard boundary is and which item index that corresponds to.
    //
    const usize first_item_offset = items_page_offset + (first_item_i * sizeof(PackedKeyValue));
    const usize end_of_shard_offset = Self::round_up_to_shard_offset(first_item_offset + 1);
    usize end_of_shard_item_i = (end_of_shard_offset - items_page_offset) / sizeof(PackedKeyValue);
    if (end_of_shard_item_i > 2) {
      end_of_shard_item_i -= 2;
    } else {
      end_of_shard_item_i = 0;
    }

    return std::clamp<usize>(end_of_shard_item_i,
                             /*min=*/first_item_i + 1,
                             /*max=*/this->header_->key_count);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  PageSliceReader slice_reader_;
  PageSliceStorage slice_storage_;
  llfs::PinPageToJob pin_pages_to_job_ = llfs::PinPageToJob::kDefault;
  const llfs::PageSize head_shard_size_;
  const void* page_start_ = nullptr;
  const PackedLeafPage* header_ = nullptr;
  Optional<Interval<usize>> item_range_;
  Interval<usize> key_data_page_slice_{0, 0};
  const void* key_data_start_ = nullptr;
  isize item_to_key_data_offset_ = 0;
  Slice<const PackedKeyValue> loaded_items_{nullptr, nullptr};
};

}  // namespace turtle_kv
