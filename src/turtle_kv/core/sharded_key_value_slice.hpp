#pragma once

#include <turtle_kv/config.hpp>

#include <turtle_kv/core/packed_key_value.hpp>

#include <turtle_kv/import/slice.hpp>

#include <turtle_kv/util/page_slice_reader.hpp>

#include <llfs/page_id.hpp>
#include <llfs/page_loader.hpp>

namespace turtle_kv {

/** \brief A representation of a Slice<const PackedKeyValue> where the elements have been
 * shifted/sharded.
 */
class ShardedKeyValueSlice
{
 public:
  static constexpr u64 kDefaultLeafShardedViewSize = 4096;

  ShardedKeyValueSlice()
      : page_loader_{nullptr}
      , slice_storage_{nullptr}
      , kv_range_{}
      , key_data_offset_{0}
      , value_data_lower_bound_{0}
      , leaf_page_id_{llfs::PageId{}}
  {
  }

  explicit ShardedKeyValueSlice(const PackedKeyValue* begin, const PackedKeyValue* end) noexcept
      : page_loader_{nullptr}
      , slice_storage_{nullptr}
      , kv_range_{begin, end}
      , key_data_offset_{0}
      , value_data_lower_bound_{0}
      , leaf_page_id_{llfs::PageId{}}
  {
  }

  explicit ShardedKeyValueSlice(const PackedKeyValue* begin,
                                const PackedKeyValue* end,
                                isize key_offset,
                                usize value_lower_bound,
                                llfs::PageId leaf_page_id,
                                llfs::PageLoader* loader,
                                PageSliceStorage* storage) noexcept
      : page_loader_{loader}
      , slice_storage_{storage}
      , kv_range_{begin, end}
      , key_data_offset_{key_offset}
      , value_data_lower_bound_{value_lower_bound}
      , leaf_page_id_{leaf_page_id}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  inline KeyView front_key() const
  {
    BATT_CHECK_NE(this->kv_range_.size(), 0);
    return this->kv_range_.front().shifted_key_view(this->key_data_offset_);
  }

  inline ValueView front_value() const
  {
    BATT_CHECK_NE(this->kv_range_.size(), 0);

    const PackedKeyValue& pkv = this->kv_range_.front();

    // Check if the backing slice is non-sharded. If it non-sharded, no need to load value data.
    //
    if (!this->page_loader_) {
      return unpack_value_view(pkv.value_data(), pkv.value_size());
    }

    // Sanity checks to assert that all the necessary member variables used to load value data are
    // set.
    //
    BATT_CHECK_NE(this->value_data_lower_bound_, 0);
    BATT_CHECK_NE(this->slice_storage_, nullptr);

    usize value_size = pkv.shifted_value_size(this->key_data_offset_);

    Interval<usize> value_data_slice{this->value_data_lower_bound_,
                                     value_data_lower_bound_ + value_size};

    BATT_CHECK(this->leaf_page_id_);
    PageSliceReader slice_reader{*this->page_loader_,
                                 this->leaf_page_id_,
                                 llfs::PageSize{kDefaultLeafShardedViewSize}};

    // Load the value data!
    //
    StatusOr<ConstBuffer> value_data_buffer =
        slice_reader.read_slice(value_data_slice,
                                *this->slice_storage_,
                                llfs::PinPageToJob::kFalse,
                                llfs::LruPriority{kLeafValueDataShardLruPriority});

    BATT_CHECK(value_data_buffer.ok());

    // Calculate the distance between the key data shard and the newly loaded value data shard. We
    // use this to calculate exactly where the value data starts.
    //
    const isize value_offset_base = pkv.shifted_value_offset(this->key_data_offset_).int_value;
    const isize value_offset_target =
        byte_distance(&(pkv.shifted_value_offset(this->key_data_offset_)),
                      value_data_buffer->data());
    const isize value_offset_delta = value_offset_target - value_offset_base;

    return unpack_value_view(pkv.shifted_value_data(this->key_data_offset_) + value_offset_delta,
                             value_size);
  }

  inline void drop_front()
  {
    // Before we call drop_front on the slice, update this->value_data_lower_bound_ to reflect the
    // page offset for the new front value's data start. We only need to do this if the slice is in
    // a sharded state and if drop_front won't make the slice empty.
    //
    if (this->page_loader_ && this->kv_range_.size() > 1) {
      const PackedKeyValue& pkv = this->kv_range_.front();
      this->value_data_lower_bound_ += pkv.shifted_value_size(this->key_data_offset_);
    }

    this->kv_range_.drop_front();
  }

  inline bool empty() const
  {
    return this->kv_range_.empty();
  }

 private:
  /** \brief Used for loading values. Set to nullptr if the data is not sharded. Users of this class
   * MUST guarantee that the object that this pointer points to outlives this ShardedKeyValueObject.
   */
  llfs::PageLoader* page_loader_;

  /** \brief Used for storing page slices read when loading values. Set to nullptr if the data is
   * not sharded. Users of this class MUST guarantee that the object that this pointer points to
   * outlives this ShardedKeyValueObject.
   */
  PageSliceStorage* slice_storage_;

  /** \brief The backing container for this object. If this->key_data_offset_ AND
   * this->value_data_offset_ are both 0, this represents a regular slice with no shift.
   */
  Slice<const PackedKeyValue> kv_range_;

  /** \brief The signed difference indicating the distance (bytes) between the key offsets/pointers
   * shard and key data shard.
   */
  isize key_data_offset_;

  /** \brief Represents the absolute byte offset (from the start of the leaf page that this data
   * comes from) of the start of the front value's (kv_range_.front()) data.
   */
  usize value_data_lower_bound_;

  /** \brief The leaf page from which the key-value data comes from.
   */
  llfs::PageId leaf_page_id_;
};
}  // namespace turtle_kv
