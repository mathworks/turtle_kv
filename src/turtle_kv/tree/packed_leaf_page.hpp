#pragma once

#include <turtle_kv/tree/tree_serialize_context.hpp>

#include <turtle_kv/core/edit_slice.hpp>
#include <turtle_kv/core/key_view.hpp>
#include <turtle_kv/core/packed_key_value.hpp>

#include <turtle_kv/util/page_buffers.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/interval.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/seq.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/trie.hpp>

#include <batteries/assert.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/compare.hpp>

#include <algorithm>
#include <tuple>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafPage {
  static constexpr u64 kMagic = 0x14965f812f8a16c3ull;

  struct Metrics {
    LatencyMetric find_key_latency;
    FastCountMetric<u64> find_key_success_count;
    FastCountMetric<u64> find_key_failure_count;
  };

  static Metrics& metrics()
  {
    static Metrics metrics_;
    return metrics_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u64 magic;                                                 // +8 = 8
  u32 key_count;                                                 // +4 = 12
  u32 index_step;                                                // +4 = 16
  u32 trie_index_size;                                           // +4 = 20
  u32 total_packed_size;                                         // +4 = 24
  llfs::PackedPointer<llfs::PackedArray<PackedKeyValue>> items;  // +4 = 28
  llfs::PackedPointer<const llfs::PackedBPTrie> trie_index;      // +4 = 32

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  static const PackedLeafPage& view_of(T&& t)
  {
    const ConstBuffer buffer = get_page_const_payload(BATT_FORWARD(t));
    BATT_ASSERT_GE(buffer.size(), sizeof(PackedLeafPage));

    const PackedLeafPage& packed_leaf_page = *static_cast<const PackedLeafPage*>(buffer.data());

    return packed_leaf_page;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void check_invariants(Optional<usize> payload_size) const
  {
    BATT_CHECK_EQ(this->magic, PackedLeafPage::kMagic);
    BATT_CHECK(this->items);
    if (payload_size) {
      BATT_CHECK_GE(*payload_size, this->total_packed_size);
    }
    BATT_CHECK_EQ(this->key_count, this->items->size() - 2);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  KeyView key_at(usize i) const
  {
    return (*this->items)[i].key_view();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  ValueView value_at(usize i) const
  {
    return (*this->items)[i].value_view();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue& front_item() const
  {
    return this->items->front();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue& back_item() const
  {
    return (*this->items)[this->key_count - 1];
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* items_begin() const
  {
    return this->items->data();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* items_end() const
  {
    return this->items->data() + this->key_count;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Slice<const PackedKeyValue> items_slice() const
  {
    return Slice<const PackedKeyValue>{this->items_begin(), this->items_end()};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  BoxedSeq<EditSlice> as_edit_slice_seq() const
  {
    return seq::single_item(EditSlice{this->items_slice()}) | seq::boxed();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  KeyView min_key() const
  {
    return get_key(this->front_item());
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  KeyView max_key() const
  {
    return get_key(this->back_item());
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  CInterval<KeyView> get_key_crange() const
  {
    return CInterval<KeyView>{this->min_key(), this->max_key()};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  Interval<usize> calculate_search_range(const std::string_view& key, usize& key_prefix_match) const
  {
    if (!this->trie_index) {
      return Interval<usize>{0, this->key_count};
    }

    Interval<usize> search_range = this->trie_index->find(key, key_prefix_match);

    const usize max_i = this->key_count - 1;

    search_range.lower_bound = std::min(search_range.lower_bound * this->index_step,  //
                                        max_i);

    search_range.upper_bound = std::min((search_range.upper_bound + 1) * this->index_step,  //
                                        max_i + 1);

    return search_range;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* find_key_in_range(const std::string_view& key,
                                          Interval<usize> search_range,
                                          usize skip_n = 0) const
  {
    auto [first, last] = [&] {
      if (skip_n) {
        return std::equal_range(this->items->data() + search_range.lower_bound,  //
                                this->items->data() + search_range.upper_bound,  //
                                key,
                                KeySuffixOrder{.skip_n = skip_n});
      }
      return std::equal_range(this->items->data() + search_range.lower_bound,  //
                              this->items->data() + search_range.upper_bound,  //
                              key,
                              [](const auto& l, const auto& r) {
                                return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
                              });
    }();

    if (first == last) {
      return nullptr;
    }
    return std::addressof(*first);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* find_key(const std::string_view& key) const
  {
    LatencyTimer timer{Every2ToTheConst<16>{}, PackedLeafPage::metrics().find_key_latency};

    usize key_prefix_match = 0;
    Interval<usize> search_range = this->calculate_search_range(key, key_prefix_match);

    const PackedKeyValue* found = this->find_key_in_range(key, search_range);

    if (found != nullptr) {
      PackedLeafPage::metrics().find_key_success_count.add(1);
    } else {
      PackedLeafPage::metrics().find_key_failure_count.add(1);
    }

    return found;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* lower_bound_in_range(const std::string_view& key,
                                             Interval<usize> search_range,
                                             usize skip_n = 0) const
  {
    auto first = [&] {
      if (skip_n) {
        return std::lower_bound(this->items->data() + search_range.lower_bound,  //
                                this->items->data() + search_range.upper_bound,  //
                                key,
                                KeySuffixOrder{.skip_n = skip_n});
      }
      return std::lower_bound(this->items->data() + search_range.lower_bound,  //
                              this->items->data() + search_range.upper_bound,  //
                              key,
                              [](const auto& l, const auto& r) {
                                return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
                              });
    }();

    auto last = this->items->data() + this->items->size();

    if (first == last) {
      return nullptr;
    }
    return std::addressof(*first);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  const PackedKeyValue* lower_bound(const std::string_view& key) const
  {
    usize key_prefix_match = 0;
    Interval<usize> search_range = this->calculate_search_range(key, key_prefix_match);

    return this->lower_bound_in_range(key, search_range);
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedLeafPage), 32);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline usize leaf_max_space_from_size(usize leaf_size)
{
  return leaf_size - (sizeof(llfs::PackedPageHeader) + sizeof(PackedLeafPage) +
                      sizeof(llfs::PackedArray<PackedKeyValue>));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafLayoutPlan {
  using Self = PackedLeafLayoutPlan;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize page_size;
  usize key_count;
  bool use_trie_index;

  usize trie_index_begin;
  usize trie_index_end;

  usize leaf_header_begin;
  usize leaf_header_end;

  usize key_array_header_begin;
  usize key_array_header_end;

  usize key_headers_begin;
  usize key_headers_end;

  usize key_data_begin;
  usize key_data_end;

  usize final_value_offset_begin;
  usize final_value_offset_end;

  usize value_data_begin;
  usize value_data_end;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename ItemsRangeT>
  static Self from_items(usize page_size, const ItemsRangeT& items, bool use_trie_index);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  T* place(const MutableBuffer& buffer, usize offset) const
  {
    return const_cast<T*>(static_cast<const T*>(advance_pointer(buffer.data(), offset)));
  }

  bool is_valid() const
  {
    return this->value_data_end <= this->page_size;
  }

  void check_valid(std::string_view label) const;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_OBJECT_PRINT_IMPL((inline),
                       PackedLeafLayoutPlan,
                       (page_size,
                        key_count,
                        use_trie_index,
                        trie_index_begin,
                        trie_index_end,
                        leaf_header_begin,
                        leaf_header_end,
                        key_array_header_begin,
                        key_array_header_end,
                        key_headers_begin,
                        key_headers_end,
                        key_data_begin,
                        key_data_end,
                        final_value_offset_begin,
                        final_value_offset_end,
                        value_data_begin,
                        value_data_end))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline void PackedLeafLayoutPlan::check_valid(std::string_view label) const
{
  BATT_CHECK(this->is_valid()) << *this << BATT_INSPECT_STR(label);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackedLeafLayoutPlanBuilder
{
 public:
  using Self = PackedLeafLayoutPlanBuilder;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize page_size = 0;
  usize key_count = 0;
  usize key_data_size = 0;
  usize value_data_size = 0;
  bool use_trie_index = false;

  Self& add(const std::string_view& key, const ValueView& value)
  {
    this->key_count += 1;
    this->key_data_size += key.size() + sizeof(PackedValueOffset);
    this->value_data_size += 1 + value.size();

    return *this;
  }

  PackedLeafLayoutPlan build(bool check = true) const
  {
    PackedLeafLayoutPlan plan;

    plan.page_size = this->page_size;
    plan.key_count = BATT_CHECKED_CAST(u32, this->key_count);
    plan.use_trie_index = this->use_trie_index;

    usize offset = 0;
    const auto append = [&offset](usize size) {
      usize begin = offset;
      offset += size;
      usize end = offset;
      return std::make_tuple(begin, end);
    };

    append(sizeof(llfs::PackedPageHeader));

    std::tie(plan.leaf_header_begin,  //
             plan.leaf_header_end) =  //
        append(sizeof(PackedLeafPage));

    std::tie(plan.key_array_header_begin,  //
             plan.key_array_header_end) =  //
        append(sizeof(llfs::PackedArray<PackedKeyValue>));

    std::tie(plan.key_headers_begin,  //
             plan.key_headers_end) =  //
        append(sizeof(PackedKeyValue) * (this->key_count + 2));

    std::tie(plan.key_data_begin,  //
             plan.key_data_end) =  //
        append(this->key_data_size);

    std::tie(plan.final_value_offset_begin,  //
             plan.final_value_offset_end) =  //
        append(sizeof(PackedValueOffset));

    std::tie(plan.value_data_begin,  //
             plan.value_data_end) =  //
        append(this->value_data_size);

    if (check) {
      plan.check_valid("first");
    }

    if (plan.use_trie_index) {
      const usize space_for_trie = batt::round_down_bits(6, this->page_size - plan.value_data_end);

      offset = plan.leaf_header_end;
      std::tie(plan.trie_index_begin,  //
               plan.trie_index_end) =  //
          append(space_for_trie);

      for (usize* fixup : {
               &plan.key_array_header_begin,
               &plan.key_array_header_end,
               &plan.key_headers_begin,
               &plan.key_headers_end,
               &plan.key_data_begin,
               &plan.key_data_end,
               &plan.final_value_offset_begin,
               &plan.final_value_offset_end,
               &plan.value_data_begin,
               &plan.value_data_end,
           }) {
        *fixup += space_for_trie;
      }

      if (check) {
        plan.check_valid("second");
      }
    }

    return plan;
  }
};

// #=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct LeafItemsSummary {
  usize drop_count = 0;
  usize key_count = 0;
  usize key_data_size = 0;
  usize value_data_size = 0;
};

struct AddLeafItemsSummary {
  LeafItemsSummary operator()(const LeafItemsSummary& prior, const EditView& edit) const noexcept
  {
    if (!decays_to_item(edit.value)) {
      return LeafItemsSummary{
          .drop_count = prior.drop_count + 1,
          .key_count = prior.key_count,
          .key_data_size = prior.key_data_size,
          .value_data_size = prior.value_data_size,
      };
    } else {
      return LeafItemsSummary{
          .drop_count = prior.drop_count,
          .key_count = prior.key_count + 1,
          .key_data_size = prior.key_data_size + (edit.key.size() + 4),
          .value_data_size = prior.value_data_size + (1 + edit.value.size()),
      };
    }
  }

  LeafItemsSummary operator()(const LeafItemsSummary& left,
                              const LeafItemsSummary& right) const noexcept
  {
    return LeafItemsSummary{
        .drop_count = left.drop_count + right.drop_count,
        .key_count = left.key_count + right.key_count,
        .key_data_size = left.key_data_size + right.key_data_size,
        .value_data_size = left.value_data_size + right.value_data_size,
    };
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT>
/*static*/ PackedLeafLayoutPlan PackedLeafLayoutPlan::from_items(usize page_size,
                                                                 const ItemsRangeT& items,
                                                                 bool use_trie_index)
{
  LeafItemsSummary summary = std::accumulate(std::begin(items),
                                             std::end(items),
                                             LeafItemsSummary{},
                                             AddLeafItemsSummary{});

  BATT_CHECK_EQ(summary.drop_count, 0);

  PackedLeafLayoutPlanBuilder plan_builder;

  plan_builder.page_size = page_size;
  plan_builder.key_count = summary.key_count;
  plan_builder.key_data_size = summary.key_data_size;
  plan_builder.value_data_size = summary.value_data_size;
  plan_builder.use_trie_index = use_trie_index;

  PackedLeafLayoutPlan plan = plan_builder.build();

  return plan;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct BufferBoundsChecker {
  MutableBuffer buffer;

  const void* buffer_begin() const
  {
    return this->buffer.data();
  }

  const void* buffer_end() const
  {
    return advance_pointer(this->buffer.data(), this->buffer.size());
  }

  template <typename T>
  bool contains(const T* ptr) const
  {
    return ((const void*)(ptr + 0) >= this->buffer_begin()) &&  //
           ((const void*)(ptr + 1) <= this->buffer_end());
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// NOTE: `buffer` is the *entire* page buffer, including 64-byte llfs::PackedPageHeader.
//
template <typename Items>
inline PackedLeafPage* build_leaf_page(MutableBuffer buffer,
                                       const PackedLeafLayoutPlan& plan,
                                       const Items& items)
{
  BATT_CHECK_EQ(plan.key_count, std::end(items) - std::begin(items));
  BATT_CHECK_LE(plan.value_data_end, buffer.size());

  auto* const p_header = plan.place<PackedLeafPage>(buffer, plan.leaf_header_begin);

  p_header->magic = PackedLeafPage::kMagic;
  p_header->key_count = plan.key_count;
  p_header->total_packed_size = plan.value_data_end - plan.leaf_header_begin;

  auto* const p_keys = plan.place<llfs::PackedArray<PackedKeyValue>>(buffer,  //
                                                                     plan.key_array_header_begin);

  p_keys->initialize(plan.key_count + 2);
  p_keys->initialize_size_in_bytes(plan.key_headers_end - plan.key_headers_begin);

  BufferBoundsChecker bounds_checker{buffer};

  p_header->items.reset(p_keys, &bounds_checker);

  // First pass - write all key headers and copy all key data.
  //
  {
    auto* p_key_header = plan.place<PackedKeyValue>(buffer, plan.key_headers_begin);
    char* p_key_data = plan.place<char>(buffer, plan.key_data_begin);
    char* const p_key_data_expected_end = plan.place<char>(buffer, plan.key_data_end);

    for (const auto& item : items) {
      const auto& key_view = get_key(item);

      // Set the key data offset.
      //
      p_key_header->set_key_data(p_key_data);

      // Copy the key data.
      //
      std::memcpy(p_key_data, key_view.data(), key_view.size());

      // Advance pointers.
      //
      ++p_key_header;
      p_key_data += key_view.size() + sizeof(PackedValueOffset);
    }
    BATT_CHECK_EQ((void*)p_key_data, (void*)p_key_data_expected_end);

    // Write a final headers at the end.
    //
    p_key_header[0].set_key_data(p_key_data);
    p_key_header[1].set_key_data(p_key_data + sizeof(PackedValueOffset));
  }

  // Second pass - write all value data and offsets.
  //
  {
    auto* p_key_header = plan.place<PackedKeyValue>(buffer, plan.key_headers_begin);
    char* p_value_data = plan.place<char>(buffer, plan.value_data_begin);

    for (const auto& item : items) {
      const auto& value_view = get_value(item);

      // Set the value data offset.
      //
      p_key_header->set_value_data(p_value_data);

      // Copy the value data.
      //
      *p_value_data = static_cast<u8>(value_view.op());
      std::memcpy(p_value_data + 1, value_view.data(), value_view.size());

      // Advance pointers.
      //
      ++p_key_header;
      p_value_data += 1 + value_view.size();
    }

    // Write a final value offset at the end.
    //
    p_key_header->set_value_data(p_value_data);
  }

  p_header->trie_index.offset = 0;
  p_header->index_step = 0;
  p_header->trie_index_size = 0;

  if (plan.use_trie_index) {
    const MutableBuffer trie_buffer{(void*)advance_pointer(buffer.data(), plan.trie_index_begin),
                                    plan.trie_index_end - plan.trie_index_begin};
    usize step_size = 16;
    batt::SmallVec<std::string_view, 1024> pivot_keys;
    for (;;) {
      BATT_CHECK_GT(step_size, 0);
      for (usize i = step_size; i < plan.key_count; i += step_size) {
        std::string_view k0 = p_header->key_at(i - 1);
        std::string_view k1 = p_header->key_at(i);
        std::string_view prefix = llfs::find_common_prefix(0, k0, k1);
        std::string_view pivot = std::string_view{k1.data(), prefix.size() + 1};

        pivot_keys.emplace_back(pivot);
      }
      llfs::BPTrie in_memory_trie{pivot_keys};

      const usize packed_trie_size = llfs::packed_sizeof(in_memory_trie);
      if (packed_trie_size > trie_buffer.size()) {
        step_size *= 2;
        if (step_size * 2 > plan.key_count) {
          break;
        }
      }

      llfs::DataPacker packer{trie_buffer};
      p_header->trie_index.reset(llfs::pack_object(in_memory_trie, &packer), &bounds_checker);
      p_header->index_step = BATT_CHECKED_CAST(u32, step_size);
      p_header->trie_index_size = BATT_CHECKED_CAST(u32, packed_trie_size);
      break;
    }
  }

  return p_header;
}

llfs::PageLayoutId packed_leaf_page_layout_id();

StatusOr<llfs::PinnedPage> pin_leaf_page_to_job(llfs::PageCacheJob& page_job,
                                                std::shared_ptr<llfs::PageBuffer>&& page_buffer);

template <typename ItemsRangeT>
auto build_leaf_page_in_job(llfs::PageBuffer& page_buffer, const ItemsRangeT& items)
{
  auto plan = PackedLeafLayoutPlan::from_items(page_buffer.size(),
                                               items,
                                               /*use_trie_index=*/true);

  PackedLeafPage* const packed_leaf_page =
      build_leaf_page(page_buffer.mutable_buffer(), plan, items);

  BATT_CHECK_NOT_NULLPTR(packed_leaf_page);

  return [](llfs::PageCacheJob& page_job, std::shared_ptr<llfs::PageBuffer>&& page_buffer) {
    return pin_leaf_page_to_job(page_job, std::move(page_buffer));
  };
}

}  // namespace turtle_kv
