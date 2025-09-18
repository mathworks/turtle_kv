#pragma once

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/util/page_buffers.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <batteries/static_assert.hpp>

#include <vqf/vqf_filter.h>

#include <xxhash.h>

#include <algorithm>
#include <memory>

namespace turtle_kv {

inline constexpr u64 kVqfHashSeed = 0x9d0924dc03e79a75ull;
inline constexpr usize kMinQuotientFilterBitsPerKey = 12;
inline constexpr double kMaxQuotientFilterLoadFactor = 0.85;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline u64 vqf_hash_val(const KeyView& key)
{
  return XXH64(key.data(), key.size(), kVqfHashSeed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <int TAG_BITS>
inline double vqf_filter_load_factor(usize bits_per_key_int)
{
  if (bits_per_key_int == 0) {
    return 0;
  }

  BATT_CHECK_GE(bits_per_key_int, kMinQuotientFilterBitsPerKey);

  const double bits_per_key = bits_per_key_int;

  if (TAG_BITS == 8) {
    return 10.2 / bits_per_key;
  }
  if (TAG_BITS == 16) {
    return 18.0 / bits_per_key;
  }

  BATT_PANIC() << "TAG_BITS must be 8 or 16";
  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVqfFilter {
  using Self = PackedVqfFilter;

  static constexpr u64 kMagic = 0x16015305e0f43a7dull;

  template <typename T>
  static const PackedVqfFilter& view_of(T&& t) noexcept
  {
    const ConstBuffer buffer = get_page_const_payload(t);
    BATT_CHECK_GE(buffer.size(), sizeof(PackedVqfFilter));

    return *static_cast<const PackedVqfFilter*>(buffer.data());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  little_u64 magic;
  llfs::PackedPageId src_page_id;
  little_u64 hash_seed;
  little_u64 hash_mask;
  vqf_metadata metadata;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void initialize(llfs::PageId src_page_id, u64 mask) noexcept
  {
    std::memset(this, 0, sizeof(PackedVqfFilter));
    this->magic = Self::kMagic;
    this->hash_seed = kVqfHashSeed;
    this->hash_mask = mask;
    this->src_page_id = llfs::PackedPageId::from(src_page_id);
  }

  void check_magic() const noexcept
  {
    BATT_CHECK_EQ(this->magic, Self::kMagic);
  }

  template <int TAG_BITS>
  vqf_filter<TAG_BITS>* get_impl()
  {
    return reinterpret_cast<vqf_filter<TAG_BITS>*>(std::addressof(this->metadata));
  }

  template <int TAG_BITS>
  const vqf_filter<TAG_BITS>* get_impl() const
  {
    return reinterpret_cast<const vqf_filter<TAG_BITS>*>(std::addressof(this->metadata));
  }

  bool is_present(u64 hash_val) const
  {
    if ((hash_val & this->hash_mask.value()) != hash_val) {
      return true;
    }

    if (this->metadata.key_remainder_bits == 8) {
      return vqf_is_present(this->get_impl<8>(), hash_val);
    }

    BATT_CHECK_EQ(this->metadata.key_remainder_bits, 16);
    return vqf_is_present(this->get_impl<16>(), hash_val);
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVqfFilter), 32 + sizeof(vqf_metadata));

BATT_STATIC_ASSERT_EQ(offsetof(vqf_filter<8>, metadata), 0);
BATT_STATIC_ASSERT_EQ(offsetof(vqf_filter<16>, metadata), 0);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VqfFilterPageView : public llfs::PageView
{
 public:
  /** \brief The page layout id for all instances of this class.
   */
  static llfs::PageLayoutId page_layout_id()
  {
    static const llfs::PageLayoutId id = llfs::PageLayoutId::from_str("vqf_filt");
    return id;
  }

  /** \brief Returns the PageReader for this layout.
   */
  static llfs::PageReader page_reader()
  {
    return [](std::shared_ptr<const llfs::PageBuffer> page_buffer)
               -> batt::StatusOr<std::shared_ptr<const llfs::PageView>> {
      return {std::make_shared<VqfFilterPageView>(std::move(page_buffer))};
    };
  }

  /** \brief Registers this page layout with the passed cache, so that pages using the layout can be
   * correctly loaded and parsed by the PageCache.
   */
  static batt::Status register_layout(llfs::PageCache& cache)
  {
    return cache.register_page_reader(VqfFilterPageView::page_layout_id(),
                                      __FILE__,
                                      __LINE__,
                                      VqfFilterPageView::page_reader());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit VqfFilterPageView(std::shared_ptr<const llfs::PageBuffer>&& page_buffer) noexcept
      : llfs::PageView{std::move(page_buffer)}
      , packed_filter_{static_cast<const PackedVqfFilter*>(this->const_payload().data())}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::PageLayoutId get_page_layout_id() const override
  {
    return VqfFilterPageView::page_layout_id();
  }

  batt::BoxedSeq<llfs::PageId> trace_refs() const override
  {
    return batt::seq::Empty<llfs::PageId>{} | batt::seq::boxed();
  }

  batt::Optional<llfs::KeyView> min_key() const override
  {
    return batt::None;
  }

  batt::Optional<llfs::KeyView> max_key() const override
  {
    return batt::None;
  }

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "VqfFilter";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool is_present(u64 hash_val) const
  {
    return this->packed_filter_->is_present(hash_val);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedVqfFilter* packed_filter_;
};

}  // namespace turtle_kv
