#pragma once

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/page_cache.hpp>
#include <llfs/page_reader.hpp>
#include <llfs/page_view.hpp>

#include <vqf/vqf_filter.h>

#include <xxhash.h>

#include <memory>

namespace turtle_kv {

inline constexpr u64 kVqfHashSeed = 0x9d0924dc03e79a75ull;

inline u64 vqf_hash_val(const KeyView& key)
{
  return XXH64(key.data(), key.size(), kVqfHashSeed);
}

struct PackedVqfFilter {
  little_u64 hash_seed;
  little_u64 hash_mask;
  vqf_metadata metadata;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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
};

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

  std::shared_ptr<llfs::PageFilter> build_filter() const override
  {
    return std::make_shared<llfs::NullPageFilter>(this->page_id());
  }

  void dump_to_ostream(std::ostream& out) const override
  {
    out << "VqfFilter";
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool is_present(u64 hash_val) const
  {
    if ((hash_val & this->packed_filter_->hash_mask.value()) != hash_val) {
      return true;
    }

    if (this->packed_filter_->metadata.key_remainder_bits == 8) {
      return vqf_is_present(this->packed_filter_->get_impl<8>(), hash_val);
    } else {
      BATT_CHECK_EQ(this->packed_filter_->metadata.key_remainder_bits, 16);
      return vqf_is_present(this->packed_filter_->get_impl<16>(), hash_val);
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  const PackedVqfFilter* packed_filter_;
};

}  // namespace turtle_kv
