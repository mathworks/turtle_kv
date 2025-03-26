#pragma once

#include <turtle_kv/core/algo/decay_to_item.hpp>
#include <turtle_kv/core/merge_compactor.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>

#include <llfs/stable_string_store.hpp>

#include <atomic>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace turtle_kv {
namespace testing {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <usize kMaxSizeParam>
class MinMaxSize
{
 public:
  using Self = MinMaxSize;

  static constexpr usize kMaxSize = kMaxSizeParam;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MinMaxSize(usize min_n, usize max_n) noexcept : min_size_{min_n}, max_size_{max_n}
  {
  }

  Self& set_min_size(usize n)
  {
    this->min_size_ = n;
    this->max_size_ = std::max(this->max_size_, n);
    return *this;
  }

  Self& set_max_size(usize n)
  {
    BATT_CHECK_GE(n, this->min_size_);
    BATT_CHECK_LT(n, kMaxSize);
    this->max_size_ = n;

    return *this;
  }

  Self set_size(usize n)
  {
    return this->set_min_size(n).set_max_size(n);
  }

  template <typename Rng>
  usize pick_size(Rng& rng) const
  {
    return std::uniform_int_distribution<usize>{this->min_size_, this->max_size_}(rng);
  }

 protected:
  usize min_size_;
  usize max_size_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class RandomStringGenerator : public MinMaxSize<256>
{
 public:
  using Super = MinMaxSize<256>;
  using Self = RandomStringGenerator;

  RandomStringGenerator() noexcept : Super{24, 24}
  {
  }

  template <typename Rng>
  std::string operator()(Rng& rng)
  {
    return this->generate_impl(rng, [](char* data, usize size) -> std::string {
      return std::string(data, size);
    });
  }

  template <typename Rng>
  std::string_view operator()(Rng& rng, llfs::StableStringStore& store)
  {
    return this->generate_impl(rng, [&store](char* data, usize size) -> std::string_view {
      return store.store(std::string_view{data, size});
    });
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  template <typename Rng, typename Fn /* T (char* data, usize size) */>
  decltype(auto) generate_impl(Rng& rng, Fn&& fn)
  {
    std::array<char, kMaxSize> buffer;

    const usize n = this->Super::pick_size(rng);
    BATT_CHECK_GT(n, 4);
    BATT_CHECK_LT(n, kMaxSize);

    std::memcpy(buffer.data(), "user", 4);

    for (usize i = 4; i < n; ++i) {
      buffer[i] = this->pick_char_(rng);
    }

    return fn(buffer.data(), n);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::uniform_int_distribution<char> pick_char_{'0', '9'};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class RandomResultSetGenerator : public MinMaxSize<usize{1} << 24>
{
 public:
  using Super = MinMaxSize<usize{1} << 24>;
  using Self = RandomResultSetGenerator;

  RandomResultSetGenerator() noexcept : Super{4096, 4096}
  {
  }

  RandomStringGenerator& key_generator()
  {
    return this->key_generator_;
  }

  Self& set_key_size(usize n)
  {
    this->key_generator_.set_size(n);
    return *this;
  }

  Self& set_value_size(usize n)
  {
    this->value_size_ = n;
    return *this;
  }

  template <bool kDecayToItems, typename Rng>
  MergeCompactor::ResultSet</*kDecayToItems=*/kDecayToItems>
  operator()(DecayToItem<kDecayToItems>, Rng& rng, llfs::StableStringStore& store)
  {
    using ResultSet = MergeCompactor::ResultSet</*kDecayToItems=*/kDecayToItems>;
    using Item = typename ResultSet::value_type;

    const usize n = this->Super::pick_size(rng);
    std::vector<EditView> items;

    while (items.size() < n) {
      for (usize i = items.size(); i < n; ++i) {
        char ch = '_' + (i & 31);
        items.emplace_back(this->key_generator_(rng, store),
                           ValueView::from_str(store.store(std::string(this->value_size_, ch))));
      }
      std::sort(items.begin(), items.end(), KeyOrder{});
      items.erase(std::unique(items.begin(),
                              items.end(),
                              [](const auto& l, const auto& r) {
                                return get_key(l) == get_key(r);
                              }),
                  items.end());
    }

    ResultSet result;
    const Item* first_item = (const Item*)items.data();
    result.append(std::move(items), as_slice(first_item, n));

    return result;
  }

 private:
  RandomStringGenerator key_generator_;
  usize value_size_ = 100;
};

}  // namespace testing
}  // namespace turtle_kv
