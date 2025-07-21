#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>
#include <turtle_kv/util/seq_mutex.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/case_of.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/seq/loop_control.hpp>

#include <absl/synchronization/mutex.h>

#include <array>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

#include <emmintrin.h>  // SSE2
#include <mmintrin.h>   // MMX
#include <pmmintrin.h>  // SSE3

#ifdef __AVX512F__
#include <immintrin.h>  // AVX512 (AVX, AVX2, FMA)
#endif

namespace turtle_kv {

/** \brief Returns the index of `key_byte` in the array `keys`, if present; else returns one of: {4,
 * 5, 6, 7}.
 */
inline usize index_of(u8 key_byte, const std::array<u8, 4>& keys)
{
  __m64 pattern = _mm_set1_pi8((char)key_byte);
  u64 extended = *((const u32*)keys.data());
  __m64 values = _mm_cvtsi64_m64(extended);
  __m64 result = _m_pcmpeqb(pattern, values);

  return ((__builtin_ffsll((i64)result) - 1) >> 3) & 7;
}

/** \brief Returns the index of `key_byte` in the array `keys`, if present; else returns 31.
 */
inline usize index_of(u8 key_byte, const std::array<u8, 16>& keys)
{
  __m128i pattern = _mm_set1_epi8((char)key_byte);
  __m128i values = _mm_lddqu_si128((const __m128i*)keys.data());

#ifndef __AVX512F__
  int result = _mm_movemask_epi8(_mm_cmpeq_epi8(pattern, values));
#else
  __mmask16 result = _mm_cmpeq_epi8_mask(pattern, values);
#endif

  return (__builtin_ffs(result) - 1) & 31;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ARTBase
{
 public:
  static constexpr usize kMaxKeyLen = 64;

  /** \brief Tag type indicating that a new object should not be initialized by the ctor.
   */
  struct NoInit {
  };

  struct Metrics {
    CountMetric<u64> construct_count;
    CountMetric<u64> destruct_count;
    FastCountMetric<u64> insert_count;
    FastCountMetric<u64> byte_alloc_count;
    FastCountMetric<u64> byte_free_count;

    //----- --- -- -  -  -   -

    double bytes_per_instance() const
    {
      return (double)this->byte_alloc_count.get() / (double)this->construct_count.get();
    }

    double average_item_count() const
    {
      return (double)this->insert_count.get() / (double)this->construct_count.get();
    }

    double bytes_per_insert() const
    {
      return (double)this->byte_alloc_count.get() / (double)this->insert_count.get();
    }
  };

  static Metrics& metrics()
  {
    static Metrics m_;
    return m_;
  }

  enum struct Synchronized {
    kFalse = 0,
    kTrue = 1,
    kDynamic = 2,
  };

  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

  enum struct NodeType : u8 {
    kNode4 = 0,
    kNode16 = 1,
    kNode48 = 2,
    kNode256 = 3,
    kNodeBase = 4,
  };

  static constexpr usize sizeof_value(batt::StaticType<void>)
  {
    return 0;
  }

  template <typename ValueT>
  static constexpr usize sizeof_value(batt::StaticType<ValueT>)
  {
    return sizeof(ValueT);
  }

  template <typename FromNodeT, typename ToNodeT>
  static void* construct_value_copy(FromNodeT*, ToNodeT*, batt::StaticType<void>)
  {
    return nullptr;
  }

  template <typename FromNodeT, typename ToNodeT, typename ValueT>
  static ValueT* construct_value_copy(FromNodeT* from_node,
                                      ToNodeT* to_node,
                                      batt::StaticType<ValueT>)
  {
    return new (to_node + 1) ValueT{*reinterpret_cast<const ValueT*>(from_node + 1)};
  }

  template <typename NodeT>
  static void* uninitialized_value(NodeT* node)
  {
    return node + 1;
  }

  template <typename ValueT, typename NodeT>
  static ValueT* mutable_value(NodeT* node, batt::StaticType<ValueT> /**/ = {})
  {
    return reinterpret_cast<ValueT*>(node + 1);
  }

  template <typename ValueT, typename NodeT>
  static const ValueT* const_value(const NodeT* node, batt::StaticType<ValueT> /**/ = {})
  {
    return reinterpret_cast<const ValueT*>(node + 1);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Node class hierarchy:
  //
  //                                  ┌────────┐
  //                                  │NodeBase│
  //                                  └────────┘
  //                                       △
  //                       ┌───────────────┤
  //                       │               │
  //               ┌───────────────┐       │
  //               │GrowableNode<B>│       │
  //               └───────────────┘       │
  //                       △               │
  //             ┌─────────┴────────────┐  └────────┐
  //             │                      │           │
  // ┌──────────────────────┐┌────────────────────┐ │
  // │IndirectIndexedNode<B>││DirectIndexedNode<B>│ │
  // └──────────────────────┘└────────────────────┘ │
  //             △                      △           │
  //       ┌─────┴───────┐              │           │
  //       │             │              │           │
  //    ┌─────┐      ┌──────┐       ┌──────┐    ┌───────┐
  //    │Node4│      │Node16│       │Node48│    │Node256│
  //    └─────┘      └──────┘       └──────┘    └───────┘
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Node memory layout:
  //
  // ┌────────────┬──────────┬──────────────┬───────────────┐
  // │ key prefix │ NodeBase │  (impl) ...  │    ValueT     │
  // └────────────┴──────────┴──────────────┴───────────────┘
  //  ◀──────────▶ ◀───────────────────────▶ ◀─────────────▶
  //    variable         sizeof(NodeT)       sizeof(ValueT)
  //     length
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  struct NodeBase {
    using Self = NodeBase;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static constexpr u8 kFlagTerminal = 0x80;
    static constexpr u8 kFlagObsolete = 0x40;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const NodeType node_type;

    u8 flags_;
    u8 prefix_len_;
    u8 branch_count_;
    SeqMutex<u32> mutex_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit NodeBase(NodeType t) noexcept
        : node_type{t}
        , flags_{0}
        , prefix_len_{0}
        , branch_count_{0}
    {
    }

    explicit NodeBase(NodeType t, ARTBase::NoInit) noexcept : node_type{t}
    {
    }

    NodeBase(const NodeBase&) = delete;
    NodeBase& operator=(const NodeBase&) = delete;

    template <typename... CaseFns>
    void visit(CaseFns&&... case_fns);

    bool is_terminal() const
    {
      return (this->flags_ & kFlagTerminal) != 0;
    }

    void set_terminal()
    {
      this->flags_ |= kFlagTerminal;
    }

    bool is_obsolete() const
    {
      return (this->flags_ & kFlagObsolete) != 0;
    }

    void set_obsolete()
    {
      this->flags_ |= kFlagObsolete;
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->flags_ = that.flags_;
      this->branch_count_ = that.branch_count_;
      this->set_prefix(that.prefix() + prefix_offset, that.prefix_len_ - prefix_offset);
    }

    const char* prefix() const
    {
      return (const char*)((((std::uintptr_t)this) - this->prefix_len_) & ~std::uintptr_t{7});
    }

    void set_prefix(const char* data, usize len)
    {
      this->prefix_len_ = len;
      if (len) {
        __builtin_memcpy((char*)this->prefix(), data, len);
      }
    }
  };

  struct BranchView {
    NodeBase** p_ptr;
    NodeBase* ptr;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    BranchView() noexcept : p_ptr{nullptr}, ptr{nullptr}
    {
    }

    explicit BranchView(NodeBase*& branch) noexcept : p_ptr{&branch}, ptr{branch}
    {
    }

    void load(NodeBase*& branch)
    {
      this->p_ptr = &branch;
      this->ptr = branch;
    }

    template <typename NodeT>
    NodeT* store(NodeT* new_ptr)
    {
      static_assert(std::is_base_of_v<NodeBase, NodeT>);

      *this->p_ptr = new_ptr;
      this->ptr = new_ptr;
      return new_ptr;
    }

    NodeBase* reload()
    {
      this->ptr = *this->p_ptr;
      return this->ptr;
    }
  };

  static constexpr NodeType node_type_from_branch_count(usize branch_count)
  {
    if (branch_count == 4) {
      return NodeType::kNode4;
    } else if (branch_count == 16) {
      return NodeType::kNode16;
    } else if (branch_count == 48) {
      return NodeType::kNode48;
    } else {
      return NodeType::kNode256;
    }
  }

  static_assert(sizeof(NodeBase) == 8);

  using BranchIndex = u8;

  static constexpr BranchIndex kInvalidBranchIndex = u8{255};

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount, typename Derived>
  struct GrowableNode : NodeBase {
    using Self = GrowableNode;
    using Super = NodeBase;
    using NoInit = ARTBase::NoInit;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<NodeBase*, kBranchCount> branches;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit GrowableNode() noexcept : NodeBase{node_type_from_branch_count(kBranchCount)}
    {
    }

    explicit GrowableNode(NoInit no_init) noexcept
        : NodeBase{node_type_from_branch_count(kBranchCount), no_init}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Derived* derived()
    {
      return (Derived*)this;
    }

    //----- --- -- -  -  -   -

    usize branch_count() const
    {
      return this->branch_count_;
    }

    usize add_branch()
    {
      const usize i = this->branch_count_;
      ++this->branch_count_;
      return i;
    }

    static constexpr usize max_branch_count()
    {
      return kBranchCount;
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      __builtin_memcpy(this->branches.data(),
                       that.branches.data(),
                       this->branch_count() * sizeof(NodeBase*));
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct IndirectIndexedNode : GrowableNode<kBranchCount, IndirectIndexedNode<kBranchCount>> {
    using Self = IndirectIndexedNode;
    using Super = GrowableNode<kBranchCount, Self>;
    using NoInit = ARTBase::NoInit;

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_1(key_byte_offset)                                     \
  if (bit_i == 64) {                                                                               \
    break;                                                                                         \
  }                                                                                                \
  key_byte = key_byte_offset + bit_i;                                                              \
  branch = branch_for_byte[key_byte];                                                              \
  if (branch) {                                                                                    \
    this->sorted_branches_[this->branch_count_] = branch;                                          \
    this->sorted_keys_[this->branch_count_] = key_byte;                                            \
    ++this->branch_count_;                                                                         \
  }                                                                                                \
  bit_i = next_bit(word_val, bit_i)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_2(key_byte_offset)                                     \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_1(key_byte_offset);                                          \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_1(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_4(key_byte_offset)                                     \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_2(key_byte_offset);                                          \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_2(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_8(key_byte_offset)                                     \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_4(key_byte_offset);                                          \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_4(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_16(key_byte_offset)                                    \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_8(key_byte_offset);                                          \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_8(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_32(key_byte_offset)                                    \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_16(key_byte_offset);                                         \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_16(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_64(key_byte_offset)                                    \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_32(key_byte_offset);                                         \
  TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_32(key_byte_offset)

#define TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(word_i, key_byte_offset)                               \
  word_val = key_bitmap[word_i];                                                                   \
  for (;;) {                                                                                       \
    i32 bit_i = first_bit(word_val);                                                               \
    TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_64(key_byte_offset);                                       \
    break;                                                                                         \
  }

    struct ScanState {
      Self& self_;
      usize branch_count_;
      usize i_;
      std::array<NodeBase*, kBranchCount> sorted_branches_;
      std::array<i32, kBranchCount> sorted_keys_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, i32 min_key) noexcept : self_{self}, branch_count_{0}, i_{0}
      {
        std::array<NodeBase*, 256> branch_for_byte;
        std::array<u64, 4> key_bitmap = {0, 0, 0, 0};

        const usize n_branches = this->self_.branch_count();

        for (usize i = 0; i < n_branches; ++i) {
          const i32 key_byte = this->self_.key[i];
          if (key_byte < min_key) {
            continue;
          }
          branch_for_byte[key_byte] = this->self_.branches[i];
          key_bitmap[(key_byte >> 6) & 3] |= (u64{1} << (key_byte & 0x3f));
        }

        u64 word_val;
        i32 key_byte;
        NodeBase* branch;

        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(0, 0)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(1, 64)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(2, 128)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(3, 192)
      }

      i32 get_key_byte() const
      {
        return this->sorted_keys_[this->i_];
      }

      NodeBase* get_branch() const
      {
        return this->sorted_branches_[this->i_];
      }

      bool is_done() const
      {
        return this->i_ >= this->branch_count_;
      }

      void advance()
      {
        ++this->i_;
      }
    };

#undef TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_64
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_32
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_16
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_8
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_4
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_2
#undef TURTLE_KV_ART_SMALL_NODE_INNER_LOOP_1

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<u8, kBranchCount> key;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit IndirectIndexedNode() noexcept : Super{}
    {
    }

    explicit IndirectIndexedNode(NoInit no_init) noexcept : Super{no_init}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    usize index_of_branch(u8 key_byte)
    {
      return index_of(key_byte, this->key);
    }

    void set_branch_index(u8 key_byte, usize i)
    {
      this->key[i] = key_byte;
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      __builtin_memcpy(this->key.data(), that.key.data(), this->branch_count());
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct DirectIndexedNode : GrowableNode<kBranchCount, DirectIndexedNode<kBranchCount>> {
    using Self = DirectIndexedNode;
    using Super = GrowableNode<kBranchCount, Self>;
    using NoInit = ARTBase::NoInit;

    struct ScanState {
      Self& self_;
      i32 min_key_;
      i32 key_byte_;
      usize i_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, i32 min_key) noexcept
          : self_{self}
          , min_key_{min_key}
          , key_byte_{min_key}
          , i_{self.branch_for_key[min_key]}
      {
        this->skip_invalid_branches();
      }

      i32 get_key_byte() const
      {
        return this->key_byte_;
      }

      NodeBase* get_branch() const
      {
        return this->self_.branches[this->key_byte_];
      }

      bool is_done() const
      {
        return this->i_ == kInvalidBranchIndex;
      }

      void advance()
      {
        this->i_ = kInvalidBranchIndex;
        this->skip_invalid_branches();
      }

      void skip_invalid_branches()
      {
        while (this->i_ == kInvalidBranchIndex) {
          ++this->key_byte_;
          this->i_ = this->self_.branch_for_key[this->key_byte_];
        }
      }
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<BranchIndex, 256> branch_for_key;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit DirectIndexedNode() noexcept : Super{}
    {
      this->branch_for_key.fill(kInvalidBranchIndex);
    }

    explicit DirectIndexedNode(NoInit no_init) noexcept : Super{no_init}
    {
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    usize index_of_branch(u8 key_byte)
    {
      return this->branch_for_key[key_byte];
    }

    void set_branch_index(u8 key_byte, usize i)
    {
      this->branch_for_key[key_byte] = i;
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      this->branch_for_key = that.branch_for_key;
    }
  };

  struct Node4 : IndirectIndexedNode<4> {
    using IndirectIndexedNode<4>::IndirectIndexedNode;
  };

  struct Node16 : IndirectIndexedNode<16> {
    using IndirectIndexedNode<16>::IndirectIndexedNode;
  };

  struct Node48 : DirectIndexedNode<48> {
    using DirectIndexedNode<48>::DirectIndexedNode;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node256 : NodeBase {
    using Self = Node256;
    using Super = NodeBase;
    using NoInit = ARTBase::NoInit;

    struct ScanState {
      Self& self_;
      i32 min_key_;
      i32 key_byte_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, i32 min_key) noexcept
          : self_{self}
          , min_key_{min_key}
          , key_byte_{min_key}
      {
      }

      i32 get_key_byte() const
      {
        return this->key_byte_;
      }

      NodeBase* get_branch() const
      {
        return this->self_.branches[this->key_byte_];
      }

      bool is_done() const
      {
        return this->key_byte_ >= 256;
      }

      void advance()
      {
        ++this->key_byte_;
      }
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<NodeBase*, 256> branches;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Node256() noexcept : Super{NodeType::kNode256}
    {
      this->branches.fill(nullptr);
    }

    explicit Node256(NoInit no_init) noexcept : Super{NodeType::kNode256, no_init}
    {
    }

    Node256(const Node256&) = delete;
    Node256& operator=(const Node256&) = delete;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static constexpr usize branch_count()
    {
      return 256;
    }

    usize add_branch()
    {
      BATT_PANIC() << "Node256::add_branch is illegal!";
      BATT_UNREACHABLE();
    }

    static constexpr usize max_branch_count()
    {
      return 256;
    }

    usize index_of_branch(u8 key_byte) const
    {
      return key_byte;
    }

    void set_branch_index(u8, usize)
    {
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      this->branches = that.branches;
    }
  };

  //----- --- -- -  -  -   -

  static_assert(sizeof(Node4) == 48);
  static_assert(sizeof(Node4) % 8 == 0);
  static_assert(alignof(Node4) >= 8);

  static_assert(sizeof(Node16) == 152);
  static_assert(sizeof(Node16) % 8 == 0);
  static_assert(alignof(Node16) >= 8);

  static_assert(sizeof(Node48) == 648);
  static_assert(sizeof(Node48) % 8 == 0);
  static_assert(alignof(Node48) >= 8);

  static_assert(sizeof(Node256) == 2056);
  static_assert(sizeof(Node256) % 8 == 0);
  static_assert(alignof(Node256) >= 8);

  static constexpr usize kExtentSize = 64 * kKiB;

  using ExtentStorageT = std::aligned_storage_t<kExtentSize, 64>;

  static_assert(sizeof(ExtentStorageT) == kExtentSize);

  //----- --- -- -  -  -   -

  struct MemoryContext {
    ARTBase* art_{nullptr};
    std::vector<std::unique_ptr<ExtentStorageT>> thread_extents_;
    u8* data_{nullptr};
    usize in_use_{sizeof(ExtentStorageT)};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    ~MemoryContext() noexcept
    {
      if (this->art_) {
        absl::MutexLock lock{&this->art_->mutex_};
        for (auto& p_ex : this->thread_extents_) {
          this->art_->extents_.emplace_back(std::move(p_ex));
        }
      }
    }

    void* alloc(usize n, ARTBase* art)
    {
      this->art_ = art;

      const usize in_use_prior = this->in_use_;
      if (in_use_prior + n <= sizeof(ExtentStorageT)) {
        this->in_use_ += n;
        return this->data_ + in_use_prior;
      }

      ARTBase::metrics().byte_alloc_count.add(sizeof(ExtentStorageT));

      this->thread_extents_.emplace_back(std::make_unique<ExtentStorageT>());
      this->data_ = reinterpret_cast<u8*>(this->thread_extents_.back().get());
      this->in_use_ = 0;

      return this->alloc(n, art);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  void* alloc_storage(usize n, usize pre)
  {
    const usize pad = (pre + 7) & ~usize{7};
    char* const ptr = (char*)this->per_thread_memory_context_.get().alloc(n + pad, this);
    return ptr + pad;
  }

  absl::Mutex mutex_;
  std::vector<std::unique_ptr<ExtentStorageT>> extents_;
  ObjectThreadStorage<MemoryContext>::ScopedSlot per_thread_memory_context_;
};

namespace detail {

template <typename ValueT>
struct DefaultCopyInserter {
  const ValueT& copy_from_;

  explicit DefaultCopyInserter(const ValueT& copy_from) noexcept : copy_from_{copy_from}
  {
  }

  Status insert_at(void* copy_to)
  {
    new (copy_to) ValueT{this->copy_from_};
    return OkStatus();
  }

  Status update_at(ValueT* copy_to)
  {
    *copy_to = this->copy_from_;
    return OkStatus();
  }
};

template <typename ValueT>
struct DefaultMoveInserter {
  ValueT&& move_from_;

  explicit DefaultMoveInserter(ValueT&& move_from) noexcept : move_from_{move_from}
  {
  }

  Status insert_at(void* move_to)
  {
    new (move_to) ValueT{std::move(this->move_from_)};
    return OkStatus();
  }

  Status update_at(ValueT* move_to)
  {
    *move_to = std::move(this->move_from_);
    return OkStatus();
  }
};

struct DefaultVoidInserter {
  BATT_ALWAYS_INLINE Status insert_at(void*)
  {
    return OkStatus();
  }

  BATT_ALWAYS_INLINE Status update_at(void*)
  {
    return OkStatus();
  }
};

}  // namespace detail

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename ValueT = void>
class ART : public ARTBase
{
 public:
  using Self = ART;
  using Super = ARTBase;

  using value_type = ValueT;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr usize kValueStorageSize = Super::sizeof_value(batt::StaticType<ValueT>{});

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <Synchronized kSynchronized>
  class Scanner;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() noexcept
  {
    ART::metrics().construct_count.add(1);
  }

  ~ART() noexcept
  {
    ART::metrics().destruct_count.add(1);
  }

  template <typename InserterT>
  Status insert(std::string_view key, InserterT&& inserter);

  BATT_ALWAYS_INLINE void insert(std::string_view key)
  {
    static_assert(std::is_same_v<void, ValueT>);

    this->insert(key, detail::DefaultVoidInserter{}).IgnoreError();
  }

  const ValueT* unsynchronized_find(std::string_view key);

  Optional<ValueT> find(std::string_view key);

  bool contains(std::string_view key);

  template <typename Fn>
  void scan(std::string_view lower_bound_key, const Fn& fn);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  template <typename NodeT, typename = std::enable_if_t<!std::is_same_v<NodeT, Node256>>>
  NodeBase* add_child(NodeT* node, u8 key_byte, NodeBase* child);

  NodeBase* add_child(Node256* node, u8 key_byte, NodeBase* child);

  template <typename NodeT>
  Node4* add_child(NodeT* node, u8 key_byte, const char* new_key_data, usize new_key_len);

  Node4* make_node4(const char* prefix, usize prefix_len);

  Node16* grow_node(Node4* old_node);

  Node48* grow_node(Node16* old_node);

  Node256* grow_node(Node48* old_node);

  Node256* grow_node(Node256*);

  Node4* clone_node(Node4* orig_node, usize prefix_offset);

  Node16* clone_node(Node16* orig_node, usize prefix_offset);

  Node48* clone_node(Node48* orig_node, usize prefix_offset);

  Node256* clone_node(Node256* orig_node, usize prefix_offset);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  NodeBase super_root_{NodeType::kNodeBase};
  NodeBase* root_ = nullptr;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... CaseFns>
inline void ARTBase::NodeBase::visit(CaseFns&&... case_fns)
{
  auto visitor = batt::make_case_of_visitor(BATT_FORWARD(case_fns)...);

  const NodeType observed = this->node_type;

  switch (observed) {
    case NodeType::kNode4:
      visitor(static_cast<Node4*>(this));
      break;

    case NodeType::kNode16:
      visitor(static_cast<Node16*>(this));
      break;

    case NodeType::kNode48:
      visitor(static_cast<Node48*>(this));
      break;

    case NodeType::kNode256:
      visitor(static_cast<Node256*>(this));
      break;

    case NodeType::kNodeBase:  // fall-through
    default:
      BATT_PANIC() << "Bad node type: " << (int)observed;
      BATT_UNREACHABLE();
  }
}

namespace detail {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT>
NodeT& scanner_view_of(usize node_prefix_len,
                       NodeT* node,
                       AlignedStorageT* storage,
                       std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kTrue>,
                       const Optional<bool>&)
{
  NodeT& node_view = *(new (storage) NodeT{ARTBase::NoInit{}});

  // Retry the node read until we get a consistent view.
  //
  for (;;) {
    SeqMutex<u32>::ReadLock read_lock{node->mutex_};
    node_view.assign_from(*node, /*prefix_offset=*/node_prefix_len);
    if (!read_lock.changed()) {
      break;
    }
  }

  return node_view;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT>
NodeT& scanner_view_of(usize,
                       NodeT* node,
                       AlignedStorageT*,
                       std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kFalse>,
                       const Optional<bool>&)
{
  return *node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename AlignedStorageT>
NodeT& scanner_view_of(
    usize node_prefix_len,
    NodeT* node,
    AlignedStorageT* storage,
    std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kDynamic>,
    const Optional<bool>& sync)
{
  if (sync.value_or(true)) {
    return scanner_view_of(
        node_prefix_len,
        node,
        storage,
        std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kTrue>{},
        sync);
  }
  return scanner_view_of(
      node_prefix_len,
      node,
      storage,
      std::integral_constant<ARTBase::Synchronized, ARTBase::Synchronized::kFalse>{},
      sync);
}

}  // namespace detail

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename ValueT>
template <ARTBase::Synchronized kSynchronized>
class ART<ValueT>::Scanner
{
 public:
  using Node4 = ARTBase::Node4;
  using Node16 = ARTBase::Node16;
  using Node48 = ARTBase::Node48;
  using Node256 = ARTBase::Node256;

  using NodeScanState = std::variant<batt::NoneType,
                                     Node4::ScanState,
                                     Node16::ScanState,
                                     Node48::ScanState,
                                     Node256::ScanState>;

  static constexpr usize kMaxDepth = ART<ValueT>::kMaxKeyLen;

  using SyncType = std::integral_constant<ARTBase::Synchronized, kSynchronized>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static_assert(sizeof(Node256) > sizeof(Node48));
  static_assert(sizeof(Node256) > sizeof(Node16));
  static_assert(sizeof(Node256) > sizeof(Node4));

  struct Frame {
    static constexpr usize kStorageSize =
        (kSynchronized == ARTBase::Synchronized::kFalse) ? 1 : sizeof(Node256);

    std::aligned_storage_t<kStorageSize, alignof(usize)> node_storage_;
    NodeScanState scan_state_;
    usize key_prefix_len_;
    std::string_view lower_bound_key_;
    i32 min_key_byte_;

    explicit Frame(usize key_prefix_len, std::string_view lower_bound_key) noexcept
        : scan_state_{None}
        , key_prefix_len_{key_prefix_len}
        , lower_bound_key_{lower_bound_key}
        , min_key_byte_{0}
    {
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::aligned_storage_t<sizeof(Frame) * kMaxDepth, /*alignment=*/64> stack_storage_;
  Frame* end_ = reinterpret_cast<Frame*>(&this->stack_storage_);
  usize depth_ = 0;
  std::array<char, ART<ValueT>::kMaxKeyLen> key_buffer_;
  Optional<std::string_view> next_key_;
  Optional<bool> synchronized_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Scanner(ART& art,
                   std::string_view lower_bound_key,
                   Optional<bool> synchronized = None) noexcept
      : synchronized_{synchronized}
  {
    NodeBase* root = nullptr;
    for (;;) {
      SeqMutex<u32>::ReadLock root_read_lock{art.super_root_.mutex_};
      root = art.root_;
      if (!root_read_lock.changed()) {
        break;
      }
    }

    if (root) {
      root->visit([&](auto* node) {
        this->enter(node, /*key_prefix_len=*/0, lower_bound_key);
      });

      if (!this->next_key_) {
        this->advance();
      }
    }
  }

  bool is_synchronized() const
  {
    if (kSynchronized == ARTBase::Synchronized::kFalse) {
      return false;
    }
    if (kSynchronized == ARTBase::Synchronized::kTrue) {
      return true;
    }
    return this->synchronized_.value_or(true);
  }

  template <typename NodeT>
  void enter(NodeT* node, usize key_prefix_len, std::string_view lower_bound_key)
  {
    Frame* top = new (this->end_) Frame{key_prefix_len, lower_bound_key};
    ++this->depth_;
    ++this->end_;

    // Node prefix is immutable, so we don't need synchronization.
    //
    const char* const node_prefix = node->prefix();
    const usize node_prefix_len = node->prefix_len_;

    // We need to create a copy of the node data to protect against data races.
    //
    NodeT& node_view = detail::scanner_view_of(node_prefix_len,
                                               node,
                                               &top->node_storage_,
                                               SyncType{},
                                               this->synchronized_);

    // Compare the lower bound key to the current node prefix.
    //
    const usize compare_len = std::min<usize>(node_prefix_len, top->lower_bound_key_.size());
    if (compare_len) {
      const i32 order = __builtin_memcmp(node_prefix, top->lower_bound_key_.data(), compare_len);

      // If all keys in this subtree come before the lower bound, then there is nothing to do.
      //
      if (order < 0) {
        --this->depth_;
        --this->end_;
        return;
      }

      // If the node prefix is a prefix of the lower bound key, then drop the prefix from the lower
      // bound; otherwise the node prefix comes *after* the lower bound, so we can safely ignore the
      // lower bound for the rest of the recursion.
      //
      if (order == 0 && compare_len == node_prefix_len) {
        top->lower_bound_key_.remove_prefix(compare_len);
      } else {
        top->lower_bound_key_ = {};
      }
    }

    // Set bounds for branch visitation.
    //
    top->min_key_byte_ = [&]() -> i32 {
      if (top->lower_bound_key_.empty()) {
        return 0;
      }
      const i32 next_char = top->lower_bound_key_.front();
      top->lower_bound_key_.remove_prefix(1);
      return next_char;
    }();

    // Append the node prefix to the buffer.
    //
    if (node_prefix_len) {
      __builtin_memcpy(this->key_buffer_.data() + top->key_prefix_len_,
                       node_prefix,
                       node_prefix_len);

      top->key_prefix_len_ += node_prefix_len;
    }

    // If the current node is a key-terminal, emit the contents of the buffer.
    //
    if (node_view.is_terminal()) {
      this->next_key_.emplace(this->key_buffer_.data(), top->key_prefix_len_);
    } else {
      this->next_key_ = None;
    }

    top->scan_state_.template emplace<typename NodeT::ScanState>(node_view, top->min_key_byte_);
  }

  bool is_done() const
  {
    return this->depth_ == 0;
  }

  const std::string_view& get_key() const
  {
    return *this->next_key_;
  }

  void advance()
  {
    this->next_key_ = None;

    for (;;) {
      if (this->depth_ == 0) {
        return;
      }

      Frame* top = this->end_ - 1;

      batt::case_of(
          top->scan_state_,
          [](batt::NoneType&) {
            BATT_PANIC() << "empty Scanner stack frame!";
          },
          [&](auto& scan_state)
              -> std::enable_if_t<
                  !std::is_same_v<std::decay_t<decltype(scan_state)>, batt::NoneType>> {
            //----- --- -- -  -  -   -
            if (scan_state.is_done()) {
              --this->depth_;
              --this->end_;
              return;
            }

            const i32 key_byte = scan_state.get_key_byte();
            NodeBase* const child = scan_state.get_branch();

            this->key_buffer_[top->key_prefix_len_] = (char)key_byte;

            if (key_byte == top->min_key_byte_) {
              child->visit([&](auto* child_node) {
                this->enter(child_node, top->key_prefix_len_ + 1, top->lower_bound_key_);
              });
            } else {
              child->visit([&](auto* child_node) {
                this->enter(child_node, top->key_prefix_len_ + 1, std::string_view{});
              });
            }

            scan_state.advance();
          });

      if (this->next_key_) {
        return;
      }
    }
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
template <typename Fn>
inline void ART<ValueT>::scan(std::string_view lower_bound_key, const Fn& fn)
{
  Scanner<Synchronized::kTrue> scanner{*this, lower_bound_key};

  while (!scanner.is_done()) {
    if (!fn(scanner.get_key())) {
      return;
    }
    scanner.advance();
  }
}

}  // namespace turtle_kv

#include <turtle_kv/util/art.ipp>
