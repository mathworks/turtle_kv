#pragma once
#define TURTLE_KV_UTIL_ART_BASE_HPP

#include "art_bit_ops.hpp"
#include "art_metrics.hpp"
#include "art_mutex.hpp"
#include "byte_int.hpp"
#include "seq_mutex.hpp"

#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/object_thread_storage.hpp>
#include <batteries/type_traits.hpp>

namespace turtle_kv {

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

  using Metrics = ARTMetrics;

  static Metrics& default_metrics()
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
  struct LeafNode;

  enum struct NodeType : u8 {
    kLeafNode = 0,
    kNode4 = 1,
    kNode16 = 2,
    kNode48 = 3,
    kNode256 = 4,
    kNodeBase = 5,
  };

  //----- --- -- -  -  -   -

  static constexpr usize sizeof_value(batt::StaticType<void>)
  {
    return 0;
  }

  template <typename ValueT>
  static constexpr usize sizeof_value(batt::StaticType<ValueT>)
  {
    return sizeof(ValueT);
  }

  //----- --- -- -  -  -   -

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

  //----- --- -- -  -  -   -

  template <typename FromNodeT, typename ToNodeT>
  static void* construct_value_copy_node(FromNodeT*, ToNodeT* to_node, batt::StaticType<void>)
  {
    to_node->set_terminal();
    return nullptr;
  }

  template <typename FromNodeT>
  static void* construct_value_copy_addr(FromNodeT*, void*, batt::StaticType<void>)
  {
    return nullptr;
  }

  //----- --- -- -  -  -   -

  template <typename FromNodeT, typename ToNodeT, typename ValueT>
  static ValueT* construct_value_copy_node(FromNodeT* from_node,
                                           ToNodeT* to_node,
                                           batt::StaticType<ValueT> type_of_value)
  {
    to_node->set_terminal();
    return ARTBase::construct_value_copy_addr(from_node,
                                              ARTBase::uninitialized_value(to_node),
                                              type_of_value);
  }

  template <typename FromNodeT, typename ValueT>
  static ValueT* construct_value_copy_addr(FromNodeT* from_node,
                                           void* to_address,
                                           batt::StaticType<ValueT> type_of_value)
  {
    return new (to_address) ValueT{*ARTBase::const_value(from_node, type_of_value)};
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

  struct LeafNode : NodeBase {
    using Self = LeafNode;
    using Super = NodeBase;
    using NoInit = ARTBase::NoInit;

    explicit LeafNode() noexcept : Super{NodeType::kLeafNode}
    {
    }

    explicit LeafNode(NoInit no_init) noexcept : Super{NodeType::kLeafNode, no_init}
    {
    }

    static usize add_branch()
    {
      BATT_PANIC() << "not supported!";
      return 0;
    }

    static void set_branch_index(u8 key_byte [[maybe_unused]], usize index [[maybe_unused]])
    {
      BATT_PANIC() << "not supported!";
    }

    static void set_branch_pointer(usize index [[maybe_unused]], NodeBase* child [[maybe_unused]])
    {
      BATT_PANIC() << "not supported!";
    }

    static constexpr usize max_branch_count()
    {
      return 0;
    }

    static constexpr usize branch_count()
    {
      return 0;
    }

    static constexpr usize index_of_branch(u8 key_byte [[maybe_unused]])
    {
      return 0;
    }

    static NodeBase*& get_branch_ref(usize i [[maybe_unused]])
    {
      static NodeBase* null_ = nullptr;
      return null_;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    struct ScanState {
      explicit ScanState(Self&, ByteInt /*min_key*/) noexcept
      {
      }

      static constexpr ByteInt get_key_byte()
      {
        return ByteInt::from_char('\0');
      }

      static constexpr NodeBase* get_branch()
      {
        return nullptr;
      }

      static constexpr bool is_done()
      {
        return true;
      }

      static constexpr void advance()
      {
      }
    };
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

    std::array<NodeBase*, kBranchCount> branches_;

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

    NodeBase*& get_branch_ref(usize i) BATT_ALWAYS_INLINE
    {
      return this->branches_[i];
    }

    void set_branch_pointer(usize i, NodeBase* child) BATT_ALWAYS_INLINE
    {
      this->branches_[i] = child;
    }

    static constexpr usize max_branch_count()
    {
      return kBranchCount;
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      __builtin_memcpy(this->branches_.data(),
                       that.branches_.data(),
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
  key_byte = ByteInt::from_i32(key_byte_offset + bit_i);                                           \
  branch = branch_for_byte[key_byte.to_i32()];                                                     \
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
      std::array<ByteInt, kBranchCount> sorted_keys_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, ByteInt min_key) noexcept
          : self_{self}
          , branch_count_{0}
          , i_{0}
      {
        std::array<NodeBase*, 256> branch_for_byte;
        std::array<u64, 4> key_bitmap = {0, 0, 0, 0};

        const usize n_branches = this->self_.branch_count();

        for (usize i = 0; i < n_branches; ++i) {
          const ByteInt key_byte = ByteInt::from_u8(this->self_.key[i]);
          if (key_byte < min_key) {
            continue;
          }
          branch_for_byte[key_byte.to_i32()] = this->self_.branches_[i];
          key_bitmap[(key_byte.to_i32() >> 6) & 3] |= (u64{1} << (key_byte.to_i32() & 0x3f));
        }

        u64 word_val;
        ByteInt key_byte;
        NodeBase* branch;

        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(0, 0)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(1, 64)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(2, 128)
        TURTLE_KV_ART_SMALL_NODE_OUTER_LOOP(3, 192)
      }

      ByteInt get_key_byte() const
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

    void set_branch_index(u8 key_byte, usize i) BATT_ALWAYS_INLINE
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
      ByteInt key_byte_;
      usize branch_i_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, ByteInt min_key) noexcept
          : self_{self}
          , key_byte_{min_key}
          , branch_i_{kInvalidBranchIndex}
      {
        this->skip_invalid_branches();
      }

      ByteInt get_key_byte() const
      {
        return this->key_byte_;
      }

      NodeBase* get_branch() const
      {
        return this->self_.branches_[this->branch_i_];
      }

      bool is_done() const
      {
        return this->key_byte_ >= ByteInt::from_i32(256);
      }

      void advance()
      {
        ++this->key_byte_;
        this->skip_invalid_branches();
      }

      void skip_invalid_branches()
      {
        while (!this->is_done()) {
          this->branch_i_ = this->self_.branch_for_key[this->key_byte_.to_i32()];
          if (this->branch_i_ != kInvalidBranchIndex) {
            break;
          }
          ++this->key_byte_;
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

    void set_branch_index(u8 key_byte, usize i) BATT_ALWAYS_INLINE
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
      ByteInt key_byte_;

      //----- --- -- -  -  -   -

      explicit ScanState(Self& self, ByteInt min_key) noexcept : self_{self}, key_byte_{min_key}
      {
        this->skip_null_branches();
      }

      ByteInt get_key_byte() const
      {
        return this->key_byte_;
      }

      NodeBase* get_branch() const
      {
        return this->self_.branches_[this->key_byte_.to_i32()];
      }

      bool is_done() const
      {
        return this->key_byte_ >= ByteInt::from_i32(256);
      }

      void advance()
      {
        ++this->key_byte_;
        this->skip_null_branches();
      }

     private:
      void skip_null_branches()
      {
        while (!this->is_done() && this->get_branch() == nullptr) {
          ++this->key_byte_;
        }
      }
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    std::array<NodeBase*, 256> branches_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    Node256() noexcept : Super{NodeType::kNode256}
    {
      this->branches_.fill(nullptr);
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

    NodeBase*& get_branch_ref(usize i) BATT_ALWAYS_INLINE
    {
      return this->branches_[i];
    }

    void assign_from(const Self& that, usize prefix_offset = 0)
    {
      this->Super::assign_from(static_cast<const Super&>(that), prefix_offset);
      this->branches_ = that.branches_;
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
  static constexpr usize kExtentAlign = 4096;

  using ExtentStorageT = std::aligned_storage_t<kExtentSize, kExtentAlign>;

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
        ARTMutexLock lock{this->art_->mutex_};
        for (auto& p_ex : this->thread_extents_) {
          this->art_->extents_.emplace_back(std::move(p_ex));
        }
      }
    }

    void* alloc(usize n, ARTBase* art)
    {
      this->art_ = art;

      const usize in_use_prior = this->in_use_;
      if (in_use_prior + n <= kExtentSize) {
        this->in_use_ += n;
        return this->data_ + in_use_prior;
      }

      this->art_->metrics_.byte_alloc_count.add(sizeof(ExtentStorageT));

      this->thread_extents_.emplace_back(std::make_unique<ExtentStorageT>());
      u8* start = reinterpret_cast<u8*>(this->thread_extents_.back().get());
      this->data_ = start;
      this->in_use_ = 0;

      return this->alloc(n, art);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit ARTBase() noexcept : ARTBase{ARTBase::default_metrics()}
  {
    // No update of construct count because we delegate to general-case ctor.
  }

  explicit ARTBase(Metrics& metrics) noexcept : metrics_{metrics}
  {
    this->metrics_.construct_count.add(1);
  }

  ~ARTBase() noexcept
  {
    this->metrics_.destruct_count.add(1);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  /** \brief RAII (guard) class that updates byte_free_count metric at the right moment during
   * destruction of the ART (see comment in data member declarations below).
   */
  struct ExtentMetricsUpdateGuard {
    ARTBase& art_base_;

    //----- --- -- -  -  -   -

    explicit ExtentMetricsUpdateGuard(ARTBase& art_base) noexcept : art_base_{art_base}
    {
    }

    ~ExtentMetricsUpdateGuard() noexcept
    {
      this->art_base_.metrics_.byte_free_count.add(this->art_base_.extents_.size() *
                                                   sizeof(ExtentStorageT));
    }
  };

  void* alloc_storage(usize n, usize pre)
  {
    const usize pad = (pre + 7) & ~usize{7};
    char* const ptr = (char*)this->per_thread_memory_context_.get().alloc(n + pad, this);
    return ptr + pad;
  }

  Metrics& metrics_;
  ARTMutex mutex_;
  std::vector<std::unique_ptr<ExtentStorageT>> extents_;
  //
  // Must be placed exactly here, so it will be destructed after the ScopedSlot but before extents_.
  ExtentMetricsUpdateGuard guard_{*this};
  //
  batt::ObjectThreadStorage<MemoryContext>::ScopedSlot per_thread_memory_context_;
};

}  // namespace turtle_kv
