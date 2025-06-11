#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <batteries/case_of.hpp>

#include <absl/synchronization/mutex.h>

#include <array>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

#include <emmintrin.h>
#include <immintrin.h>
#include <mmintrin.h>
#include <pmmintrin.h>

namespace turtle_kv {

#define TURTLE_KV_ART_MEMORY_POOL 1

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

#if 0
  // TODO [tastolfi 2025-06-11] if AVX-512 isn't available:
  __m128i _mm_cmpeq_epi8 (__m128i a, __m128i b);
  int _mm_movemask_epi8 (__m128i a);
#else
  __mmask16 result = _mm_cmpeq_epi8_mask(pattern, values);

  return (__builtin_ffs(result) - 1) & 31;
#endif
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class AtomicBranchCount
{
 public:
  using Self = AtomicBranchCount;

  static usize branch_count_from_state(u32 state)
  {
    return (state >> 16);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit AtomicBranchCount(usize init_val) noexcept
      : state_{(init_val & 0xffff) | ((init_val << 16) & 0xffff0000)}
  {
  }

  usize try_reserve(usize max_branches)
  {
    const usize branch_i = this->state_.fetch_add(1);
    if (branch_i < max_branches) {
      return branch_i;
    }
    this->state_.fetch_sub(1);
    return max_branches;
  }

  void commit(usize branch_i)
  {
    for (;;) {
      const u32 observed = this->state_.load();
      if (Self::branch_count_from_state(observed) == branch_i) {
        this->state_.fetch_add(0x10000);
        return;
      }
    }
  }

  usize get() const
  {
    return Self::branch_count_from_state(this->state_.load());
  }

 private:
  std::atomic<u32> state_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SeqMutex
{
 public:
  class ScopedLock
  {
   public:
    explicit ScopedLock(SeqMutex* mutex) noexcept : mutex_{mutex}
    {
      if (mutex) {
        for (;;) {
          const u16 old_state = mutex->state_.fetch_or(1);
          if ((old_state & 1) == 0) {
            mutex->state_.fetch_add(2);
            return;
          }
        }
      }
    }

    ~ScopedLock() noexcept
    {
      if (this->mutex_) {
        this->mutex_->state_.fetch_add(1);
      }
    }

   private:
    SeqMutex* mutex_;
  };

  u32 observe() const
  {
    return this->state_.load();
  }

 private:
  std::atomic<u32> state_{0};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class ART
{
 public:
  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

  class NodePtr
  {
   public:
    using Self = NodePtr;

    static constexpr u64 kTypeNode4 = 0b000;
    static constexpr u64 kTypeNode16 = 0b001;
    static constexpr u64 kTypeNode48 = 0b010;
    static constexpr u64 kTypeNode256 = 0b011;

    static constexpr u64 kTypeMask = u64{0b011};
    static constexpr u64 kLeafMask = u64{0b100};
    static constexpr u64 kPtrMask = ~u64{0b111};

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    NodePtr() noexcept : val_{0}
    {
    }

    explicit NodePtr(u64 val) noexcept : val_{val}
    {
    }

    NodePtr(std::nullptr_t) noexcept : val_{0}
    {
    }

    NodePtr(Node4* ptr) noexcept : val_{reinterpret_cast<u64>(ptr)}
    {
      BATT_CHECK_EQ(this->val_ & kPtrMask, this->val_);
      this->val_ |= kTypeNode4;
    }

    NodePtr(Node16* ptr) noexcept : val_{reinterpret_cast<u64>(ptr)}
    {
      BATT_CHECK_EQ(this->val_ & kPtrMask, this->val_);
      this->val_ |= kTypeNode16;
    }

    NodePtr(Node48* ptr) noexcept : val_{reinterpret_cast<u64>(ptr)}
    {
      BATT_CHECK_EQ(this->val_ & kPtrMask, this->val_);
      this->val_ |= kTypeNode48;
    }

    NodePtr(Node256* ptr) noexcept : val_{reinterpret_cast<u64>(ptr)}
    {
      BATT_CHECK_EQ(this->val_ & kPtrMask, this->val_);
      this->val_ |= kTypeNode256;
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    bool compare_exchange(NodePtr& expected_value, NodePtr desired_value)
    {
      return this->atomic_val().compare_exchange_weak(expected_value, desired_value);
    }

    template <typename... CaseFns>
    bool visit(CaseFns&&... case_fns);

    void set_leaf(bool b)
    {
      if (b) {
        this->val_ |= kLeafMask;
      } else {
        this->val_ &= ~kLeafMask;
      }
    }

    bool is_leaf() const
    {
      return (this->val_ & kLeafMask) == kLeafMask;
    }

    u64 get_type() const
    {
      return this->val_ & kTypeMask;
    }

    bool operator!() const noexcept
    {
      return this->get() == nullptr;
    }

    explicit operator bool() const noexcept
    {
      return this->get() != nullptr;
    }

    friend inline bool operator==(NodePtr left, NodePtr right) noexcept
    {
      return left.val_ == right.val_;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const NodePtr& t)
    {
      return out << (void*)t.get();
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
   private:
    std::atomic<u64>& atomic_val()
    {
      return reinterpret_cast<std::atomic<u64>&>(this->val_);
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    u64 val_;
  };

  static_assert(sizeof(NodePtr) == sizeof(u64));

  struct NodePtrView {
    using Self = NodePtrView;

    NodePtr ptr;
    NodePtr* p_ptr;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    NodePtrView() noexcept : ptr{nullptr}, p_ptr{nullptr}
    {
    }

    explicit NodePtrView(NodePtr& that) noexcept : ptr{that}, p_ptr{&that}
    {
    }

    explicit operator bool() const noexcept
    {
      return this->p_ptr != nullptr && this->ptr != nullptr;
    }

    bool operator!() const noexcept
    {
      return this->p_ptr == nullptr || this->ptr == nullptr;
    }

    Self& operator=(NodePtr& that) noexcept
    {
      this->ptr = that;
      this->p_ptr = &that;
      return *this;
    }

    friend inline std::ostream& operator<<(std::ostream& out, const NodePtrView& t)
    {
      return out << "{.ptr=" << t.ptr << ", .p_ptr=" << t.p_ptr << ",}";
    }
  };

  using BranchIndex = u8;

  static constexpr BranchIndex kInvalidBranchIndex = u8{255};

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct SmallNode {
    std::array<NodePtr, kBranchCount> branches;
    std::array<u8, kBranchCount> key;
    AtomicBranchCount branch_count;
    NodePtr successor;

    //----- --- -- -  -  -   -

    explicit SmallNode() noexcept
    {
    }

    explicit SmallNode(const SmallNode<4>& old) noexcept : SmallNode{}
    {
      this->size_ = old.size_;
      std::copy(old.branches.begin(), old.branches.end(), this->branches.begin());
      std::copy(old.key.begin(), old.key.end(), this->key.begin());
    }

    NodePtrView find(u8 key_byte)
    {
      BATT_PANIC() << "TODO [tastolfi 2025-06-11] ";
      BATT_UNREACHABLE();
    }

    NodePtrView insert(u8 key_byte, ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node4 : SmallNode<4> {
    using SmallNode<4>::SmallNode;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node16 : SmallNode<16> {
    using SmallNode<16>::SmallNode;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node48 {
    std::array<NodePtr, 48> branches;
    std::array<BranchIndex, 256> branch_for_key;
    AtomicBranchCount branch_count;
    NodePtr successor;

    //----- --- -- -  -  -   -

    explicit Node48(const Node16& old) noexcept : NodeBase{NodeType::kNode48}
    {
      BATT_CHECK_EQ(old.size_, 16);

      this->size_ = old.size_;
      this->branch_for_key.fill(kInvalidBranchIndex);

      for (usize i = 0; i < 16; ++i) {
        this->branch_for_key[old.key[i]] = i;
        this->branches[i] = old.branches[i];
      }
    }

    void destroy_branches() noexcept
    {
      for (usize i = 0; i < this->size_; ++i) {
        NodePtr& branch = this->branches[i];
        NodeBase* ptr = branch.get();
        if (ptr) {
          delete ptr;
        }
      }
    }

    NodePtrView find(u8 key_byte)
    {
      for (;;) {
        const u16 before_state = this->NodeBase::state_.load();
        if ((before_state & 3) != 0) {
          continue;
        }

        NodePtrView found;

        const BranchIndex branch_i = this->branch_for_key[key_byte];
        if (branch_i != kInvalidBranchIndex) {
          found = this->branches[branch_i];
        }

        const u16 after_state = this->NodeBase::state_.load();
        if (before_state != after_state) {
          continue;
        }

        return found;
      }
    }

    NodePtrView insert(u8 key_byte, ART* art);

    NodePtr grow(ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node256 {
    std::array<NodePtr, 256> branches;

    //----- --- -- -  -  -   -

    Node256() noexcept : NodeBase{NodeType::kNode256}
    {
      this->branches.fill(nullptr);
    }

    explicit Node256(const Node48& old) noexcept : NodeBase{NodeType::kNode256}
    {
      BATT_CHECK_EQ(old.size_, 48);

      for (usize key_byte = 0; key_byte < 256; ++key_byte) {
        const BranchIndex branch_i = old.branch_for_key[key_byte];
        if (branch_i == kInvalidBranchIndex) {
          this->branches[key_byte] = nullptr;
        } else {
          this->branches[key_byte] = old.branches[branch_i];
        }
      }
    }

    NodePtrView find(u8 key_byte)
    {
      return NodePtrView{this->branches[key_byte]};
    }

    NodePtrView insert(u8 key_byte, ART* art);

    NodePtr grow(ART* art);
  };

  //----- --- -- -  -  -   -

  static_assert(sizeof(Node4) == 48);
  static_assert(sizeof(Node4) % 8 == 0);
  static_assert(alignof(Node4) >= 8);

  static_assert(sizeof(Node16) == 152);
  static_assert(sizeof(Node16) % 8 == 0);
  static_assert(alignof(Node16) >= 8);

  static_assert(sizeof(Node48) == 644);
  static_assert(sizeof(Node48) % 8 == 0);
  static_assert(alignof(Node48) >= 8);

  static_assert(sizeof(Node256) == 2048);
  static_assert(sizeof(Node256) % 8 == 0);
  static_assert(alignof(Node256) >= 8);

  using ExtentStorageT = std::aligned_storage_t<sizeof(Node256) * 1024 * 1024, 64>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() = default;

  void put(std::string_view key)
  {
    DVLOG(1) << "[put]" << BATT_INSPECT_STR(key);

    NodePtr root = &this->root_;
    NodePtrView node{root};
    NodePtrView parent;

    for (char key_char : key) {
      //----- --- -- -  -  -   -
      for (bool retry = true; retry;) {
        const u16 before_state = parent ? parent.ptr->state_.load() : 0;
        if ((before_state & 3) != 0) {
          continue;
        }

        const u8 key_byte = key_char;

        retry = false;
        if (!this->visit_node(node.ptr, [&](auto&& node_case) {
              NodePtrView child = node_case->insert(key_byte, this);
              const u16 after_state = parent ? parent.ptr->state_.load() : 0;
              if (before_state != after_state) {
                retry = true;
              } else if (!child) {
                NodeBase::SpinLock lock{parent.ptr.get()};
                node.ptr = node_case->grow(this);
                *node.p_ptr = node.ptr;
                retry = true;
              } else {
                parent = node;
                node = child;
              }
            })) {
          retry = true;
        }
      }
    }

    if (parent) {
      parent.p_ptr->set_term(true);
    }
  }

  bool contains(std::string_view key)
  {
    DVLOG(1) << "[contains]" << BATT_INSPECT_STR(key);

    NodePtr root = &this->root_;
    NodePtrView node{root};

    for (char key_char : key) {
      //----- --- -- -  -  -   -
      const u8 key_byte = key_char;

      while (!this->visit_node(node.ptr, [&](auto* node_case) {
        node = node_case->find(key_byte);
      })) {
        continue;
      }

      if (node.p_ptr == nullptr || node.ptr == nullptr) {
        return false;
      }
    }

    return true;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct ExtentState {
    u8* data_{nullptr};
    usize in_use_{sizeof(ExtentStorageT)};

    void reset(u8* data)
    {
      this->data_ = data;
      this->in_use_ = 0;
    }

    u8* alloc(usize n)
    {
      const usize in_use_prior = this->in_use_;
      if (in_use_prior + n > sizeof(ExtentStorageT)) {
        return nullptr;
      }
      this->in_use_ += n;
      return this->data_ + in_use_prior;
    }
  };

  struct MemoryContext {
    std::vector<std::unique_ptr<ExtentStorageT>> extents_;
    ExtentState current_state_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    void* alloc(usize n)
    {
      ExtentState& state = this->current_state_;
      void* ptr = state.alloc(n);
      if (ptr) {
        return ptr;
      }

      this->extents_.emplace_back(std::make_unique<ExtentStorageT>());
      state.reset((u8*)this->extents_.back().get());
      return this->alloc(n);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void* alloc_storage(usize n)
  {
    return this->per_thread_memory_context_.get().alloc(n);
  }

  template <typename... CaseFns>
  bool visit_node(NodePtr node, CaseFns&&... case_fns)
  {
    return node->visit(BATT_FORWARD(case_fns)...);
  }

#if 0
  Leaf* alloc_leaf()
  {
    return &this->leaf_;
  }
#endif

  Node4* alloc_node4()
  {
#if TURTLE_KV_ART_MEMORY_POOL
    return new (this->alloc_storage(sizeof(Node4))) Node4{};
#else   // TURTLE_KV_ART_MEMORY_POOL
    return new Node4{};
#endif  // TURTLE_KV_ART_MEMORY_POOL
  }

  Node16* alloc_node16(const Node4& old)
  {
#if TURTLE_KV_ART_MEMORY_POOL
    return new (this->alloc_storage(sizeof(Node16))) Node16{old};
#else   // TURTLE_KV_ART_MEMORY_POOL
    return new Node16{old};
#endif  // TURTLE_KV_ART_MEMORY_POOL
  }

  Node48* alloc_node48(const Node16& old)
  {
#if TURTLE_KV_ART_MEMORY_POOL
    return new (this->alloc_storage(sizeof(Node48))) Node48{old};
#else   // TURTLE_KV_ART_MEMORY_POOL
    return new Node48{old};
#endif  // TURTLE_KV_ART_MEMORY_POOL
  }

  Node256* alloc_node256(const Node48& old)
  {
#if TURTLE_KV_ART_MEMORY_POOL
    return new (this->alloc_storage(sizeof(Node256))) Node256{old};
#else   // TURTLE_KV_ART_MEMORY_POOL
    return new Node256{old};
#endif  // TURTLE_KV_ART_MEMORY_POOL
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Node256 root_;
  // Leaf leaf_;
  ObjectThreadStorage<MemoryContext>::ScopedSlot per_thread_memory_context_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... CaseFns>
inline bool ART::NodeBase::visit(CaseFns&&... case_fns)
{
  auto visitor = batt::make_case_of_visitor(BATT_FORWARD(case_fns)...);

  switch (this->node_type) {
#if 0
    case NodeType::kLeaf:
      visitor(static_cast<Leaf*>(this));
      break;
#endif
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
    default:
      return false;
  }
  return true;
}

#if 0
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Leaf::grow(ART* art) -> NodePtr
{
  return art->alloc_node4();
}
#endif

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kBranchCount>
inline auto ART::SmallNode<kBranchCount>::insert(u8 key_byte, ART* art) -> NodePtrView
{
  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    const usize observed_size = std::min<usize>(kBranchCount, this->size_);
    NodePtrView found;
    {
      const usize i = index_of(key_byte, this->key);
      if (i < observed_size) {
        found = this->branches[i];
      }
    }

    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    if (found) {
      return found;
    }

    if (observed_size < kBranchCount) {
      NodeBase::SpinLock lock{this};

      // If the size changes, we must re-check the keys array.
      //
      if (this->size_ != observed_size) {
        continue;
      }

      const usize i = this->size_;
      //----- --- -- -  -  -   -
      this->key[i] = key_byte;
      this->branches[i] = art->alloc_node4();
      //----- --- -- -  -  -   -
      ++this->size_;

      return NodePtrView{this->branches[i]};
    }

    return found;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node4::grow(ART* art) -> NodePtr
{
  return art->alloc_node16(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node16::grow(ART* art) -> NodePtr
{
  return art->alloc_node48(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node48::insert(u8 key_byte, ART* art) -> NodePtrView
{
  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    usize i = this->branch_for_key[key_byte];
    if (i == kInvalidBranchIndex) {
      NodeBase::SpinLock lock{this};

      // We must re-check the branch index for the given byte, to make sure some other thread didn't
      // insert it.
      //
      if (this->branch_for_key[key_byte] != kInvalidBranchIndex) {
        continue;
      }

      // If there is no more room, fail.
      //
      if (this->size_ == 48) {
        return NodePtrView{};
      }

      // We have exclusive access, a branch for the search key is still not found, and we have room
      // in this node; add a new branch.
      //
      i = this->size_;
      //----- --- -- -  -  -   -
      this->branches[i] = art->alloc_node4();
      this->branch_for_key[key_byte] = i;
      //----- --- -- -  -  -   -
      ++this->size_;

      return NodePtrView{this->branches[i]};
    }

    NodePtrView found{this->branches[i]};

    // Done reading; re-load the SeqLock state to see if we must retry.
    //
    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    return found;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node48::grow(ART* art) -> NodePtr
{
  return art->alloc_node256(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::insert(u8 key_byte, ART* art) -> NodePtrView
{
#if 1
  auto& atomic_branch = reinterpret_cast<std::atomic<u64>&>(this->branches[key_byte]);

  u64 observed_val = atomic_branch.load();
  for (;;) {
    if (observed_val != 0) {
      NodePtrView view;
      view.ptr = NodePtr{observed_val};
      view.p_ptr = &this->branches[key_byte];
      return view;
    }

    NodeBase* new_child = art->alloc_node4();
    u64 new_branch_val = reinterpret_cast<u64>(new_child);

    if (atomic_branch.compare_exchange_weak(observed_val, new_branch_val)) {
      observed_val = new_branch_val;
      this->NodeBase::state_.fetch_add(2);
    }
  }
  BATT_UNREACHABLE();

#else
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    NodePtrView found{this->branches[key_byte]};

    if (!found) {
      NodeBase::SpinLock lock{this};
      if (this->branches[key_byte] != nullptr) {
        continue;
      }
      this->branches[key_byte] = art->alloc_node4();
      return NodePtrView{this->branches[key_byte]};
    }

    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    return found;
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::grow(ART*) -> NodePtr
{
  BATT_PANIC() << "invalid operation: Node256::grow()";
  return nullptr;
}

}  // namespace turtle_kv
