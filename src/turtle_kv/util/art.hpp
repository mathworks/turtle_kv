#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  AtomicBranchCount() noexcept : committed_{0}, reserved_{0}
  {
  }

  explicit AtomicBranchCount(u16 init_val) noexcept : committed_{init_val}, reserved_{init_val}
  {
  }

  usize try_reserve(usize max_branches)
  {
    const usize branch_i = this->reserved_.fetch_add(1);
    if (branch_i < max_branches) {
      return branch_i;
    }
    this->reserved_.fetch_sub(1);
    return max_branches;
  }

  void wait_for(u16 n)
  {
    while (this->committed_.load() != n) {
      continue;
    }
  }

  void commit(usize branch_i)
  {
    const usize old_count = this->committed_.exchange(branch_i + 1);
    BATT_CHECK_EQ(old_count, branch_i);
  }

  usize get() const
  {
    return this->committed_.load();
  }

 private:
  std::atomic<u16> committed_;
  std::atomic<u16> reserved_;
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
  using Self = ART;

  static constexpr usize kMaxKeyLen = 64;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <usize kBranchCount>
  struct SmallNode;

  using Node4 = SmallNode<4>;
  using Node16 = SmallNode<16>;

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

    NodePtr sync()
    {
      return NodePtr{this->atomic_val().load()};
    }

    NodePtr latch(NodePtr desired)
    {
      NodePtr observed = this->sync();
      while (!observed) {
        if (this->atomic_val().compare_exchange_weak(observed.val_, desired.val_)) {
          observed = desired;
          break;
        }
      }
      return observed;
    }

    NodePtr try_update(const NodePtr expected, NodePtr desired)
    {
      NodePtr observed = expected;
      while (observed == expected) {
        if (this->atomic_val().compare_exchange_weak(observed.val_, desired.val_)) {
          observed = desired;
          break;
        }
      }
      return observed;
    }

    u64 int_value() const noexcept
    {
      return this->val_;
    }

    bool operator!() const noexcept
    {
      return this->val_ == 0;
    }

    explicit operator bool() const noexcept
    {
      return this->val_ != 0;
    }

    friend inline bool operator==(NodePtr left, NodePtr right) noexcept
    {
      return left.val_ == right.val_;
    }

    friend inline bool operator!=(NodePtr left, NodePtr right) noexcept
    {
      return !(left == right);
    }

    friend inline std::ostream& operator<<(std::ostream& out, const NodePtr& t)
    {
      return out << (void*)t.val_;
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

    explicit NodePtrView(NodePtr& that) noexcept : ptr{that.sync()}, p_ptr{&that}
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

    SmallNode(const SmallNode&) = delete;
    SmallNode& operator=(const SmallNode&) = delete;

    explicit SmallNode(const SmallNode<4>* old) noexcept
        : branches{}
        , key{}
        , branch_count{4}
        , successor{nullptr}
    {
      static_assert(kBranchCount == 16);
      std::copy(old->branches.begin(), old->branches.end(), this->branches.begin());
      std::copy(old->key.begin(), old->key.end(), this->key.begin());
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
  struct Node48 {
    std::array<NodePtr, 48> branches;
    std::array<BranchIndex, 256> branch_for_key;
    AtomicBranchCount branch_count;
    NodePtr successor;

    //----- --- -- -  -  -   -

    explicit Node48(const Node16* old) noexcept
        : branches{}
        , branch_for_key{}
        , branch_count{16}
        , successor{nullptr}
    {
      this->branch_for_key.fill(kInvalidBranchIndex);

      for (usize i = 0; i < 16; ++i) {
        this->branch_for_key[old->key[i]] = i;
        this->branches[i] = old->branches[i];
      }
    }

    Node48(const Node48&) = delete;
    Node48& operator=(const Node48&) = delete;

    std::atomic<u8>& atomic_branch_for_key(u8 key_byte)
    {
      return reinterpret_cast<std::atomic<u8>&>(this->branch_for_key[key_byte]);
    }

    NodePtrView find(u8 /*key_byte*/)
    {
      BATT_PANIC() << "TODO [tastolfi 2025-06-11] ";
      BATT_UNREACHABLE();
    }

    NodePtrView insert(u8 key_byte, ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node256 {
    std::array<NodePtr, 256> branches;

    //----- --- -- -  -  -   -

    Node256() noexcept
    {
      this->branches.fill(nullptr);
    }

    explicit Node256(const Node48* old) noexcept
    {
      for (usize key_byte = 0; key_byte < 256; ++key_byte) {
        const BranchIndex branch_i = old->branch_for_key[key_byte];
        if (branch_i == kInvalidBranchIndex) {
          this->branches[key_byte] = nullptr;
        } else {
          BATT_CHECK(old->branches[branch_i]);
          this->branches[key_byte] = old->branches[branch_i];
        }
      }
    }

    Node256(const Node256&) = delete;
    Node256& operator=(const Node256&) = delete;

    NodePtrView find(u8 key_byte)
    {
      return NodePtrView{this->branches[key_byte]};
    }

    NodePtrView insert(u8 key_byte, ART* art);
  };

  static NodePtr get_successor(Node4* node)
  {
    return node->successor;
  }

  static NodePtr get_successor(Node16* node)
  {
    return node->successor;
  }

  static NodePtr get_successor(Node48* node)
  {
    return node->successor;
  }

  static NodePtr get_successor(Node256*)
  {
    return NodePtr{};
  }

  //----- --- -- -  -  -   -

  static_assert(sizeof(Node4) == 48);
  static_assert(sizeof(Node4) % 8 == 0);
  static_assert(alignof(Node4) >= 8);

  static_assert(sizeof(Node16) == 160);
  static_assert(sizeof(Node16) % 8 == 0);
  static_assert(alignof(Node16) >= 8);

  static_assert(sizeof(Node48) == 656);
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

    for (;;) {
      NodePtr root = &this->root_;
      SmallVec<NodePtrView, kMaxKeyLen> stack;
      stack.emplace_back(root);

      for (char key_char : key) {
        const u8 key_byte = key_char;

        NodePtrView& tip = stack.back();

        tip.ptr.visit([&](auto* node) {
          NodePtrView child = node->insert(key_byte, this);
          BATT_CHECK(child.ptr);
          NodePtr successor = get_successor(node);
          if (successor) {
            tip.ptr = tip.p_ptr->try_update(tip.ptr, successor);
          }
          stack.emplace_back(child);
        });
      }

      if (false) {
        // Check for conflicts down the path.
        //
        bool conflict = false;
        for (NodePtrView& view : stack) {
          view.ptr.visit([&](auto* node) {
            if (get_successor(node)) {
              conflict = true;
            }
          });
          if (conflict) {
            break;
          }
        }
        if (!conflict) {
          break;
        }
      }
      break;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct MemoryContext {
    ART* art_{nullptr};
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

    void* alloc(usize n, ART* art)
    {
      this->art_ = art;

      const usize in_use_prior = this->in_use_;
      if (in_use_prior + n <= sizeof(ExtentStorageT)) {
        this->in_use_ += n;
        return this->data_ + in_use_prior;
      }

      this->thread_extents_.emplace_back(std::make_unique<ExtentStorageT>());
      this->data_ = reinterpret_cast<u8*>(this->thread_extents_.back().get());
      this->in_use_ = 0;

      return this->alloc(n, art);
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void* alloc_storage(usize n)
  {
    return this->per_thread_memory_context_.get().alloc(n, this);
  }

  Node4* alloc_node()
  {
    return new (this->alloc_storage(sizeof(Node4))) Node4{};
  }

  Node16* alloc_successor(const Node4& old)
  {
    return new (this->alloc_storage(sizeof(Node16))) Node16{&old};
  }

  Node48* alloc_successor(const Node16& old)
  {
    return new (this->alloc_storage(sizeof(Node48))) Node48{&old};
  }

  Node256* alloc_successor(const Node48& old)
  {
    return new (this->alloc_storage(sizeof(Node256))) Node256{&old};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Node256 root_;
  absl::Mutex mutex_;
  std::vector<std::unique_ptr<ExtentStorageT>> extents_;
  ObjectThreadStorage<MemoryContext>::ScopedSlot per_thread_memory_context_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... CaseFns>
inline bool ART::NodePtr::visit(CaseFns&&... case_fns)
{
  auto visitor = batt::make_case_of_visitor(BATT_FORWARD(case_fns)...);

  void* ptr = reinterpret_cast<void*>(this->val_ & Self::kPtrMask);

  switch (this->get_type()) {
    case Self::kTypeNode4:
      visitor(static_cast<Node4*>(ptr));
      break;
    case Self::kTypeNode16:
      visitor(static_cast<Node16*>(ptr));
      break;
    case Self::kTypeNode48:
      visitor(static_cast<Node48*>(ptr));
      break;
    case Self::kTypeNode256:
      visitor(static_cast<Node256*>(ptr));
      break;
    default:
      return false;
  }
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kBranchCount>
inline auto ART::SmallNode<kBranchCount>::insert(u8 key_byte, ART* art) -> NodePtrView
{
  const usize i = index_of(key_byte, this->key);
  const usize observed_size = this->branch_count.get();
  if (i < observed_size) {
    return NodePtrView{this->branches[i]};
  }

  // Try to add the new key.
  //
  if (observed_size < kBranchCount) {
    const usize new_i = this->branch_count.try_reserve(/*max_branches=*/kBranchCount);
    if (new_i < kBranchCount) {
      this->key[new_i] = key_byte;

      // Wait for all reservations before this one to commit.
      //
      this->branch_count.wait_for(new_i);
      {
        auto on_scope_exit = batt::finally([&] {
          this->branch_count.commit(new_i);
        });

        // See if there was a conflict adding a branch for `key_byte`.
        //
        const usize conflict_i = index_of(key_byte, this->key);
        if (conflict_i < new_i) {
          return NodePtrView{this->branches[conflict_i]};
        }

        this->branches[new_i] = NodePtr{art->alloc_node()};
      }
      return NodePtrView{this->branches[new_i]};
    }
    // TODO [tastolfi 2025-06-11] if reserve fails, maybe try to find abandoned keys?
  }

  // If the node is full, try to allocate its successor.  First wait for any pending branches to be
  // addded to this node.
  //
  this->branch_count.wait_for(kBranchCount);

  // CAS the new node into `this->successor`.
  //
  NodePtr new_node = this->successor.latch(NodePtr{art->alloc_successor(*this)});

  // Now that successor is set, retry the insert.
  //
  NodePtrView result;
  new_node.visit([&](auto* node) {
    result = node->insert(key_byte, art);
  });
  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node48::insert(u8 key_byte, ART* art) -> NodePtrView
{
  u8 observed_i = this->atomic_branch_for_key(key_byte).load();

  if (observed_i == kInvalidBranchIndex) {
    // No branch allocated for the given key_byte; attempt to reserve one.
    //
    const usize new_i = this->branch_count.try_reserve(/*max_branches=*/48);
    if (new_i < 48) {
      this->branches[new_i] = art->alloc_node();
      if (this->atomic_branch_for_key(key_byte).compare_exchange_strong(observed_i, new_i)) {
        observed_i = new_i;
      }
    } else {
      // CAS the new node into `this->successor`.
      //
      NodePtr new_node = this->successor.latch(NodePtr{art->alloc_successor(*this)});

      // Now that successor is set, retry the insert.
      //
      NodePtrView result;
      new_node.visit([&](auto* node) {
        result = node->insert(key_byte, art);
      });
      return result;
    }
  }

  return NodePtrView{this->branches[observed_i]};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::insert(u8 key_byte, ART* art) -> NodePtrView
{
  auto& atomic_branch = reinterpret_cast<std::atomic<u64>&>(this->branches[key_byte]);

  u64 observed_val = atomic_branch.load();
  for (;;) {
    if (observed_val != 0) {
      NodePtrView view;
      view.ptr = NodePtr{observed_val};
      view.p_ptr = &this->branches[key_byte];
      return view;
    }

    NodePtr new_child{art->alloc_node()};
    if (atomic_branch.compare_exchange_weak(observed_val, new_child.int_value())) {
      observed_val = new_child.int_value();
    }
  }
  BATT_UNREACHABLE();
}

}  // namespace turtle_kv
