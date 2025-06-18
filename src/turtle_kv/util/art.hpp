#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>
#include <turtle_kv/util/seq_mutex.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/small_vec.hpp>

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
class ART
{
 public:
  using Self = ART;

  static constexpr usize kMaxKeyLen = 64;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <usize kBranchCount>
  struct SmallNode;

  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

  enum struct NodeType : u8 {
    kNodeBase,
    kNode4,
    kNode16,
    kNode48,
    kNode256,
  };

  struct NodeBase {
    using Self = NodeBase;

    struct NoInit {
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    static constexpr u8 kFlagTerminal = 0x80;
    static constexpr u8 kFlagObsolete = 0x40;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const NodeType node_type;
    u8 flags_;
    u8 prefix_len_;
    u8 branch_count_;
    SeqMutex<u32> mutex_;
    const char* prefix_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit NodeBase(NodeType t) noexcept
        : node_type{t}
        , flags_{0}
        , prefix_len_{0}
        , branch_count_{0}
        , prefix_{nullptr}
    {
    }

    explicit NodeBase(NodeType t, NoInit) noexcept : node_type{t}
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

    void assign_from(const Self& that)
    {
      this->flags_ = that.flags_;
      this->prefix_len_ = that.prefix_len_;
      this->branch_count_ = that.branch_count_;
      this->prefix_ = that.prefix_;
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

    NodeBase* store(NodeBase* new_ptr)
    {
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

  static_assert(sizeof(NodeBase) == 16);

  using BranchIndex = u8;

  static constexpr BranchIndex kInvalidBranchIndex = u8{255};

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount, typename Derived>
  struct GrowableNode : NodeBase {
    using Self = GrowableNode;
    using Super = NodeBase;
    using NoInit = NodeBase::NoInit;

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

    void assign_from(const Self& that)
    {
      this->Super::assign_from(static_cast<const Super&>(that));
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
    using NoInit = NodeBase::NoInit;

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

    template <typename Fn>
    void visit_branches(i32 min_key, i32 max_key, Fn&& fn) const
    {
      std::array<NodeBase*, 256> sorted_branches;
      std::array<u64, 4> key_bitmap;

      key_bitmap.fill(0);

      const usize n_branches = this->branch_count();
      for (usize i = 0; i < n_branches; ++i) {
        const i32 key_byte = this->key[i];
        if (key_byte < min_key || key_byte > max_key) {
          continue;
        }
        sorted_branches[key_byte] = this->branches[i];
        key_bitmap[(key_byte >> 6) & 3] |= (u64{1} << (key_byte & 0x3f));
      }

#define TURTLE_KV_ART_VISIT_BRANCH_LOOP(word_i, key_byte_offset)                                   \
  {                                                                                                \
    const u64 word_val = key_bitmap[word_i];                                                       \
    for (i32 bit_i = first_bit(word_val); bit_i != 64; bit_i = next_bit(word_val, bit_i)) {        \
      const i32 key_byte = key_byte_offset + bit_i;                                                \
      NodeBase* branch = sorted_branches[key_byte];                                                \
      if (branch) {                                                                                \
        Optional<batt::seq::LoopControl> result = batt::seq::invoke_loop_fn(fn, key_byte, branch); \
        if (result && *result == batt::seq::LoopControl::kBreak) {                                 \
          return;                                                                                  \
        }                                                                                          \
      }                                                                                            \
    }                                                                                              \
  }

      TURTLE_KV_ART_VISIT_BRANCH_LOOP(0, 0)
      TURTLE_KV_ART_VISIT_BRANCH_LOOP(1, 64)
      TURTLE_KV_ART_VISIT_BRANCH_LOOP(2, 128)
      TURTLE_KV_ART_VISIT_BRANCH_LOOP(3, 192)

#undef TURTLE_KV_ART_VISIT_BRANCH_LOOP
    }

    void assign_from(const Self& that)
    {
      this->Super::assign_from(static_cast<const Super&>(that));
      __builtin_memcpy(this->key.data(), that.key.data(), this->branch_count());
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct DirectIndexedNode : GrowableNode<kBranchCount, DirectIndexedNode<kBranchCount>> {
    using Self = DirectIndexedNode;
    using Super = GrowableNode<kBranchCount, Self>;
    using NoInit = NodeBase::NoInit;

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

    template <typename Fn>
    void visit_branches(i32 min_key, i32 max_key, Fn&& fn)
    {
      for (i32 key_byte = min_key; key_byte <= max_key; ++key_byte) {
        BranchIndex i = this->branch_for_key[key_byte];
        if (i == kInvalidBranchIndex) {
          continue;
        }
        NodeBase* branch = this->branches[i];
        BATT_INVOKE_LOOP_FN((fn, key_byte, branch));
      }
    }

    void assign_from(const Self& that)
    {
      this->Super::assign_from(static_cast<const Super&>(that));
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
    using NoInit = NodeBase::NoInit;

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

    template <typename Fn>
    void visit_branches(i32 min_key, i32 max_key, Fn&& fn)
    {
      for (i32 key_byte = min_key; key_byte <= max_key; ++key_byte) {
        NodeBase* branch = this->branches[key_byte];
        if (branch) {
          BATT_INVOKE_LOOP_FN((fn, key_byte, branch));
        }
      }
    }

    void assign_from(const Self& that)
    {
      this->Super::assign_from(static_cast<const Super&>(that));
      this->branches = that.branches;
    }
  };

  //----- --- -- -  -  -   -

  static_assert(sizeof(Node4) == 56);
  static_assert(sizeof(Node4) % 8 == 0);
  static_assert(alignof(Node4) >= 8);

  static_assert(sizeof(Node16) == 160);
  static_assert(sizeof(Node16) % 8 == 0);
  static_assert(alignof(Node16) >= 8);

  static_assert(sizeof(Node48) == 656);
  static_assert(sizeof(Node48) % 8 == 0);
  static_assert(alignof(Node48) >= 8);

  static_assert(sizeof(Node256) == 2064);
  static_assert(sizeof(Node256) % 8 == 0);
  static_assert(alignof(Node256) >= 8);

  using ExtentStorageT = std::aligned_storage_t<(64 * 1024), 64>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() = default;

  void insert(std::string_view key);

  bool contains(std::string_view key);

  template <typename Fn>
  void scan(std::string_view lower_bound_key, const Fn& fn)
  {
    std::array<char, kMaxKeyLen> buffer;
    bool done = false;
    this->scan_impl(&this->root_, buffer.data(), 0, lower_bound_key, done, fn);
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

  template <typename NodeT>
  NodeBase* add_child(NodeT* node, u8 key_byte, NodeBase* child);

  template <typename NodeT>
  NodeBase* add_child(NodeT* node, u8 key_byte, const char* new_key_data, usize new_key_len);

  Node4* make_node4(const char* prefix, usize prefix_len);

  Node16* grow_node(Node4* old_node);

  Node48* grow_node(Node16* old_node);

  Node256* grow_node(Node48* old_node);

  Node256* grow_node(Node256*);

  Node4* clone_node(Node4* orig_node);

  Node16* clone_node(Node16* orig_node);

  Node48* clone_node(Node48* orig_node);

  Node256* clone_node(Node256* orig_node);

  template <typename NodeT, typename Fn>
  void scan_impl(NodeT* node,
                 char* prefix_buffer,
                 usize prefix_len,
                 std::string_view lower_bound_key,
                 bool& done,
                 const Fn& fn) const;

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
inline void ART::NodeBase::visit(CaseFns&&... case_fns)
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename Fn>
inline void ART::scan_impl(NodeT* node,
                           char* prefix_buffer,
                           usize prefix_len,
                           std::string_view lower_bound_key,
                           bool& done,
                           const Fn& fn) const
{
  // We will need to create a copy of the node data to protect against data races.
  //
  NodeT node_view{NodeBase::NoInit{}};

  // Retry the node read until we get a consistent view.
  //
  for (;;) {
    SeqMutex<u32>::ReadLock read_lock{node->mutex_};
    node_view.assign_from(*node);
    if (!read_lock.changed()) {
      break;
    }
  }

  // Compare the lower bound key to the current node prefix.
  //
  const usize compare_len = std::min<usize>(node_view.prefix_len_, lower_bound_key.size());
  if (compare_len) {
    const i32 order = __builtin_memcmp(node_view.prefix_, lower_bound_key.data(), compare_len);

    // If all keys in this subtree come before the lower bound, then there is nothing to do.
    //
    if (order < 0) {
      return;
    }

    // If the node prefix is a prefix of the lower bound key, then drop the prefix from the lower
    // bound; otherwise the node prefix comes *after* the lower bound, so we can safely ignore the
    // lower bound for the rest of the recursion.
    //
    if (order == 0 && compare_len == node_view.prefix_len_) {
      lower_bound_key.remove_prefix(compare_len);
    } else {
      lower_bound_key = {};
    }
  }

  // Set bounds for branch visitation.
  //
  const i32 min_key_byte = [&]() -> i32 {
    if (lower_bound_key.empty()) {
      return 0;
    }
    const i32 next_char = lower_bound_key.front();
    lower_bound_key.remove_prefix(1);
    return next_char;
  }();
  const i32 max_key_byte = 255;

  // Append the node prefix to the buffer.
  //
  __builtin_memcpy(prefix_buffer + prefix_len, node_view.prefix_, node_view.prefix_len_);
  prefix_len += node_view.prefix_len_;

  // If the current node is a key-terminal, emit the contents of the buffer.
  //
  if (node_view.is_terminal()) {
    if (!fn(std::string_view{prefix_buffer, prefix_len})) {
      done = true;
      return;
    }
  }

  // Recurse on branches.
  //
  node_view.visit_branches(
      min_key_byte,
      max_key_byte,
      [&](i32 key_byte, NodeBase* child) -> batt::seq::LoopControl {
        if (done) {
          return batt::seq::LoopControl::kBreak;
        }
        prefix_buffer[prefix_len] = (char)key_byte;

        if (key_byte == min_key_byte) {
          child->visit([&](auto* child_node) {
            this->scan_impl(child_node, prefix_buffer, prefix_len + 1, lower_bound_key, done, fn);
          });
        } else {
          child->visit([&](auto* child_node) {
            this->scan_impl(child_node, prefix_buffer, prefix_len + 1, {}, done, fn);
          });
        }

        return batt::seq::LoopControl::kContinue;
      });
}

}  // namespace turtle_kv
