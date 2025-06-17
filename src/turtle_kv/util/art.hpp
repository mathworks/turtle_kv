#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>
#include <turtle_kv/util/seq_mutex.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>

#include <batteries/case_of.hpp>
#include <batteries/checked_cast.hpp>

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
    static constexpr u8 kFlagTerminal = 0x80;
    static constexpr u8 kFlagObsolete = 0x40;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    const NodeType node_type;
    u8 flags_ = 0;
    u8 prefix_len_ = 0;
    u8 branch_count_ = 0;
    SeqMutex<u32> mutex_;
    const char* prefix_ = nullptr;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit NodeBase(NodeType t) noexcept : node_type{t}
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

    void assign_base_from(const NodeBase& that)
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
    std::array<NodeBase*, kBranchCount> branches;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit GrowableNode() noexcept : NodeBase{node_type_from_branch_count(kBranchCount)}
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
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct IndirectIndexedNode : GrowableNode<kBranchCount, IndirectIndexedNode<kBranchCount>> {
    std::array<u8, kBranchCount> key;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    usize index_of_branch(u8 key_byte)
    {
      return index_of(key_byte, this->key);
    }

    void set_branch_index(u8 key_byte, usize i)
    {
      this->key[i] = key_byte;
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct DirectIndexedNode : GrowableNode<kBranchCount, DirectIndexedNode<kBranchCount>> {
    std::array<BranchIndex, 256> branch_for_key;

    explicit DirectIndexedNode() noexcept
    {
      this->branch_for_key.fill(kInvalidBranchIndex);
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

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node256 : NodeBase {
    std::array<NodeBase*, 256> branches;

    //----- --- -- -  -  -   -

    Node256() noexcept : NodeBase{NodeType::kNode256}
    {
      this->branches.fill(nullptr);
    }

    Node256(const Node256&) = delete;
    Node256& operator=(const Node256&) = delete;

    //----- --- -- -  -  -   -

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

  using ExtentStorageT = std::aligned_storage_t<(128 * 1024), 64>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() = default;

  void put(std::string_view key);

  bool contains(std::string_view key);

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Node256 root_;
  absl::Mutex mutex_;
  std::vector<std::unique_ptr<ExtentStorageT>> extents_;
  ObjectThreadStorage<MemoryContext>::ScopedSlot per_thread_memory_context_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

}  // namespace turtle_kv
