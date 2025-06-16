#pragma once

#include <turtle_kv/util/object_thread_storage.hpp>

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
template <typename IntT>
class SeqLock
{
 public:
  explicit SeqLock(std::atomic<IntT>& state) noexcept : state_{state}
  {
    for (;;) {
      const u16 old_state = state.fetch_or(1);
      if ((old_state & 1) == 0) {
        state.fetch_add(2);
        return;
      }
    }
  }

  ~SeqLock() noexcept
  {
    this->state_.fetch_add(1);
  }

 private:
  std::atomic<IntT>& state_;
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

  struct Node1;
  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

  enum struct NodeType : u8 {
    // kNode1,
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
    u8 flags_;
    u8 prefix_len_;
    u8 branch_count_;
    std::atomic<u32> state_{0};
    const u8* prefix_;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit NodeBase(NodeType t) noexcept : node_type{t}
    {
    }

    NodeBase(const NodeBase&) = delete;
    NodeBase& operator=(const NodeBase&) = delete;

    template <typename... CaseFns>
    bool visit(CaseFns&&... case_fns);

    bool is_finalized() const
    {
      return (this->size_d_ & 0x80) != 0;
    }

    void finalize()
    {
      this->size_d_ |= 0x80;
    }

    bool is_terminal() const
    {
      return (this->size_d_ & 0x40) != 0;
    }

    void set_terminal()
    {
      this->size_d_ |= 0x40;
    }

    u8 get_size() const
    {
      return this->size_d_ & 0x3f;
    }

    void set_size(u8 n)
    {
      BATT_CHECK(!this->is_finalized());
      this->size_d_ = (this->size_d_ & 0xc0) | (n & 0x3f);
    }
  };

  static_assert(sizeof(NodeBase) == 16);

  using BranchIndex = u8;

  static constexpr BranchIndex kInvalidBranchIndex = u8{255};

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node1 : NodeBase {
    const char* data_;
    NodeBase* child = nullptr;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    explicit Node1(const char* data, usize len) noexcept : NodeBase{NodeType::kNode1}, data_{data}
    {
      this->size_ = BATT_CHECKED_CAST(u8, len);
    }

    Node1(const Node1&) = delete;
    Node1& operator=(const Node1&) = delete;

    auto insert(bool& path_conflict, const char*& key_data, usize& key_len, ART* art) -> NodeBase**;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct SmallNode : NodeBase {
    std::array<NodeBase*, kBranchCount> branches;
    std::array<u8, kBranchCount> key;

    //----- --- -- -  -  -   -

    explicit SmallNode() noexcept
        : NodeBase{(kBranchCount == 4) ? NodeType::kNode4 : NodeType::kNode16}
    {
      this->set_size(0);
    }

    SmallNode(const SmallNode&) = delete;
    SmallNode& operator=(const SmallNode&) = delete;

    explicit SmallNode(const SmallNode<4>* old) noexcept : SmallNode{}
    {
      static_assert(kBranchCount == 16);
      //----- --- -- -  -  -   -
      this->set_size(old->get_size());
      std::copy(old->branches.begin(), old->branches.end(), this->branches.begin());
      std::copy(old->key.begin(), old->key.end(), this->key.begin());
    }

    explicit SmallNode(const Node1* old, ART* art) noexcept : SmallNode{}
    {
      static_assert(kBranchCount == 4);
      //----- --- -- -  -  -   -
      usize key_len = old->get_size();
      if (key_len != 0) {
        bool path_conflict = false;
        const char* key_data = old->data_;
        NodeBase** branch = this->insert(path_conflict, key_data, key_len, art);
        BATT_CHECK_NOT_NULLPTR(branch);
        BATT_CHECK_NOT_NULLPTR(*branch);
        BATT_CHECK_EQ(key_len, 0);
        BATT_CHECK(!path_conflict);
      }
    }

    //----- --- -- -  -  -   -

    auto insert(bool& path_conflict, const char*& key_data, usize& key_len, ART* art) -> NodeBase**;
  };

  struct Node4 : SmallNode<4> {
    using SmallNode<4>::SmallNode;
  };

  struct Node16 : SmallNode<16> {
    using SmallNode<16>::SmallNode;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node48 : NodeBase {
    std::array<NodeBase*, 48> branches;
    std::array<BranchIndex, 256> branch_for_key;

    //----- --- -- -  -  -   -

    explicit Node48(const Node16* old) noexcept : NodeBase{NodeType::kNode48}
    {
      this->branch_for_key.fill(kInvalidBranchIndex);

      for (usize i = 0; i < old->get_size(); ++i) {
        this->branch_for_key[old->key[i]] = i;
        this->branches[i] = old->branches[i];
      }
    }

    Node48(const Node48&) = delete;
    Node48& operator=(const Node48&) = delete;

    auto insert(bool& path_conflict, const char*& key_data, usize& key_len, ART* art) -> NodeBase**;
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

    explicit Node256(const Node48* old) noexcept : Node256{}
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

    auto insert(bool& path_conflict, const char*& key_data, usize& key_len, ART* art) -> NodeBase**;
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

  using ExtentStorageT = std::aligned_storage_t<sizeof(Node256) * 512, 64>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() = default;

  void put(std::string_view key)
  {
    DVLOG(1) << "[put]" << BATT_INSPECT_STR(key);

    for (;;) {
      const char* key_data = key.data();
      usize key_len = key.size();

      NodeBase* root = &this->root_;
      NodeBase** node = &root;
      NodeBase* parent = nullptr;

      bool path_conflict = false;

      while (key_len != 0) {
        for (;;) {
          bool retry = false;
          if (!(**node).visit([&](auto* node_case) {
                NodeBase** child = node_case->insert(path_conflict, key_data, key_len, this);
                if (!child) {
                  SeqLock<u16> lock0{parent->NodeBase::state_};
                  SeqLock<u16> lock1{node_case->NodeBase::state_};
                  if (parent->NodeBase::is_finalized()) {
                    path_conflict = true;
                  } else {
                    node_case->NodeBase::finalize();
                    *node = this->grow_node(*node_case);
                    retry = true;
                  }
                } else {
                  parent = node_case;
                  node = child;
                }
              })) {
            retry = true;
          }
          if (path_conflict || !retry) {
            break;
          }
        }
      }

      if (!path_conflict) {
        break;
      }
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

  Node1* new_node1(const char* data, usize len)
  {
    return new (this->alloc_storage(sizeof(Node1))) Node1{data, len};
  }

  Node4* new_node4()
  {
    return new (this->alloc_storage(sizeof(Node4))) Node4{};
  }

  NodeBase* new_node(const char* data, usize len)
  {
    if (len == 0) {
      return this->new_node4();
    }
    return this->new_node1(data, len);
  }

  Node4* grow_node(const Node1& old)
  {
    return new (this->alloc_storage(sizeof(Node4))) Node4{&old, this};
  }

  Node16* grow_node(const Node4& old)
  {
    return new (this->alloc_storage(sizeof(Node16))) Node16{&old};
  }

  Node48* grow_node(const Node16& old)
  {
    return new (this->alloc_storage(sizeof(Node48))) Node48{&old};
  }

  Node256* grow_node(const Node48& old)
  {
    return new (this->alloc_storage(sizeof(Node256))) Node256{&old};
  }

  Node256* grow_node(const Node256&)
  {
    BATT_PANIC() << "Node256 is the largest node type!";
    BATT_UNREACHABLE();
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
inline bool ART::NodeBase::visit(CaseFns&&... case_fns)
{
  auto visitor = batt::make_case_of_visitor(BATT_FORWARD(case_fns)...);

  switch (this->node_type) {
    case NodeType::kNode1:
      visitor(static_cast<Node1*>(this));
      break;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::Node1::insert(bool& path_conflict, const char*& key_data, usize& key_len, ART* art)
    -> NodeBase**
{
  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    const char* const observed_data = this->data_;
    usize const observed_len = this->get_size();

    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    const char* suffix_data = observed_data;
    usize suffix_len = observed_len;

    const char* insert_data = key_data;
    usize insert_len = key_len;

    while (suffix_len && insert_len && *suffix_data == *insert_data) {
      ++suffix_data;
      --suffix_len;

      ++insert_data;
      --insert_len;
    }

    const usize common_len = (observed_len - suffix_len);

    // If no common prefix was matched, fail; this node must grow.
    //
    if (common_len == 0) {
      return nullptr;
    }

    // The node must be modified.  Lock it first.
    //
    SeqLock<u16> lock{this->NodeBase::state_};

    if (observed_data != this->data_ || observed_len != this->size_) {
      // Conflict detected; retry.
      //
      continue;
    }

    // If the entire input key matched, *and* no conflict detected, then consume the input and
    // return success.
    //
    if (suffix_len == 0 && insert_len == 0) {
      key_data += key_len;
      key_len = 0;
      this->set_terminal();
      return &this->child;
    }

    if (suffix_len == 0 || insert_len == 0) {
      key_data += common_len;
      key_len -= common_len;

      this->set_size(common_len);

      Node1* new_child = nullptr;

      if (suffix_len > insert_len) {
        new_child = art->new_node1(this->data_ + common_len, suffix_len);
      } else {
        new_child = art->new_node(key_data, key_len);
      }

      new_child->child = this->child;
      this->child = new_child;

    } else {
      Node4* new_child = art->new_node4();

      bool path_conflict = false;
      const char* suffix_data = this->data_ + common_len;

      NodeBase** new_child_branch = new_child->insert(path_conflict, suffix_data, suffix_len, art);

      BATT_CHECK(!path_conflict);
      BATT_CHECK_NOT_NULLPTR(new_child_branch);
      BATT_CHECK_NOT_NULLPTR(*new_child_branch);
      BATT_CHECK_EQ((*new_child_branch)->node_type, NodeType::kNode1);

      Node1* split_node1 = static_cast<Node1*>(*new_child_branch);
      split_node1->child = this->child;
      this->child = new_child;
    }

    return &this->child;
  }
  BATT_UNREACHABLE();
}
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kBranchCount>
inline auto ART::SmallNode<kBranchCount>::insert(bool& path_conflict,
                                                 const char*& key_data,
                                                 usize& key_len,
                                                 ART* art) -> NodeBase**
{
  const u8 key_byte = key_data[0];

  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    const usize observed_size = std::min<usize>(kBranchCount, this->size_);
    NodeBase** found = nullptr;
    {
      const usize i = index_of(key_byte, this->key);
      if (i < observed_size) {
        found = &this->branches[i];
      }
    }

    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    if (!found) {
      if (observed_size < kBranchCount) {
        SeqLock<u16> lock{this->NodeBase::state_};

        // If the size changes, we must re-check the keys array.
        //
        if (this->size_ != observed_size) {
          continue;
        }

        const usize i = this->size_;
        //----- --- -- -  -  -   -
        this->key[i] = key_byte;
        this->branches[i] = art->new_node1(key_data + 1, key_len - 1);
        key_data += key_len;
        key_len = 0;
        //----- --- -- -  -  -   -
        ++this->size_;

        found = &this->branches[i];
      }
    } else {
      ++key_data;
      --key_len;
    }

    return found;
  }
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node48::insert(bool& path_conflict,
                                const char*& key_data,
                                usize& key_len,
                                ART* art) -> NodeBase**
{
  const u8 key_byte = key_data[0];

  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    usize i = this->branch_for_key[key_byte];
    if (i == kInvalidBranchIndex) {
      SeqLock<u16> lock{this->NodeBase::state_};

      // We must re-check the branch index for the given byte, to make sure some other thread
      // didn't insert it.
      //
      if (this->branch_for_key[key_byte] != kInvalidBranchIndex) {
        continue;
      }

      // If there is no more room, fail.
      //
      if (this->size_ == 48) {
        return nullptr;
      }

      // We have exclusive access, a branch for the search key is still not found, and we have
      // room in this node; add a new branch.
      //
      i = this->size_;
      //----- --- -- -  -  -   -
      this->branches[i] = art->new_node1(key_data + 1, key_len - 1);
      this->branch_for_key[key_byte] = i;
      key_data += key_len;
      key_len = 0;
      //----- --- -- -  -  -   -
      ++this->size_;
      //
      // fall-through...
    }

    NodeBase** found = &this->branches[i];

    // Done reading; re-load the SeqLock state to see if we must retry.
    //
    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    ++key_data;
    --key_len;

    return found;
  }
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::insert(bool& path_conflict,
                                 const char*& key_data,
                                 usize& key_len,
                                 ART* art) -> NodeBase**
{
  const u8 key_byte = key_data[0];

  for (;;) {
    const u16 before_state = this->NodeBase::state_.load();
    if ((before_state & 3) != 0) {
      continue;
    }

    const bool finalized = this->NodeBase::is_finalized();
    NodeBase** found = &this->branches[key_byte];

    const u16 after_state = this->NodeBase::state_.load();
    if (before_state != after_state) {
      continue;
    }

    if (finalized) {
      path_conflict = true;
      return nullptr;
    }

    if (*found == nullptr) {
      SeqLock<u16> lock{this->NodeBase::state_};
      if (*found == nullptr) {
        *found = art->new_node1(key_data + 1, key_len - 1);
        key_data += key_len;
        key_len = 0;
      } else {
        ++key_data;
        --key_len;
      }
    } else {
      ++key_data;
      --key_len;
    }

    return found;
  }
  BATT_UNREACHABLE();
}

}  // namespace turtle_kv
