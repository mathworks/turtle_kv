#pragma once

#include <turtle_kv/static_sort/static_sort.h>
#include <turtle_kv/import/int_types.hpp>

#include <batteries/case_of.hpp>

#include <absl/synchronization/mutex.h>

#include <array>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace turtle_kv {

class ART
{
 public:
  enum struct NodeType : u8 {
    kLeaf,
    kNode4,
    kNode16,
    kNode48,
    kNode256,
  };

  struct NodeBase {
    const NodeType node_type;
    u8 size_ = 0;
    u16 state_ = 0;  // TODO [tastolfi 2025-06-05] use for SeqLock

    //----- --- -- -  -  -   -

    explicit NodeBase(NodeType t) noexcept : node_type{t}
    {
    }
  };

  using BranchIndex = u8;
  using NodePtr = NodeBase*;

  static constexpr BranchIndex kInvalidBranchIndex = u8{255};
  static constexpr NodePtr kNullNode = nullptr;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Leaf : NodeBase {
    explicit Leaf() noexcept : NodeBase{NodeType::kLeaf}
    {
    }

    //----- --- -- -  -  -   -

    NodePtr* find(u8 key_byte [[maybe_unused]])
    {
      return nullptr;
    }

    NodePtr* insert(u8 key_byte [[maybe_unused]], ART* art [[maybe_unused]])
    {
      return nullptr;
    }

    NodePtr grow(ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <usize kBranchCount>
  struct SmallNode : NodeBase {
    std::array<NodePtr, kBranchCount> branches;
    std::array<u8, kBranchCount> key;

    //----- --- -- -  -  -   -

    explicit SmallNode() noexcept
        : NodeBase{(kBranchCount == 4) ? NodeType::kNode4 : NodeType::kNode16}
    {
    }

    explicit SmallNode(const SmallNode<4>& old) noexcept : SmallNode{}
    {
      this->size_ = old.size_;
      std::copy(old.branches.begin(), old.branches.end(), this->branches.begin());
      std::copy(old.key.begin(), old.key.end(), this->key.begin());
    }

    NodePtr* find(u8 key_byte)
    {
      for (usize i = 0; i < this->size_; ++i) {
        if (this->key[i] == key_byte) {
          return &this->branches[i];
        }
      }
      return nullptr;
    }

    NodePtr* insert(u8 key_byte, ART* art);

    void sort();
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node4 : SmallNode<4> {
    using SmallNode<4>::SmallNode;

    NodePtr grow(ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node16 : SmallNode<16> {
    using SmallNode<16>::SmallNode;

    NodePtr grow(ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node48 : NodeBase {
    std::array<NodePtr, 48> branches;
    std::array<BranchIndex, 256> branch_for_key;

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

    NodePtr* find(u8 key_byte)
    {
      const BranchIndex branch_i = this->branch_for_key[key_byte];
      if (branch_i == kInvalidBranchIndex) {
        return nullptr;
      }
      return &this->branches[branch_i];
    }

    NodePtr* insert(u8 key_byte, ART* art);

    NodePtr grow(ART* art);
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  struct Node256 : NodeBase {
    std::array<NodePtr, 256> branches;

    //----- --- -- -  -  -   -

    Node256() noexcept : NodeBase{NodeType::kNode256}
    {
      this->branches.fill(kNullNode);
    }

    explicit Node256(const Node48& old) noexcept : NodeBase{NodeType::kNode256}
    {
      BATT_CHECK_EQ(old.size_, 48);

      for (usize key_byte = 0; key_byte < 256; ++key_byte) {
        const BranchIndex branch_i = old.branch_for_key[key_byte];
        if (branch_i == kInvalidBranchIndex) {
          this->branches[key_byte] = kNullNode;
        } else {
          this->branches[key_byte] = old.branches[branch_i];
        }
      }
    }

    NodePtr* find(u8 key_byte)
    {
      return &this->branches[key_byte];
    }

    NodePtr* insert(u8 key_byte, ART* art);

    NodePtr grow(ART* art);
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

  using ExtentStorageT = std::aligned_storage_t<sizeof(Node256) * 4096, 64>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() = default;

  void put(std::string_view key)
  {
    DVLOG(1) << "[put]" << BATT_INSPECT_STR(key);

    NodeBase* root = &this->root_;
    NodeBase** node = &root;

    for (char key_char : key) {
      DVLOG(1) << BATT_INSPECT(key_char) << BATT_INSPECT(node);
      //----- --- -- -  -  -   -
      const u8 key_byte = key_char;

      for (bool retry = true; retry;) {
        retry = false;
        this->visit_node(*node, [&](auto* node_case) {
          NodeBase** child = node_case->insert(key_byte, this);
          if (!child) {
            *node = node_case->grow(this);
            retry = true;
          } else {
            node = child;
          }
        });
      }
    }
  }

  bool contains(std::string_view key)
  {
    DVLOG(1) << "[contains]" << BATT_INSPECT_STR(key);

    NodeBase* root = &this->root_;
    NodeBase** node = &root;

    for (char key_char : key) {
      DVLOG(1) << BATT_INSPECT(key_char) << BATT_INSPECT(node);
      //----- --- -- -  -  -   -
      const u8 key_byte = key_char;

      this->visit_node(*node, [&](auto* node_case) {
        node = node_case->find(key_byte);
      });

      if (node == nullptr || *node == kNullNode) {
        return false;
      }
    }

    return true;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  struct ExtentState {
    u8* data_{nullptr};
    std::atomic<usize> in_use_{sizeof(ExtentStorageT)};

    u8* alloc(usize n)
    {
      usize in_use_prior = this->in_use_.fetch_add(n);
      if (in_use_prior + n > sizeof(ExtentStorageT)) {
        this->in_use_.fetch_sub(n);
        return nullptr;
      }
      return this->data_ + in_use_prior;
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename... CaseFns>
  void visit_node(NodeBase* node, CaseFns&&... case_fns)
  {
    auto visitor = batt::make_case_of_visitor(BATT_FORWARD(case_fns)...);

    switch (node->node_type) {
      case NodeType::kLeaf:
        DVLOG(1) << " visiting Leaf";
        visitor(static_cast<Leaf*>(node));
        break;
      case NodeType::kNode4:
        DVLOG(1) << " visiting Node4";
        visitor(static_cast<Node4*>(node));
        break;
      case NodeType::kNode16:
        DVLOG(1) << " visiting Node16";
        visitor(static_cast<Node16*>(node));
        break;
      case NodeType::kNode48:
        DVLOG(1) << " visiting Node48";
        visitor(static_cast<Node48*>(node));
        break;
      case NodeType::kNode256:
        DVLOG(1) << " visiting Node256";
        visitor(static_cast<Node256*>(node));
        break;
      default:
        BATT_PANIC() << "bad node type";
        BATT_UNREACHABLE();
    }
  }

  void* alloc_storage(usize n_arg)
  {
    usize n = (n_arg + 7) & ~usize{7};

    DVLOG(1) << "alloc(" << n_arg << " -> " << n << ")";
    usize seq_i = this->extent_seq_.load();

    for (;;) {
      ExtentState& state = this->extent_state_[seq_i % 256];
      void* ptr = state.alloc(n);
      DVLOG(1) << BATT_INSPECT(ptr) << BATT_INSPECT(seq_i);
      if (ptr) {
        return ptr;
      }

      VLOG(1) << " no space in current extent;" << BATT_INSPECT(seq_i);
      {
        absl::MutexLock lock{&this->mutex_};
        seq_i = this->extent_seq_.load();
        ptr = this->extent_state_[seq_i % 256].alloc(n);
        VLOG(1) << " retry with lock;" << BATT_INSPECT(ptr) << BATT_INSPECT(seq_i);
        if (ptr) {
          return ptr;
        }

        this->extents_.emplace_back(std::make_unique<ExtentStorageT>());
        ++seq_i;
        VLOG(1) << " new extent added;" << BATT_INSPECT(seq_i);

        ExtentState& new_state = this->extent_state_[seq_i % 256];
        new_state.data_ = reinterpret_cast<u8*>(this->extents_.back().get());
        new_state.in_use_.store(0);

        const usize prior_seq = this->extent_seq_.fetch_add(1);
        BATT_CHECK_EQ(prior_seq + 1, seq_i)
            << "extent_seq_ must only be modified while holding the mutex!";
      }
    }
    BATT_UNREACHABLE();
  }

  Leaf* alloc_leaf()
  {
    return &this->leaf_;
  }

  Node4* alloc_node4()
  {
    return new (this->alloc_storage(sizeof(Node4))) Node4{};
  }

  Node16* alloc_node16(const Node4& old)
  {
    return new (this->alloc_storage(sizeof(Node16))) Node16{old};
  }

  Node48* alloc_node48(const Node16& old)
  {
    return new (this->alloc_storage(sizeof(Node48))) Node48{old};
  }

  Node256* alloc_node256(const Node48& old)
  {
    return new (this->alloc_storage(sizeof(Node256))) Node256{old};
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Node256 root_;
  Leaf leaf_;

  absl::Mutex mutex_;
  std::vector<std::unique_ptr<ExtentStorageT>> extents_;
  std::atomic<usize> extent_seq_{0};
  std::array<ExtentState, 256> extent_state_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Leaf::grow(ART* art) -> NodePtr
{
  return art->alloc_node4();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kBranchCount>
inline auto ART::SmallNode<kBranchCount>::insert(u8 key_byte, ART* art) -> NodePtr*
{
  for (usize i = 0; i < this->size_; ++i) {
    if (this->key[i] == key_byte) {
      return &this->branches[i];
    }
  }

  if (this->size_ == kBranchCount) {
    return nullptr;
  }

  const usize i = this->size_;
  //----- --- -- -  -  -   -
  this->key[i] = key_byte;
  this->branches[i] = art->alloc_leaf();
  //----- --- -- -  -  -   -
  ++this->size_;

  return &this->branches[i];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <usize kBranchCount>
inline void ART::SmallNode<kBranchCount>::sort()
{
  if (kBranchCount == 4) {
    std::array<u32, 4> tmp{
        (u32(this->key[0]) << 8) | 0,
        (u32(this->key[1]) << 8) | 1,
        (u32(this->key[2]) << 8) | 2,
        (u32(this->key[3]) << 8) | 3,
    };

    StaticSort<4> sorter;
    sorter(tmp);
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
inline auto ART::Node48::insert(u8 key_byte, ART* art) -> NodePtr*
{
  usize i = this->branch_for_key[key_byte];
  if (i == kInvalidBranchIndex) {
    if (this->size_ == 48) {
      return nullptr;
    }
    i = this->size_;
    this->branches[i] = art->alloc_leaf();
    this->branch_for_key[key_byte] = i;
    ++this->size_;
  }

  return &this->branches[i];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node48::grow(ART* art) -> NodePtr
{
  return art->alloc_node256(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::insert(u8 key_byte, ART* art) -> NodePtr*
{
  if (!this->branches[key_byte]) {
    this->branches[key_byte] = art->alloc_leaf();
  }

  return &this->branches[key_byte];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline auto ART::Node256::grow(ART*) -> NodePtr
{
  BATT_PANIC() << "invalid operation: Node256::grow()";
  return nullptr;
}

}  // namespace turtle_kv
