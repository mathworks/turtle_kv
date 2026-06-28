#pragma once

#include "art_base.hpp"
#include "art_bit_ops.hpp"
#include "art_default_inserters.hpp"

#include <turtle_kv/util/art_metrics.hpp>

#include <turtle_kv/util/byte_int.hpp>
#include <turtle_kv/util/seq_mutex.hpp>

#include <turtle_kv/import/bit_ops.hpp>
#include <turtle_kv/import/constants.hpp>
#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/metrics.hpp>
#include <turtle_kv/import/object_thread_storage.hpp>
#include <turtle_kv/import/optional.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/case_of.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/seq/loop_control.hpp>

#include <array>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace turtle_kv {

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

  template <Synchronized kSynchronized, bool kValuesOnly = false>
  class Scanner;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ART() noexcept
  {
  }

  explicit ART(ARTBase::Metrics& metrics) noexcept : ARTBase{metrics}
  {
  }

  ~ART() noexcept
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename InserterT>
  Status insert(std::string_view key, InserterT&& inserter);

  BATT_ALWAYS_INLINE void insert(std::string_view key)
  {
    static_assert(std::is_same_v<void, ValueT>);

    this->insert(key, DefaultVoidInserter{}).IgnoreError();
  }

  bool contains(std::string_view key);

  const ValueT* unsynchronized_find(std::string_view key);

  Optional<ValueT> find(std::string_view key);

  template <typename Fn>
  void scan(std::string_view lower_bound_key, const Fn& fn);

  /** \brief Returns true iff the container is empty.
   */
  bool empty()
  {
    BranchView branch;
    for (;;) {
      SeqMutex<u32>::ReadLock root_read_lock{this->super_root_.mutex_};
      branch.load(this->root_);
      if (!root_read_lock.changed()) {
        break;
      }
    }
    return branch.ptr == nullptr;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  using SmallestParentNode = Node4;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename NodeT, typename = std::enable_if_t<!std::is_same_v<NodeT, Node256>>>
  NodeBase* add_child(NodeT* node, u8 key_byte, NodeBase* child);

  NodeBase* add_child(Node256* node, u8 key_byte, NodeBase* child);

  template <typename NodeT>
  LeafNode* add_child_leaf(NodeT* node, u8 key_byte, const char* new_key_data, usize new_key_len);

  LeafNode* make_leaf_node(const char* prefix, usize prefix_len);

  Node4* make_parent_node(const char* prefix, usize prefix_len);

  Node4* grow_node(LeafNode* old_node);

  Node16* grow_node(Node4* old_node);

  Node48* grow_node(Node16* old_node);

  Node256* grow_node(Node48* old_node);

  Node256* grow_node(Node256*);

  LeafNode* clone_node(LeafNode* orig_node, usize prefix_offset);

  Node4* clone_node(Node4* orig_node, usize prefix_offset);

  Node16* clone_node(Node16* orig_node, usize prefix_offset);

  Node48* clone_node(Node48* orig_node, usize prefix_offset);

  Node256* clone_node(Node256* orig_node, usize prefix_offset);

  template <typename NodeLockT, typename NodeCallbackFn /*= void(NodeT*) */>
  void find_impl(std::string_view key, batt::StaticType<NodeLockT>, NodeCallbackFn&& node_callback);

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
    case NodeType::kLeafNode:
      visitor(static_cast<LeafNode*>(this));
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

    case NodeType::kNodeBase:  // fall-through
    default:
      BATT_PANIC() << "Bad node type: " << (int)observed;
      BATT_UNREACHABLE();
  }
}

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
#include <turtle_kv/util/art_scanner.hpp>
