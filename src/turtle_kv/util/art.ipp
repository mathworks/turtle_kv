#pragma once

#include <turtle_kv/import/optional.hpp>

namespace turtle_kv {

namespace detail {

inline usize find_common_prefix_len(const char* data0, usize size0, const char* data1, usize size1)
{
  usize n = 0;
  usize common_size = std::min(size0, size1);
  while (common_size && *data0 == *data1) {
    ++n;
    --common_size;
    ++data0;
    ++data1;
  }
  return n;
}

}  // namespace detail

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
template <typename InserterT>
inline Status ART<ValueT>::insert(std::string_view key, InserterT&& inserter)
{
  bool reset = true;

  const char* key_data = nullptr;
  usize key_len = 0;
  BranchView branch;
  NodeBase* parent = nullptr;

  ART::metrics().insert_count.add(1);

  for (;;) {
    if (reset) {
      reset = false;

      key_data = key.data();
      key_len = key.size();

      for (;;) {
        SeqMutex<u32>::ReadLock root_read_lock{this->super_root_.mutex_};
        branch.load(this->root_);
        if (!root_read_lock.changed()) {
          break;
        }
      }

      if (branch.ptr == nullptr) {
        SeqMutex<u32>::WriteLock root_write_lock{this->super_root_.mutex_};
        if (branch.reload() == nullptr) {
          Node4* new_node = branch.store(this->make_node4(key_data, key_len));
          Status status = inserter.insert_new(ARTBase::uninitialized_value(new_node));
          if (status.ok()) {
            new_node->set_terminal();
          }
          return status;
        }
      }

      parent = &this->super_root_;
    }

    Status status = OkStatus();
    bool done = false;

    branch.ptr->visit([&](auto* node) {
      SeqMutex<u32>::ReadLock node_read_lock{node->mutex_};

      const char* const node_prefix = node->prefix();
      const usize node_prefix_len = node->prefix_len_;
      const bool node_is_terminal = node->is_terminal();

      if (node_read_lock.changed()) {
        reset = true;
        return;
      }

      const usize common_len =
          detail::find_common_prefix_len(node_prefix, node_prefix_len, key_data, key_len);

      if (common_len != node_prefix_len) {
        //----- --- -- -  -  -   -
        SeqMutex<u32>::WriteLock parent_write_lock{parent->mutex_};
        if (parent->is_obsolete()) {
          reset = true;
          return;
        }

        //----- --- -- -  -  -   -
        SeqMutex<u32>::WriteLock node_write_lock{node->mutex_};

        if (node != branch.reload() || node_prefix != node->prefix() ||
            node_prefix_len != node->prefix_len_ || node->is_obsolete()) {
          reset = true;
          return;
        }

        Node4* new_parent = this->make_node4(node_prefix, common_len);
        auto* new_node = this->clone_node(node, /*prefix_offset=*/(common_len + 1));

        this->add_child(new_parent, /*key_byte=*/node_prefix[common_len], new_node);

        if (common_len < key_len) {
          auto* new_child = this->add_child(new_parent,
                                            /*key_byte=*/key_data[common_len],
                                            key_data + (common_len + 1),
                                            key_len - (common_len + 1));
          status = inserter.insert_new(ARTBase::uninitialized_value(new_child));
          if (status.ok()) {
            new_child->set_terminal();
          }

        } else {
          if (new_parent->is_terminal()) {
            status = inserter.update_existing(ARTBase::mutable_value<ValueT>(new_parent));
          } else {
            status = inserter.insert_new(ARTBase::uninitialized_value(new_parent));
            if (status.ok()) {
              new_parent->set_terminal();
            }
          }
        }

        node->set_obsolete();
        branch.store(new_parent);
        done = true;
        return;
      }

      // If the common prefix is exactly the length of this node's prefix, then the search key is
      // fully consumed exactly at this node; just update the node and we are done!
      //
      if (key_len == common_len) {
        if (!node_is_terminal || !std::is_same_v<ValueT, void>) {
          //----- --- -- -  -  -   -
          SeqMutex<u32>::WriteLock node_write_lock{node->mutex_};

          // Now that we are holding the lock, check if anything has changed, and retry if so.
          //
          if (node->is_obsolete() || node_prefix != node->prefix() ||
              node_prefix_len != node->prefix_len_) {
            reset = true;
            return;
          }

          // If the node was already terminal, then update (in the case of ValueT=void, this is a
          // no-op; we would have short-circuited at the immediately enclosing conditional),
          // otherwise mark the node as storing a value, and call InserterT::insert_new.
          //
          if (!node_is_terminal) {
            status = inserter.insert_new(uninitialized_value(node));
            if (status.ok()) {
              node->set_terminal();
            }
          } else {
            status = inserter.update_existing(ARTBase::mutable_value<ValueT>(node));
          }
        }
        done = true;
        return;
      }

      const u8 key_byte = key_data[common_len];
      const char* const new_key_data = key_data + (common_len + 1);
      const usize new_key_len = key_len - (common_len + 1);

      const usize observed_branch_count = std::min(node->branch_count(), node->max_branch_count());
      BranchView next;
      {
        const usize i = node->index_of_branch(key_byte);
        if (i < observed_branch_count) {
          next.load(node->branches[i]);
        }
      }

      if (node_read_lock.changed()) {
        reset = true;
        return;
      }

      // The node has not changed, and there *is* a branch for the current key byte; step into that
      // subtree and continue in the outer loop.
      //
      if (next.p_ptr != nullptr && next.ptr != nullptr) {
        parent = node;
        branch = next;
        key_data = new_key_data;
        key_len = new_key_len;
        return;
      }

      // There is no branch on the current node for the current key byte.
      //
      Optional<SeqMutex<u32>::WriteLock> parent_write_lock;
      Optional<SeqMutex<u32>::WriteLock> node_write_lock;

      // If we can't add a branch because the observed branch count is the maximum, then grow the
      // node.  This requires a lock on the current node and its parent.
      //
      if (next.p_ptr == nullptr && observed_branch_count == node->max_branch_count()) {
        //----- --- -- -  -  -   -
        parent_write_lock.emplace(parent->mutex_);
        if (parent->is_obsolete()) {
          reset = true;
          return;
        }

        //----- --- -- -  -  -   -
        node_write_lock.emplace(node->mutex_);
        if (node->is_obsolete()) {
          reset = true;
          return;
        }

        BATT_CHECK_EQ(observed_branch_count, node->branch_count());

        auto* new_node = this->grow_node(node);
        Node4* new_child = this->add_child(new_node, key_byte, new_key_data, new_key_len);
        status = inserter.insert_new(ARTBase::mutable_value<ValueT>(new_child));
        if (status.ok()) {
          new_child->set_terminal();
        }

        node->set_obsolete();
        branch.store(new_node);

      } else {
        //----- --- -- -  -  -   -
        // It looks like there *may* be room for an extra branch on the current node, so try to add
        // one.  This requires only a lock on the current node.
        //
        node_write_lock.emplace(node->mutex_);
        if (node->is_obsolete()) {
          reset = true;
          return;
        }

        if (next.p_ptr == nullptr) {
          if (observed_branch_count != node->branch_count()) {
            reset = true;
            return;
          }
          Node4* new_child = this->add_child(node, key_byte, new_key_data, new_key_len);
          status = inserter.insert_new(uninitialized_value(new_child));
          if (status.ok()) {
            new_child->set_terminal();
          }

        } else {
          if (next.reload() != nullptr) {
            reset = true;
            return;
          }
          Node4* new_child = this->make_node4(new_key_data, new_key_len);
          next.store(new_child);
          status = inserter.insert_new(uninitialized_value(new_child));
          if (status.ok()) {
            new_child->set_terminal();
          }
        }
      }

      done = true;
    });

    if (done) {
      return status;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
template <typename NodeLockT, typename NodeCallbackFn /*= void(NodeT*) */>
void ART<ValueT>::find_impl(std::string_view key,
                            batt::StaticType<NodeLockT>,
                            NodeCallbackFn&& node_callback)
{
  bool reset = true;

  const char* key_data = nullptr;
  usize key_len = 0;
  BranchView branch;

  for (;;) {
    if (reset) {
      reset = false;

      key_data = key.data();
      key_len = key.size();

      for (;;) {
        NodeLockT root_read_lock{this->super_root_.mutex_};
        branch.load(this->root_);
        if (!root_read_lock.changed()) {
          break;
        }
      }
      if (branch.ptr == nullptr) {
        return;  // find_impl
      }
    }

    bool done = false;

    branch.ptr->visit([&](auto* node) {
      NodeLockT node_read_lock{node->mutex_};

      const char* const node_prefix = node->prefix();
      const usize node_prefix_len = node->prefix_len_;
      const bool node_is_terminal = node->is_terminal();

      if (node_read_lock.changed()) {
        reset = true;
        return;  // visit
      }

      const usize common_len =
          detail::find_common_prefix_len(node_prefix, node_prefix_len, key_data, key_len);

      // Mismatch in the middle of the prefix means the branch we would have taken isn't there.  Not
      // found.
      //
      if (common_len != node_prefix_len) {
        done = true;
        return;  // visit
      }

      // If the search key is fully consumed, we are done; set `result` if the node is marked as
      // terminal.
      //
      if (common_len == key_len) {
        if (node_is_terminal) {
          node_callback(node);
          if (std::is_same_v<ValueT, void> || !node_read_lock.changed()) {
            done = true;
          } else {
            node_callback((decltype(node))nullptr);
            reset = true;
          }
        } else {
          done = true;
        }
        return;  // visit
      }

      const u8 key_byte = key_data[common_len];

      const usize observed_branch_count = std::min(node->branch_count(), node->max_branch_count());
      BranchView next;
      {
        const usize i = node->index_of_branch(key_byte);
        if (i < observed_branch_count) {
          next.load(node->branches[i]);
        }
      }

      if (node_read_lock.changed()) {
        reset = true;
        return;  // visit
      }

      if (next.p_ptr == nullptr || next.ptr == nullptr) {
        done = true;
        return;  // visit
      }

      key_data += (common_len + 1);
      key_len -= (common_len + 1);
      branch = next;
    });

    if (done) {
      return;  // find_impl
    }
  }
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline bool ART<ValueT>::contains(std::string_view key)
{
  bool found = false;

  this->find_impl(key, batt::StaticType<SeqMutex<u32>::ReadLock>{}, [&found](auto* node) {
    found = (node != nullptr);
  });

  return found;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::unsynchronized_find(std::string_view key) -> const ValueT*
{
  const ValueT* p_value = nullptr;

  this->find_impl(key, batt::StaticType<SeqMutex<u32>::NullLock>{}, [&p_value](auto* node) {
    if (node == nullptr) {
      p_value = nullptr;
    } else {
      p_value = ARTBase::const_value<ValueT>(node);
    }
  });

  return p_value;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::find(std::string_view key) -> Optional<ValueT>
{
  Optional<ValueT> value_copy;

  this->find_impl(key, batt::StaticType<SeqMutex<u32>::ReadLock>{}, [&value_copy](auto* node) {
    if (node == nullptr) {
      value_copy = None;
    } else {
      value_copy.emplace(*ARTBase::const_value<ValueT>(node));
    }
  });

  return value_copy;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
template <typename NodeT, typename>
inline auto ART<ValueT>::add_child(NodeT* node, u8 key_byte, NodeBase* child) -> NodeBase*
{
  const usize i = node->add_branch();
  node->set_branch_index(key_byte, i);
  node->branches[i] = child;
  return child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::add_child(Node256* node, u8 key_byte, NodeBase* child) -> NodeBase*
{
  const usize i = key_byte;
  BATT_CHECK_EQ(node->branches[i], nullptr);
  node->branches[i] = child;
  return child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
template <typename NodeT>
inline auto ART<ValueT>::add_child(NodeT* node,
                                   u8 key_byte,
                                   const char* new_key_data,
                                   usize new_key_len) -> Node4*
{
  Node4* new_child = this->make_node4(new_key_data, new_key_len);
  this->add_child(node, key_byte, new_child);
  return new_child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::make_node4(const char* prefix, usize prefix_len) -> Node4*
{
  Node4* new_node =
      new (this->alloc_storage(sizeof(Node4) + kValueStorageSize, prefix_len)) Node4{};

  new_node->set_prefix(prefix, prefix_len);

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::grow_node(Node4* old_node) -> Node16*
{
  Node16* new_node =
      new (this->alloc_storage(sizeof(Node16) + kValueStorageSize, old_node->prefix_len_)) Node16{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);
  new_node->branch_count_ = old_node->branch_count_;

  std::copy(old_node->key.begin(), old_node->key.end(), new_node->key.begin());
  std::copy(old_node->branches.begin(), old_node->branches.end(), new_node->branches.begin());

  if (old_node->is_terminal()) {
    ARTBase::construct_value_copy(old_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::grow_node(Node16* old_node) -> Node48*
{
  Node48* new_node =
      new (this->alloc_storage(sizeof(Node48) + kValueStorageSize, old_node->prefix_len_)) Node48{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);
  new_node->branch_count_ = old_node->branch_count_;

  for (usize i = 0; i < new_node->branch_count_; ++i) {
    new_node->branch_for_key[old_node->key[i]] = i;
    new_node->branches[i] = old_node->branches[i];
  }

  if (old_node->is_terminal()) {
    ARTBase::construct_value_copy(old_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
auto ART<ValueT>::grow_node(Node48* old_node) -> Node256*
{
  Node256* new_node =
      new (this->alloc_storage(sizeof(Node256) + kValueStorageSize, old_node->prefix_len_))
          Node256{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);

  for (usize key_byte = 0; key_byte < 256; ++key_byte) {
    const BranchIndex i = old_node->branch_for_key[key_byte];
    new_node->branches[key_byte] = (i == kInvalidBranchIndex) ? nullptr : old_node->branches[i];
  }

  if (old_node->is_terminal()) {
    ARTBase::construct_value_copy(old_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::grow_node(Node256*) -> Node256*
{
  BATT_PANIC() << "Node256 can not grow larger!";
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::clone_node(Node4* orig_node, usize prefix_offset) -> Node4*
{
  Node4* new_node =
      new (this->alloc_storage(sizeof(Node4) + kValueStorageSize,
                               (orig_node->prefix_len_ - prefix_offset))) Node4{ARTBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  if (orig_node->is_terminal()) {
    ARTBase::construct_value_copy(orig_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::clone_node(Node16* orig_node, usize prefix_offset) -> Node16*
{
  Node16* new_node =
      new (this->alloc_storage(sizeof(Node16) + kValueStorageSize,
                               (orig_node->prefix_len_ - prefix_offset))) Node16{ARTBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  if (orig_node->is_terminal()) {
    ARTBase::construct_value_copy(orig_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::clone_node(Node48* orig_node, usize prefix_offset) -> Node48*
{
  Node48* new_node =
      new (this->alloc_storage(sizeof(Node48) + kValueStorageSize,
                               (orig_node->prefix_len_ - prefix_offset))) Node48{ARTBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  if (orig_node->is_terminal()) {
    ARTBase::construct_value_copy(orig_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ValueT>
inline auto ART<ValueT>::clone_node(Node256* orig_node, usize prefix_offset) -> Node256*
{
  Node256* new_node = new (this->alloc_storage(sizeof(Node256) + kValueStorageSize,
                                               (orig_node->prefix_len_ - prefix_offset)))
      Node256{ARTBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  if (orig_node->is_terminal()) {
    ARTBase::construct_value_copy(orig_node, new_node, batt::StaticType<ValueT>{});
  }

  return new_node;
}

}  // namespace turtle_kv
