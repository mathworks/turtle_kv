#include <turtle_kv/util/art.hpp>
//

#include <turtle_kv/import/optional.hpp>

namespace turtle_kv {

namespace {

usize find_common_prefix_len(const char* data0, usize size0, const char* data1, usize size1)
{
  usize n = 0;
  while (size0 && size1 && *data0 == *data1) {
    ++n;
    --size0;
    --size1;
    ++data0;
    ++data1;
  }
  return n;
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ART::insert(std::string_view key)
{
  bool reset = true;

  const char* key_data = nullptr;
  usize key_len = 0;
  BranchView branch;
  NodeBase* parent = nullptr;

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
          branch.store(this->make_node4(key_data, key_len))->set_terminal();
          return;
        }
      }

      parent = &this->super_root_;
    }

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
          find_common_prefix_len(node_prefix, node_prefix_len, key_data, key_len);

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
          this->add_child(new_parent,
                          /*key_byte=*/key_data[common_len],
                          key_data + (common_len + 1),
                          key_len - (common_len + 1))
              ->set_terminal();
        } else {
          new_parent->set_terminal();
        }

        node->set_obsolete();
        branch.store(new_parent);
        done = true;
        return;
      }

      if (key_len == common_len) {
        if (!node_is_terminal) {
          //----- --- -- -  -  -   -
          SeqMutex<u32>::WriteLock node_write_lock{node->mutex_};

          if (node->is_obsolete() || node_prefix != node->prefix() ||
              node_prefix_len != node->prefix_len_) {
            reset = true;
            return;
          }

          node->set_terminal();
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

      if (next.p_ptr != nullptr && next.ptr != nullptr) {
        parent = node;
        branch = next;
        key_data = new_key_data;
        key_len = new_key_len;
        return;
      }

      Optional<SeqMutex<u32>::WriteLock> parent_write_lock;
      Optional<SeqMutex<u32>::WriteLock> node_write_lock;

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
        this->add_child(new_node, key_byte, new_key_data, new_key_len)->set_terminal();

        node->set_obsolete();
        branch.store(new_node);

      } else {
        //----- --- -- -  -  -   -
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
          this->add_child(node, key_byte, new_key_data, new_key_len)->set_terminal();

        } else {
          if (next.reload() != nullptr) {
            reset = true;
            return;
          }
          next.store(this->make_node4(new_key_data, new_key_len))->set_terminal();
        }
      }

      done = true;
    });

    if (done) {
      return;
    }
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool ART::contains(std::string_view key)
{
  VLOG(1) << "ART::contains(" << batt::c_str_literal(key) << ")";

  bool reset = true;

  const char* key_data = nullptr;
  usize key_len = 0;
  BranchView branch;

  for (;;) {
    if (reset) {
      VLOG(1) << " -- (reset=true)";

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
        return false;
      }
    }

    bool done = false;
    bool result = false;

    branch.ptr->visit([&](auto* node) {
      SeqMutex<u32>::ReadLock node_read_lock{node->mutex_};

      const char* const node_prefix = node->prefix();
      const usize node_prefix_len = node->prefix_len_;
      const bool node_is_terminal = node->is_terminal();

      if (node_read_lock.changed()) {
        reset = true;
        return;
      }

      VLOG(1) << " -- node.prefix = "
              << batt::c_str_literal(std::string_view{node_prefix, node_prefix_len});

      VLOG(1) << " -- key_data    = " << batt::c_str_literal(std::string_view{key_data, key_len});

      const usize common_len =
          find_common_prefix_len(node_prefix, node_prefix_len, key_data, key_len);

      VLOG(1) << " --" << BATT_INSPECT(common_len) << BATT_INSPECT(key_len);

      if (common_len != node_prefix_len) {
        VLOG(1) << " -- (partial prefix mismatch; not found)";
        done = true;
        result = false;
        return;
      }

      if (common_len == key_len) {
        VLOG(1) << " -- key consumed;" << BATT_INSPECT(node_is_terminal);
        done = true;
        result = node_is_terminal;
        return;
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
        return;
      }

      if (next.p_ptr == nullptr || next.ptr == nullptr) {
        VLOG(1) << BATT_INSPECT((void*)next.p_ptr) << BATT_INSPECT((void*)next.ptr);
        done = true;
        result = false;
        return;
      }

      key_data += (common_len + 1);
      key_len -= (common_len + 1);
      branch = next;
    });

    if (done) {
      return result;
    }
  }
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT, typename>
auto ART::add_child(NodeT* node, u8 key_byte, NodeBase* child) -> NodeBase*
{
  const usize i = node->add_branch();
  node->set_branch_index(key_byte, i);
  node->branches[i] = child;
  return child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::add_child(Node256* node, u8 key_byte, NodeBase* child) -> NodeBase*
{
  const usize i = key_byte;
  BATT_CHECK_EQ(node->branches[i], nullptr);
  node->branches[i] = child;
  return child;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename NodeT>
auto ART::add_child(NodeT* node, u8 key_byte, const char* new_key_data, usize new_key_len)
    -> NodeBase*
{
  return this->add_child(node, key_byte, this->make_node4(new_key_data, new_key_len));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::make_node4(const char* prefix, usize prefix_len) -> Node4*
{
  Node4* new_node = new (this->alloc_storage(sizeof(Node4), prefix_len)) Node4{};

  new_node->set_prefix(prefix, prefix_len);

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::grow_node(Node4* old_node) -> Node16*
{
  Node16* new_node = new (this->alloc_storage(sizeof(Node16), old_node->prefix_len_)) Node16{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);
  new_node->branch_count_ = old_node->branch_count_;

  std::copy(old_node->key.begin(), old_node->key.end(), new_node->key.begin());
  std::copy(old_node->branches.begin(), old_node->branches.end(), new_node->branches.begin());

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::grow_node(Node16* old_node) -> Node48*
{
  Node48* new_node = new (this->alloc_storage(sizeof(Node48), old_node->prefix_len_)) Node48{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);
  new_node->branch_count_ = old_node->branch_count_;

  for (usize i = 0; i < new_node->branch_count_; ++i) {
    new_node->branch_for_key[old_node->key[i]] = i;
    new_node->branches[i] = old_node->branches[i];
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::grow_node(Node48* old_node) -> Node256*
{
  Node256* new_node = new (this->alloc_storage(sizeof(Node256), old_node->prefix_len_)) Node256{};

  new_node->set_prefix(old_node->prefix(), old_node->prefix_len_);

  for (usize key_byte = 0; key_byte < 256; ++key_byte) {
    const BranchIndex i = old_node->branch_for_key[key_byte];
    new_node->branches[key_byte] = (i == kInvalidBranchIndex) ? nullptr : old_node->branches[i];
  }

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::grow_node(Node256*) -> Node256*
{
  BATT_PANIC() << "Node256 can not grow larger!";
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::clone_node(Node4* orig_node, usize prefix_offset) -> Node4*
{
  Node4* new_node =
      new (this->alloc_storage(sizeof(Node4), (orig_node->prefix_len_ - prefix_offset)))
          Node4{NodeBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::clone_node(Node16* orig_node, usize prefix_offset) -> Node16*
{
  Node16* new_node =
      new (this->alloc_storage(sizeof(Node16), (orig_node->prefix_len_ - prefix_offset)))
          Node16{NodeBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::clone_node(Node48* orig_node, usize prefix_offset) -> Node48*
{
  Node48* new_node =
      new (this->alloc_storage(sizeof(Node48), (orig_node->prefix_len_ - prefix_offset)))
          Node48{NodeBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  return new_node;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto ART::clone_node(Node256* orig_node, usize prefix_offset) -> Node256*
{
  Node256* new_node =
      new (this->alloc_storage(sizeof(Node256), (orig_node->prefix_len_ - prefix_offset)))
          Node256{NodeBase::NoInit{}};

  new_node->assign_from(*orig_node, prefix_offset);

  return new_node;
}

}  // namespace turtle_kv
